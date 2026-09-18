import base64
import hashlib
import logging
import time
import secrets
from typing import Optional, Tuple

from axiso import X25519PrivateKey, X25519PublicKey, HKDF
from cryptography.hazmat.primitives import serialization

from .postgres_store import PostgresStore
from .redis_store import RedisStore
from .models import User
from protocol import MAX_ATTEMPTS, ATTEMPT_WINDOW, BLOCK_DURATION, CODE_REQUEST_COOLDOWN
from .send_code_service import SendCodeService

logger = logging.getLogger(__name__)


class AuthService:
    def __init__(
        self,
        data_store: PostgresStore,
        redis_store: RedisStore,
        server_private_key_b64: str,
        send_code_service: SendCodeService,
    ):
        self.data_store = data_store
        self.redis_store = redis_store
        self.server_private_key_b64 = server_private_key_b64
        self.send_code_service = send_code_service

        private_raw = base64.urlsafe_b64decode(server_private_key_b64)
        private_key = X25519PrivateKey.from_private_bytes(private_raw)
        public_key = private_key.public_key()
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        self.server_public_key_b64 = base64.urlsafe_b64encode(public_key_bytes).decode()
        digest = hashlib.sha256(public_key_bytes).hexdigest()
        self.fingerprint = digest[:8]

    def build_handshake_response(self) -> dict:
        return {
            "type": "handshake_response",
            "public_key": self.server_public_key_b64,
            "fingerprint": self.fingerprint,
        }

    async def check_sms_attempts(self, email: str) -> Tuple[bool, str]:
        record = await self.redis_store.get_sms_attempts(email)
        if not record:
            return True, ""

        now = time.time()
        blocked_until = record.get("blocked_until", 0)
        if blocked_until > now:
            remaining = int(blocked_until - now)
            return False, f"Too many attempts, try again in {remaining} seconds"

        first_attempt = record.get("first_attempt", now)
        if now - first_attempt > ATTEMPT_WINDOW:
            await self.redis_store.delete_sms_attempts(email)
            return True, ""

        count = record.get("count", 0)
        if count >= MAX_ATTEMPTS:
            record["blocked_until"] = now + BLOCK_DURATION
            await self.redis_store.set_sms_attempts(email, record)
            return False, f"Too many attempts, blocked for {BLOCK_DURATION} seconds"

        return True, ""

    async def register_failed_attempt(self, email: str):
        record = await self.redis_store.get_sms_attempts(email)
        now = time.time()
        if not record:
            record = {"count": 1, "first_attempt": now}
        else:
            if now - record.get("first_attempt", now) > ATTEMPT_WINDOW:
                record = {"count": 1, "first_attempt": now}
            else:
                record["count"] = record.get("count", 0) + 1
        await self.redis_store.set_sms_attempts(email, record)

    async def reset_attempts(self, email: str):
        await self.redis_store.delete_sms_attempts(email)

    async def request_code(self, email: str) -> Tuple[bool, str]:
        last = await self.redis_store.get_last_code_request(email)
        now = time.time()
        if last and (now - last) < CODE_REQUEST_COOLDOWN:
            remaining = int(CODE_REQUEST_COOLDOWN - (now - last))
            return False, f"Подождите {remaining} секунд перед новым запросом"

        code = f"{secrets.randbelow(1000000):06d}"
        expires = time.time() + 300

        await self.redis_store.set_auth_code(email, code, expires)

        sent = await self.send_code_service.send_code(email, code)
        if not sent:
            await self.redis_store.delete_auth_code(email)
            return False, "Не удалось отправить код на email"

        await self.redis_store.set_last_code_request(email, now)
        return True, "Код отправлен на вашу почту"

    async def authenticate(
        self,
        email: str,
        sms_code: str,
        eddsa_public_key: str,
        client_public_key_raw: bytes,
    ) -> Tuple[bool, Optional[bytes], Optional[User], str]:
        allowed, msg = await self.check_sms_attempts(email)
        if not allowed:
            return False, None, None, msg

        stored = await self.redis_store.get_auth_code(email)
        if not stored:
            return False, None, None, "Код не запрошен или истёк. Запросите код заново."

        code = stored.get("code")
        expires = stored.get("expires", 0)

        if time.time() > expires:
            await self.redis_store.delete_auth_code(email)
            return False, None, None, "Код истёк. Запросите новый."

        if not secrets.compare_digest(sms_code, code):
            await self.register_failed_attempt(email)
            return False, None, None, "Неверный код"

        await self.reset_attempts(email)
        await self.redis_store.delete_auth_code(email)

        try:
            user = await self.data_store.get_user(email)
            if user is None:
                user = await self.data_store.create_user(email, eddsa_public_key)
            else:
                if user.public_key != eddsa_public_key:
                    logger.warning(f"EdDSA public key mismatch for {email}")
                    return False, None, None, "EdDSA public key mismatch"
        except Exception as e:
            logger.error(f"DB error while resolving user {email}: {e}")
            return False, None, None, "Внутренняя ошибка сервера"

        try:
            server_priv = X25519PrivateKey.from_private_bytes(
                base64.urlsafe_b64decode(self.server_private_key_b64)
            )
            client_pub = X25519PublicKey.from_public_bytes(client_public_key_raw)
            shared = server_priv.exchange(client_pub)
        except Exception as e:
            logger.error(f"DH error: {e}")
            return False, None, None, "DH key exchange failed"

        salt = email.encode("utf-8")
        info = b"chat_session"
        session_key = HKDF.extract_and_expand(salt, shared, info, 32)

        logger.info(f"User {email} authenticated successfully")
        return True, session_key, user, ""