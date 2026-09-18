# gateway.py
import asyncio
import websockets
import json
import time
import logging
import base64
import hashlib
import os
import re
from typing import Optional, Dict, List, Any
import ssl
from dotenv import load_dotenv
import secrets
from axiso import DH, X25519PrivateKey, X25519PublicKey, HKDF, EdDSA
from cryptography.hazmat.primitives import serialization
from protocol import (
    STREAM_ID_CONTROL,
    MAX_MESSAGE_SIZE,
    MAX_FILE_SIZE,
    MAX_HISTORY,
    SESSION_TIMEOUT,
    RATE_LIMIT,
    ENTITY_TYPE_CHAT,
    ENTITY_TYPE_GROUP,
    ENTITY_TYPE_CHANNEL,
    CMD_CREATE_ENTITY,
    CMD_JOIN_ENTITY,
    CMD_LEAVE_ENTITY,
    CMD_DELETE_ENTITY,
    CMD_SEND_MESSAGE,
    CMD_GET_MESSAGES,
    CMD_DELETE_MESSAGE,
    CMD_PROMOTE_ADMIN,
    CMD_DEMOTE_ADMIN,
    CMD_GET_USER_INFO,
    CMD_DOWNLOAD_FILE,
    CMD_SET_USERNAME,
    CMD_SET_BIO,
    CMD_SET_SHOW_EMAIL,
    CMD_SET_FIRST_NAME,
    TAG_VERIFIED,
    TAG_SCAM,
    encrypt_control,
    decrypt_control,
    encrypt_stream,
    decrypt_stream,
    derive_stream_key,
    serialize,
    deserialize,
    serialize_sorted,
    compute_file_checksum,
    CMD_REQUEST_UPLOAD_STREAM,
    ATTEMPT_WINDOW,
    REQUEST_CODE_TYPE,
    validate_email,
    CMD_SET_ENTITY_USERNAME,
    validate_username,
    BOT_AUTH_REQUEST,
    CMD_CREATE_BOT,
    CMD_DELETE_BOT,
    CMD_LIST_BOTS,
    CMD_GET_BOT_TOKEN,
    CMD_INVITE_BOT,
    REFRESH_SESSION_TYPE,
    CMD_REFRESH_SESSION,
    CMD_GET_ENTITY_INFO,
    CMD_ADD_REACTION,
    CMD_REMOVE_REACTION,
    NOTIFICATION_REACTION_UPDATE,
    REACTION_RATE_WINDOW,
    REACTION_RATE_LIMIT,
    CMD_GET_STICKERS,
    CMD_SEND_STICKER,
    CMD_GET_STICKER_FILE,
    MESSAGE_TYPE_STICKER,
    CMD_SET_ENTITY_PRIVATE,
    CMD_GET_JOIN_REQUESTS,
    CMD_APPROVE_JOIN_REQUEST,
    CMD_REJECT_JOIN_REQUEST,
    NOTIFICATION_NEW_JOIN_REQUEST,
    CMD_CALLBACK_QUERY,
    NOTIFICATION_CALLBACK,
    CMD_EDIT_MESSAGE,
    CMD_ANSWER_CALLBACK,
    CMD_GET_ANALYTICS,
    CMD_GET_SESSIONS,
    CMD_TERMINATE_SESSION,
    SPAMBOT_USERNAME,
    SPAMBOT_SYSTEM_EMAIL,
    SPAMBOT_DISPLAY_NAME,
)
from services.models import User, Entity, Message
from services.postgres_store import PostgresStore
from services.redis_store import RedisStore
from services.user_service import UserService
from services.entity_service import EntityService
from services.message_service import MessageService
from services.file_service import FileService
from services.auth_service import AuthService
from services.profile_service import ProfileService
from services.security_service import SecurityService
from services.send_code_service import SendCodeService
from services.bot_service import BotService
from services.spam_service import SpamService
from services.reaction_service import ReactionService
from services.stickers_service import StickerService
from services.analytics_service import AnalyticsService
from services.command_service import CommandService
from services.poll_service import PollService

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Gateway:
    def __init__(self, key_file: str = "server_private.key"):
        self.key_file = key_file
        self.private_key_b64 = self._load_or_generate_private_key()

        private_raw = base64.urlsafe_b64decode(self.private_key_b64)
        private_key = X25519PrivateKey.from_private_bytes(private_raw)
        public_key = private_key.public_key()
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        self.public_key_b64 = base64.urlsafe_b64encode(public_key_bytes).decode()
        digest = hashlib.sha256(public_key_bytes).hexdigest()
        self.fingerprint = digest[:8]

        self.db_store = PostgresStore(os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///./messenger.db'))
        self.redis_store = RedisStore()

        smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.getenv("SMTP_PORT", 587))
        sender_email = os.getenv("SMTP_EMAIL")
        sender_password = os.getenv("SMTP_PASSWORD")
        if not sender_email or not sender_password:
            logger.warning("SMTP credentials not set. Code sending will fail.")
        self.send_code_service = SendCodeService(
            smtp_server, smtp_port, sender_email, sender_password
        )

        self.user_service = UserService(self.db_store)
        self.entity_service = EntityService(self.db_store, self.redis_store)
        self.message_service = MessageService(self.db_store, self.redis_store, self)
        self.file_service = FileService(self.db_store, self.redis_store, self)
        self.auth_service = AuthService(
            self.db_store, self.redis_store, self.private_key_b64, self.send_code_service
        )
        self.profile_service = ProfileService(self.db_store, self.user_service)
        self.security_service = SecurityService(self.db_store, self.redis_store)
        self.bot_service = BotService(self.db_store, self.redis_store)
        self.reaction_service = ReactionService(self.db_store, self.redis_store)
        self.sticker_service = StickerService(self.db_store, self.redis_store)
        self.analytics_service = AnalyticsService(self.db_store)
        self.poll_service = PollService(self.db_store, self.redis_store)
        self.spam_service = SpamService(self.db_store)

        # id системного бота @spambot, заполняется в ensure_spambot() при старте
        self.spambot_bot_id: Optional[int] = None

        self.command_service = CommandService(
            db_store=self.db_store,
            redis_store=self.redis_store,
            user_service=self.user_service,
            entity_service=self.entity_service,
            message_service=self.message_service,
            file_service=self.file_service,
            auth_service=self.auth_service,
            profile_service=self.profile_service,
            security_service=self.security_service,
            bot_service=self.bot_service,
            reaction_service=self.reaction_service,
            sticker_service=self.sticker_service,
            analytics_service=self.analytics_service,
            poll_service=self.poll_service,
            spam_service=self.spam_service,
            gateway=self,
        )

        self.connections: Dict[str, "ChatServerProtocol"] = {}
        self._cleanup_task = asyncio.create_task(self._cleanup_join_requests_loop())

    async def init_redis(self):
        await self.redis_store.connect()
        await self.ensure_spambot()

    async def ensure_spambot(self):
        """
        Создаёт системного бота @spambot при первом запуске (аналог
        официального @spambot в Telegram), если он ещё не существует.
        От его имени сервер сам присылает пользователю статус его
        анти-спам ограничений в ответ на любое сообщение.
        """
        try:
            existing = await self.bot_service.get_bot_by_username(SPAMBOT_USERNAME)
            if existing:
                self.spambot_bot_id = existing.id
                if not existing.is_active:
                    existing.is_active = True
                    await self.db_store.update_bot(existing)
                return

            system_user = await self.db_store.get_user(SPAMBOT_SYSTEM_EMAIL)
            if not system_user:
                dh_keys = DH.generate_keypair()
                await self.db_store.create_user(SPAMBOT_SYSTEM_EMAIL, dh_keys["public_key"])

            bot = await self.bot_service.create_bot(
                SPAMBOT_SYSTEM_EMAIL, SPAMBOT_DISPLAY_NAME, username=SPAMBOT_USERNAME
            )
            if bot:
                self.spambot_bot_id = bot.id
                logger.info(f"Системный бот @{SPAMBOT_USERNAME} создан, id={bot.id}")
            else:
                logger.error("Не удалось создать системного бота @spambot")
        except Exception as e:
            logger.error(f"Ошибка инициализации @spambot: {e}", exc_info=True)

    def _load_or_generate_private_key(self) -> str:
        if os.path.exists(self.key_file):
            with open(self.key_file, "r") as f:
                return f.read().strip()
        else:
            dh_keys = DH.generate_keypair()
            private_b64 = dh_keys["private_key"]
            with open(self.key_file, "w") as f:
                f.write(private_b64)
            os.chmod(self.key_file, 0o600)
            return private_b64

    async def create_session(
        self,
        session_id: str,
        email: str,                     # <-- новый параметр
        session_key: bytes,
        client_public_key: bytes,
        is_bot: bool = False,
        bot_id: Optional[int] = None,
        ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        key = f"session:{session_id}"
        data = {
            "session_key": base64.urlsafe_b64encode(session_key).decode(),
            "client_public_key": base64.urlsafe_b64encode(client_public_key).decode(),
            "authenticated": "1",
            "created_at": str(time.time()),
            "last_active": str(time.time()),
            "is_bot": "1" if is_bot else "0",
            "bot_id": str(bot_id) if bot_id is not None else "",
            "ip": ip or "",
            "user_agent": user_agent or "",
            "email": email,              # <-- сохраняем email
        }
        await self.redis_store.redis.hset(key, mapping=data)
        await self.redis_store.redis.expire(key, SESSION_TIMEOUT)
        if email:
            await self.redis_store.add_user_session(email, session_id)
    
    async def send_refresh_entities(self, email: str):
        proto = self.connections.get(email)
        if proto:
            notification = {"type": "refresh_entities"}
            payload = serialize(notification)
            encrypted = encrypt_control(payload, proto.session_key)
            packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
            asyncio.create_task(proto.safe_send(packet))
          
    async def get_session(self, session_id: str) -> Optional[dict]:
        data = await self.redis_store.redis.hgetall(f"session:{session_id}")
        if not data:
            return None
        return {
            "session_key": base64.urlsafe_b64decode(data["session_key"]),
            "client_public_key": base64.urlsafe_b64decode(data["client_public_key"]),
            "authenticated": data.get("authenticated") == "1",
            "created_at": float(data["created_at"]),
            "last_active": float(data["last_active"]),
            "is_bot": data.get("is_bot") == "1",
            "bot_id": int(data["bot_id"]) if data.get("bot_id") else None,
            "ip": data.get("ip", ""),              # ← добавляем
            "user_agent": data.get("user_agent", ""),  # ← добавляем
            "email": data.get("email", ""),
        }

    async def update_session_ttl(self, session_id: str):
        await self.redis_store.redis.expire(f"session:{session_id}", SESSION_TIMEOUT)

    async def delete_session(self, session_id: str):
        session = await self.get_session(session_id)
        if session:
            email = session.get("email")
            if email:
                await self.redis_store.remove_user_session(email, session_id)
        await self.redis_store.redis.delete(f"session:{session_id}")

    async def broadcast_to_entity(self, entity_id: int, data: bytes, exclude_email: Optional[str] = None):
        members = await self.db_store.get_entity_members(entity_id)
        for email in members:
            if email == exclude_email:
                continue
            session = await self.get_session(email)
            if not session:
                continue
            proto = self.connections.get(email)
            if proto:
                asyncio.create_task(proto.safe_send(data))

    async def broadcast_file(self, entity_id: int, filename: str, file_content: bytes, sender_email: str):
        members = await self.db_store.get_entity_members(entity_id)
        for email in members:
            if email == sender_email:
                continue
            # Получаем все активные сессии этого пользователя
            session_ids = await self.redis_store.get_user_sessions(email)
            for sid in session_ids:
                proto = self.connections.get(sid)
                if not proto or not proto.session_key:
                    continue
                # Проверяем, что сессия ещё валидна
                session_data = await self.get_session(sid)
                if not session_data:
                    continue
                # Генерируем новый поток для каждого получателя отдельно
                new_stream = await self.redis_store.get_next_stream_id()
                # Ключ для шифрования потока – используется email получателя
                stream_key = derive_stream_key(proto.session_key, email, new_stream)
                header = {
                    "type": "file_header",
                    "filename": filename,
                    "size": len(file_content),
                    "entity_id": entity_id,
                    "checksum": compute_file_checksum(file_content),
                }
                header_data = serialize(header)
                encrypted_header = encrypt_stream(header_data, stream_key)
                encrypted_data = encrypt_stream(file_content, stream_key)
                # Отправляем три пакета: заголовок, данные и завершающий пустой
                packet1 = new_stream.to_bytes(4, "big") + encrypted_header
                packet2 = new_stream.to_bytes(4, "big") + encrypted_data
                packet3 = new_stream.to_bytes(4, "big")
                asyncio.create_task(proto.safe_send_multiple(packet1, packet2, packet3))

    async def _cleanup_join_requests_loop(self):
        while True:
            await asyncio.sleep(3600)
            try:
                deleted = await self.db_store.delete_expired_join_requests()
                if deleted:
                    logger.info(f"Deleted {deleted} expired join requests")
            except Exception as e:
                logger.error(f"Error cleaning join requests: {e}")

    async def send_control_to_entity(self, entity_id: int, obj: dict, exclude_email: Optional[str] = None):
        members = await self.db_store.get_entity_members(entity_id)
        payload = serialize(obj)
        for email in members:
            if email == exclude_email:
                continue
            # Получаем все активные сессии пользователя
            session_ids = await self.redis_store.get_user_sessions(email)
            for sid in session_ids:
                proto = self.connections.get(sid)
                if not proto:
                    continue
                session_data = await self.get_session(sid)
                if not session_data:
                    continue
                encrypted = encrypt_control(payload, session_data["session_key"])
                packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
                asyncio.create_task(proto.safe_send(packet))

    async def close(self):
        pass


class ChatServerProtocol:
    def __init__(self, websocket, gateway: Gateway, ip: str = None, user_agent: str = None):
        self.websocket = websocket
        self.gateway = gateway
        self.email: Optional[str] = None
        self.session_key: Optional[bytes] = None
        self.client_public_key: Optional[bytes] = None
        self.is_authenticated = False
        self._pending_tasks = set()
        self.processed_request_ids = set()
        self.command_timestamps: List[float] = []
        self.timeout_task: Optional[asyncio.Task] = None
        self.is_bot = False
        self.session_id: Optional[str] = None
        self.bot = None
        self.ip = ip
        self.user_agent = user_agent

    async def safe_send(self, data: bytes) -> None:
        task = asyncio.current_task()
        self._pending_tasks.add(task)
        try:
            await self.websocket.send(data)
        except Exception as e:
            logger.debug(f"Ошибка отправки {self.email}: {e}")
        finally:
            self._pending_tasks.discard(task)

    async def safe_send_multiple(self, *datas: bytes) -> None:
        task = asyncio.current_task()
        self._pending_tasks.add(task)
        try:
            for data in datas:
                await self.websocket.send(data)
        except Exception as e:
            logger.debug(f"Ошибка отправки нескольких пакетов {self.email}: {e}")
        finally:
            self._pending_tasks.discard(task)

    async def send_raw(self, data: bytes):
        try:
            await self.websocket.send(data)
        except Exception as e:
            logger.error(f"Ошибка отправки: {e}")

    async def send_control(self, data: bytes, encrypted: bool = True):
        if encrypted and self.session_key:
            payload = encrypt_control(data, self.session_key)
        else:
            payload = data
        packet = STREAM_ID_CONTROL.to_bytes(4, "big") + payload
        await self.send_raw(packet)

    async def send_error(self, request_id: int, reason: str):
        resp = {"request_id": request_id, "status": "error", "reason": reason}
        data = serialize(resp)
        await self.send_control(data)

    async def send_success(self, request_id: int, data: Any = None):
        resp = {"request_id": request_id, "status": "ok"}
        if data is not None:
            resp["data"] = data
        payload = serialize(resp)
        await self.send_control(payload)

    async def handle_bot_auth(self, obj: dict):
        token = obj.get("token")
        if not token:
            await self.send_error(0, "Token required")
            return
        bot = await self.gateway.bot_service.get_bot_by_token(token)
        if not bot or not bot.is_active:
            await self.send_error(0, "Invalid or inactive bot token")
            return
        if self.client_public_key is None:
            await self.send_error(0, "Handshake not performed")
            return
        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
        server_priv = X25519PrivateKey.from_private_bytes(
            base64.urlsafe_b64decode(self.gateway.private_key_b64)
        )
        client_pub = X25519PublicKey.from_public_bytes(self.client_public_key)
        shared = server_priv.exchange(client_pub)
        salt = f"bot_{bot.id}".encode('utf-8')
        info = b"chat_session"
        session_key = HKDF.extract_and_expand(salt, shared, info, 32)
        self.session_key = session_key
        self.is_authenticated = True
        self.is_bot = True
        self.bot = bot
        self.email = None
        self.session_id = f"bot_{bot.id}"
        self.gateway.connections[self.session_id] = self
        await self.gateway.create_session(
            self.session_id,
            self.session_id,              # email для бота (можно использовать session_id)
            session_key,                  # сессионный ключ
            self.client_public_key,       # публичный ключ клиента
            is_bot=True,
            bot_id=bot.id,
            ip=self.ip,
            user_agent=self.user_agent,
        )
        await self.send_success(0, {"bot_id": bot.id, "name": bot.name})
        logger.info(f"Bot {bot.id} ({bot.name}) authenticated")

    async def handle_refresh_session(self, obj: dict):
        request_id = obj.get("request_id", 0)
        email = obj.get("email")
        timestamp = obj.get("timestamp")
        signature_b64 = obj.get("signature")
        if not email or not signature_b64:
            await self.send_error(request_id, "Missing email or signature")
            return
        if await self.gateway.security_service.is_request_replayed(email, request_id):
            await self.send_error(request_id, "Replay attack detected")
            return
        user = await self.gateway.db_store.get_user(email)
        if not user:
            await self.send_error(request_id, "User not found")
            return
        sign_data = {k: v for k, v in obj.items() if k != "signature"}
        payload = serialize_sorted(sign_data)
        try:
            signature = base64.urlsafe_b64decode(signature_b64)
        except Exception:
            await self.send_error(request_id, "Invalid signature encoding")
            return
        if not EdDSA.verify(payload, signature, user.public_key):
            await self.send_error(request_id, "Invalid signature")
            return
        if timestamp:
            now = time.time()
            if abs(now - timestamp) > 300:
                await self.send_error(request_id, "Timestamp too old")
                return
        # Обновляем TTL текущей сессии, не создаём новую
        await self.gateway.update_session_ttl(self.session_id)
        resp_data = {"public_key": user.public_key}
        await self.send_success(request_id, resp_data)
        logger.info(f"Session refreshed for {email}")

    async def handle_message(self, message: bytes):
        if len(message) < 4:
            logger.warning("Слишком короткое сообщение")
            return
        stream_id = int.from_bytes(message[:4], "big")
        data = message[4:]
        if stream_id == STREAM_ID_CONTROL:
            await self.handle_control(data)
        else:
            await self.handle_file_stream(stream_id, data)

    async def handle_control(self, data: bytes):
        """Обработка управляющих сообщений с полным логированием и всеми проверками"""
        logger.info(f"📥 [CONTROL] Получен пакет от {self.email or 'unknown'}, длина={len(data)}")

        if not self.is_authenticated:
            try:
                obj = deserialize(data)
                logger.info(f"📥 [CONTROL] Plain объект (pre-auth): {obj}")
                await self.process_pre_auth(obj)
                return
            except Exception as e:
                logger.error(f"❌ [CONTROL] Ошибка десериализации pre-auth: {e}")
                return

        try:
            decrypted = decrypt_control(data, self.session_key)
            logger.info(f"📥 [CONTROL] Расшифровано: {decrypted[:200]}")
        except Exception as e:
            logger.error(f"❌ [CONTROL] Ошибка расшифровки: {e}", exc_info=True)
            try:
                obj = deserialize(data)
                logger.info(f"📥 [CONTROL] Plain объект (fallback): {obj}")
                if obj.get("type") == "handshake_response":
                    await self.process_pre_auth(obj)
                    return
            except Exception:
                pass
            return

        try:
            obj = deserialize(decrypted)
            logger.info(f"📥 [CONTROL] Объект: {obj}")
        except Exception as e:
            logger.error(f"❌ [CONTROL] Ошибка десериализации JSON: {e}")
            return

        if "command" not in obj:
            logger.warning(f"⚠️ [CONTROL] Нет поля command, объект: {obj}")
            return

        request_id = obj.get("request_id", 0)

        # Проверка сессии
        session = await self.gateway.get_session(self.session_id)
        if not session:
            logger.error(f"❌ [CONTROL] Сессия не найдена для {self.session_id}")
            await self.send_error(request_id, "Session expired")
            await self.close_connection("Session expired")
            return

        # Обновление TTL сессии для всех команд
        await self.gateway.update_session_ttl(self.session_id)

        # Rate limit
        if not self.check_rate_limit():
            await self.send_error(request_id, "Превышен лимит команд (10/сек)")
            return

        # Защита от replay-атак
        if await self.gateway.security_service.is_request_replayed(
            self.session_id, request_id
        ):
            await self.send_error(request_id, "Replay attack detected")
            return

        # Проверка EdDSA-подписи (для не-ботов)
        if not self.is_bot:
            if not await self.gateway.security_service.verify_signature(obj, self.email):
                logger.warning(f"⚠️ [PROCESS] Invalid signature from {self.email}")
                await self.send_error(request_id, "Неверная подпись")
                return

        # Передаём обработку команды в CommandService
        await self.gateway.command_service.handle_command(self, obj)

    async def process_pre_auth(self, obj: dict):
        msg_type = obj.get("type")
        if msg_type == "handshake":
            await self.handle_handshake(obj)
        elif msg_type == REQUEST_CODE_TYPE:
            await self.handle_request_code(obj)
        elif msg_type == "auth_request":
            await self.handle_auth_request(obj)
        elif msg_type == BOT_AUTH_REQUEST:
            await self.handle_bot_auth(obj)
        elif msg_type == REFRESH_SESSION_TYPE:
            await self.handle_refresh_session(obj)
        else:
            logger.warning(f"Неизвестный pre-auth тип: {msg_type}")

    async def handle_handshake(self, obj: dict):
        client_pub_b64 = obj.get("public_key")
        if not client_pub_b64:
            await self.send_error(0, "Отсутствует public_key")
            return
        try:
            client_pub_raw = base64.urlsafe_b64decode(client_pub_b64)
            if len(client_pub_raw) != 32:
                raise ValueError
        except Exception:
            await self.send_error(0, "Некорректный public_key")
            return
        self.client_public_key = client_pub_raw
        resp = self.gateway.auth_service.build_handshake_response()
        await self.send_control(serialize(resp), encrypted=False)

    async def handle_request_code(self, obj: dict):
        email = obj.get("email")
        if not email:
            await self.send_error(0, "Email не указан")
            return
        if not validate_email(email):
            await self.send_error(0, "Некорректный email")
            return
        success, msg = await self.gateway.auth_service.request_code(email)
        if success:
            await self.send_success(0, {"message": msg})
        else:
            await self.send_error(0, msg)

    
    
    async def handle_auth_request(self, obj: dict):
        email = obj.get("email")
        sms_code = obj.get("sms_code")
        eddsa_public_key = obj.get("eddsa_public_key")
        if not email or not sms_code or not eddsa_public_key:
            await self.send_error(0, "Недостаточно данных для аутентификации")
            return
        if self.client_public_key is None:
            await self.send_error(0, "Не выполнен handshake")
            return
        success, session_key, user, error_msg = await self.gateway.auth_service.authenticate(
            email, sms_code, eddsa_public_key, self.client_public_key
        )
        if not success:
            await self.send_error(0, error_msg or "Authentication failed")
            return
        self.session_key = session_key
        self.email = email
        self.is_authenticated = True
        self.session_id = secrets.token_hex(16)          # <-- генерируем ID
        self.gateway.connections[self.session_id] = self
        await self.gateway.create_session(
            self.session_id,
            email,                                       # <-- передаём email
            session_key,
            self.client_public_key,
            is_bot=False,
            ip=self.ip,
            user_agent=self.user_agent,
        )
        resp_data = {"public_key": user.public_key}
        if not user.first_name:
            resp_data["requires_first_name"] = True
        await self.send_success(0, resp_data)
        logger.info(f"Пользователь {email} аутентифицирован")
    
    def check_rate_limit(self) -> bool:
        now = time.time()
        self.command_timestamps = [t for t in self.command_timestamps if now - t < 1.0]
        if len(self.command_timestamps) >= RATE_LIMIT:
            return False
        self.command_timestamps.append(now)
        return True

    async def session_timeout(self):
        await asyncio.sleep(SESSION_TIMEOUT)
        if self.is_authenticated:
            logger.info(f"Сессия {self.email} истекла по таймауту")
            await self.close_connection("Session timeout")

    async def close_connection(self, reason: str = ""):
        if self._pending_tasks:
            for task in list(self._pending_tasks):
                task.cancel()
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)
            self._pending_tasks.clear()
        if self.session_id:
            await self.gateway.delete_session(self.session_id)
        if self.email and self.email in self.gateway.connections:
            del self.gateway.connections[self.email]
        self.is_authenticated = False
        self.session_key = None
        if self.timeout_task:
            self.timeout_task.cancel()
        self.processed_request_ids.clear()
        try:
            await self.websocket.close(code=1000, reason=reason)
        except Exception:
            pass

    async def handle_file_stream(self, stream_id: int, data: bytes):
        if not self.is_authenticated:
            logger.warning("Файловый поток до аутентификации")
            return
        owner = self.gateway.file_service.get_stream_owner(stream_id)
        if owner is not None and owner != self.email:
            logger.warning(f"Попытка использовать чужой stream_id {stream_id} от {self.email}")
            return
        stream_key = derive_stream_key(self.session_key, self.email, stream_id)
        if stream_id not in self.gateway.file_service.file_streams:
            if not data:
                logger.warning("Пустой заголовок файлового потока")
                return
            try:
                decrypted_header = decrypt_stream(data, stream_key)
            except Exception as e:
                logger.error(f"Ошибка расшифровки заголовка: {e}")
                return
            await self.gateway.file_service.start_upload(
                stream_id, decrypted_header, self.email
            )
        else:
            if data:
                try:
                    decrypted_data = decrypt_stream(data, stream_key)
                except Exception as e:
                    logger.error(f"Ошибка расшифровки чанка: {e}")
                    return
                await self.gateway.file_service.append_data(
                    stream_id, decrypted_data, self.email
                )
            else:
                await self.gateway.file_service.finish_upload(
                    stream_id, success=True, email=self.email
                )


async def main():
    gateway = Gateway()
    await gateway.init_redis()
    async def handler(websocket):
        remote_addr = websocket.remote_address
        ip = remote_addr[0] if remote_addr else None
        # Получаем User‑Agent из заголовков запроса
        headers = websocket.request.headers  # вместо websocket.request_headers
        user_agent = headers.get("User-Agent") if headers else None
        proto = ChatServerProtocol(websocket, gateway, ip=ip, user_agent=user_agent)
        try:
            async for message in websocket:
                await proto.handle_message(message)
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Соединение закрыто: {proto.email}")
        finally:
            await proto.close_connection()
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certfile="certs/cert.pem", keyfile="certs/key.pem")
    async with websockets.serve(handler, "0.0.0.0", 4433, ssl=ssl_context, max_size=2**31):
        logger.info("Сервер запущен на wss://127.0.0.1:4433")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
