import asyncio
import pytest
import pytest_asyncio
import tempfile
import shutil
import secrets
from unittest.mock import AsyncMock
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import os
import sys
sys.path.insert(0, os.getcwd())

from db.models import Base
from services.postgres_store import PostgresStore
from services.redis_store import RedisStore


# ---------- Мок Redis с поддержкой множеств ----------
class FakeRedisAsync:
    def __init__(self):
        self._data = {}
        self._expires = {}
        self._sets = {}

    async def get(self, key: str) -> str | None:
        if key in self._expires and asyncio.get_event_loop().time() > self._expires[key]:
            self._data.pop(key, None)
            self._expires.pop(key, None)
            return None
        return self._data.get(key)

    async def set(self, key: str, value: str, **kwargs) -> bool:
        self._data[key] = value
        if "ex" in kwargs:
            self._expires[key] = asyncio.get_event_loop().time() + kwargs["ex"]
        return True

    async def incr(self, key: str) -> int:
        if key in self._expires and asyncio.get_event_loop().time() > self._expires[key]:
            self._data.pop(key, None)
            self._expires.pop(key, None)
        if key not in self._data:
            self._data[key] = "0"
        new_val = int(self._data[key]) + 1
        self._data[key] = str(new_val)
        return new_val

    async def hset(self, name: str, key: str = None, value: str = None, mapping: dict = None) -> int:
        if name not in self._data:
            self._data[name] = {}
        if mapping:
            self._data[name].update(mapping)
        else:
            self._data[name][key] = value
        return 1

    async def hgetall(self, name: str) -> dict:
        return self._data.get(name, {})

    async def delete(self, *keys) -> int:
        count = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                count += 1
            if key in self._expires:
                del self._expires[key]
                count += 1
            if key in self._sets:
                del self._sets[key]
                count += 1
        return count

    async def expire(self, name: str, time: int) -> bool:
        self._expires[name] = asyncio.get_event_loop().time() + time
        return True

    async def setnx(self, key: str, value: str) -> bool:
        if key in self._data:
            return False
        self._data[key] = value
        return True

    async def scan(self, cursor: int, match: str = None, count: int = 10):
        keys = [k for k in self._data.keys() if match is None or k.startswith(match.replace("*", ""))]
        return (0, keys[:count])

    async def sadd(self, key: str, *values: str) -> int:
        if key not in self._sets:
            self._sets[key] = set()
        added = 0
        for v in values:
            if v not in self._sets[key]:
                self._sets[key].add(v)
                added += 1
        return added

    async def srem(self, key: str, *values: str) -> int:
        if key not in self._sets:
            return 0
        removed = 0
        for v in values:
            if v in self._sets[key]:
                self._sets[key].remove(v)
                removed += 1
        return removed

    async def smembers(self, key: str) -> set:
        return self._sets.get(key, set()).copy()

    async def scard(self, key: str) -> int:
        return len(self._sets.get(key, set()))


# ---------- Фикстуры для БД ----------
@pytest_asyncio.fixture(scope="function")
async def db_engine():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def postgres_store(db_engine):
    store = PostgresStore(db_url="sqlite+aiosqlite:///:memory:")
    store.engine = db_engine
    store.async_session = async_sessionmaker(db_engine, expire_on_commit=False)
    return store


# ---------- Мок Redis с фикстурой ----------
@pytest_asyncio.fixture(scope="function")
async def mock_redis():
    return FakeRedisAsync()


@pytest_asyncio.fixture(scope="function")
async def redis_store(mock_redis):
    store = RedisStore(redis_url="redis://localhost")
    store.redis = mock_redis
    return store


# ---------- Сервисные фикстуры ----------
@pytest_asyncio.fixture(scope="function")
async def user_service(postgres_store):
    from services.user_service import UserService
    return UserService(postgres_store)


@pytest_asyncio.fixture(scope="function")
async def entity_service(postgres_store, redis_store):
    from services.entity_service import EntityService
    return EntityService(postgres_store, redis_store)


@pytest_asyncio.fixture(scope="function")
async def message_service(postgres_store, redis_store, gateway_mock):
    from services.message_service import MessageService
    return MessageService(postgres_store, redis_store, gateway_mock)


@pytest_asyncio.fixture(scope="function")
async def security_service(postgres_store, redis_store):
    from services.security_service import SecurityService
    return SecurityService(postgres_store, redis_store)


@pytest_asyncio.fixture(scope="function")
async def auth_service(postgres_store, redis_store, send_code_service_mock):
    from services.auth_service import AuthService
    from axiso import DH
    dh_keys = DH.generate_keypair()
    return AuthService(postgres_store, redis_store, dh_keys["private_key"], send_code_service_mock)


@pytest_asyncio.fixture(scope="function")
async def bot_service(postgres_store, redis_store):
    from services.bot_service import BotService
    return BotService(postgres_store, redis_store)


@pytest_asyncio.fixture(scope="function")
async def file_service(postgres_store, redis_store, gateway_mock):
    from services.file_service import FileService
    return FileService(postgres_store, redis_store, gateway_mock)


@pytest_asyncio.fixture(scope="function")
async def reaction_service(postgres_store, redis_store):
    from services.reaction_service import ReactionService
    return ReactionService(postgres_store, redis_store)


@pytest_asyncio.fixture(scope="function")
async def poll_service(postgres_store, redis_store):
    from services.poll_service import PollService
    return PollService(postgres_store, redis_store)


# ---------- Моки ----------
@pytest.fixture
def send_code_service_mock():
    mock = AsyncMock()
    mock.send_code = AsyncMock(return_value=True)
    return mock


@pytest.fixture
def gateway_mock():
    mock = AsyncMock()
    mock.send_control_to_entity = AsyncMock()
    mock.broadcast_file = AsyncMock()
    mock.connections = {}
    return mock


# ---------- Тестовый пользователь ----------
@pytest_asyncio.fixture
async def test_user(postgres_store):
    email = "test@example.com"
    public_key = "some_ed25519_public_key_b64"
    await postgres_store.create_user(email, public_key)
    return email, public_key


# ---------- Фикстура для Gateway ----------
@pytest_asyncio.fixture(scope="function")
async def gateway(postgres_store, redis_store):
    from gateway import Gateway
    import base64
    from axiso import DH
    from cryptography.hazmat.primitives import serialization
    import hashlib

    gw = Gateway()
    gw.db_store = postgres_store
    gw.redis_store = redis_store

    dh_keys = DH.generate_keypair()
    gw.private_key_b64 = dh_keys["private_key"]
    gw.public_key_b64 = dh_keys["public_key"]
    private_raw = base64.urlsafe_b64decode(gw.private_key_b64)
    from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
    priv = X25519PrivateKey.from_private_bytes(private_raw)
    pub = priv.public_key()
    pub_bytes = pub.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    gw.fingerprint = hashlib.sha256(pub_bytes).hexdigest()[:8]

    gw.auth_service = AsyncMock()
    gw.auth_service.build_handshake_response = lambda: {
        "type": "handshake_response",
        "public_key": gw.public_key_b64,
        "fingerprint": gw.fingerprint,
    }
    gw.auth_service.request_code = AsyncMock(return_value=(True, "Code sent"))
    gw.auth_service.authenticate = AsyncMock(return_value=(True, b"session_key", None, ""))

    gw.bot_service = AsyncMock()
    gw.bot_service.get_bot_by_token = AsyncMock(return_value=None)

    gw.security_service = AsyncMock()
    gw.security_service.is_request_replayed = AsyncMock(return_value=False)
    gw.security_service.verify_signature = AsyncMock(return_value=True)

    gw.command_service = AsyncMock()
    gw.file_service = AsyncMock()
    gw.user_service = AsyncMock()
    gw.entity_service = AsyncMock()
    gw.profile_service = AsyncMock()
    gw.message_service = AsyncMock()
    gw.reaction_service = AsyncMock()
    gw.sticker_service = AsyncMock()
    gw.analytics_service = AsyncMock()
    gw.spam_service = AsyncMock()

    gw.connections = {}
    gw.create_session = AsyncMock()
    gw.get_session = AsyncMock(return_value=None)
    gw.delete_session = AsyncMock()
    gw.update_session_ttl = AsyncMock()
    gw.send_control_to_entity = AsyncMock()

    return gw


# ---------- Дополнительные фикстуры ----------
@pytest_asyncio.fixture(scope="function")
async def sticker_service(postgres_store, redis_store):
    from services.stickers_service import StickerService
    tmp_dir = tempfile.mkdtemp(prefix="stickers_")
    yield StickerService(postgres_store, redis_store, sticker_dir=tmp_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest_asyncio.fixture(scope="function")
async def analytics_service(postgres_store):
    from services.analytics_service import AnalyticsService
    return AnalyticsService(postgres_store)


@pytest_asyncio.fixture(scope="function")
async def profile_service(postgres_store, user_service):
    from services.profile_service import ProfileService
    return ProfileService(postgres_store, user_service)


# ---------- ФИКСТУРА COMMAND_SERVICE ----------
@pytest_asyncio.fixture(scope="function")
async def command_service(postgres_store, redis_store, user_service, entity_service,
                          message_service, file_service, auth_service,
                          profile_service, security_service, bot_service,
                          reaction_service, sticker_service, analytics_service,
                          poll_service, gateway_mock):
    from services.command_service import CommandService
    return CommandService(
        db_store=postgres_store,
        redis_store=redis_store,
        user_service=user_service,
        entity_service=entity_service,
        message_service=message_service,
        file_service=file_service,
        auth_service=auth_service,
        profile_service=profile_service,
        security_service=security_service,
        bot_service=bot_service,
        reaction_service=reaction_service,
        sticker_service=sticker_service,
        analytics_service=analytics_service,
        poll_service=poll_service,
        gateway=gateway_mock,
    )


# ---------- Фикстура для WebSocket мока ----------
@pytest.fixture
def websocket_mock():
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.close = AsyncMock()
    return ws