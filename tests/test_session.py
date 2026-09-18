import pytest
import asyncio
import time
from unittest.mock import AsyncMock
from gateway import Gateway
from services.command_service import CommandService
from services.redis_store import RedisStore
from services.postgres_store import PostgresStore


@pytest.mark.asyncio
async def test_session_management(postgres_store: PostgresStore, redis_store: RedisStore, command_service: CommandService):
    # --- 1. Подготовка ---
    gw = Gateway()
    gw.db_store = postgres_store
    gw.redis_store = redis_store

    gw.auth_service = AsyncMock()
    gw.bot_service = AsyncMock()
    gw.security_service = AsyncMock()
    gw.file_service = AsyncMock()
    gw.user_service = AsyncMock()
    gw.entity_service = AsyncMock()
    gw.profile_service = AsyncMock()
    gw.message_service = AsyncMock()
    gw.reaction_service = AsyncMock()
    gw.sticker_service = AsyncMock()
    gw.analytics_service = AsyncMock()
    gw.spam_service = AsyncMock()

    command_service.gateway = gw

    # --- 2. Создаём пользователя ---
    email = "test@example.com"
    await postgres_store.create_user(email, "public_key")

    session_key = b"fake_session_key_32_bytes!!"
    client_pub = b"fake_client_pub_32_bytes!!!"

    # --- 3. Создаём две сессии ---
    session_id1 = "session1"
    await gw.create_session(
        session_id1, email, session_key, client_pub,
        is_bot=False, ip="192.168.1.1", user_agent="Mozilla/5.0"
    )

    session_id2 = "session2"
    await gw.create_session(
        session_id2, email, session_key, client_pub,
        is_bot=False, ip="10.0.0.1", user_agent="curl/7.68"
    )

    # ДОБАВЛЯЕМ МОК-ПРОТОКОЛ ДЛЯ ВТОРОЙ СЕССИИ
    proto2 = AsyncMock()
    proto2.close_connection = AsyncMock()
    gw.connections[session_id2] = proto2

    # --- 4. Мок протокола для текущей сессии ---
    protocol = AsyncMock()
    protocol.email = email
    protocol.session_id = session_id1
    protocol.is_bot = False
    protocol.bot = None
    protocol.send_success = AsyncMock()
    protocol.send_error = AsyncMock()

    # --- 5. Тест получения списка сессий ---
    result = await command_service._cmd_get_sessions(protocol, 1, {})
    sessions = result["sessions"]
    assert len(sessions) == 2

    current = next(s for s in sessions if s["is_current"])
    other = next(s for s in sessions if not s["is_current"])
    assert current["session_id"] == session_id1
    assert current["ip"] == "192.168.1.1"
    assert current["user_agent"] == "Mozilla/5.0"
    assert other["session_id"] == session_id2
    assert other["ip"] == "10.0.0.1"
    assert other["user_agent"] == "curl/7.68"

    # --- 6. Попытка завершить текущую сессию → ошибка ---
    with pytest.raises(ValueError, match="Нельзя завершить текущую сессию"):
        await command_service._cmd_terminate_session(protocol, 2, {"session_id": session_id1})

    # --- 7. Попытка завершить другую сессию до истечения 30 минут → ошибка ---
    with pytest.raises(ValueError, match="Текущая сессия должна быть активна более 30 минут"):
        await command_service._cmd_terminate_session(protocol, 3, {"session_id": session_id2})

    # --- 8. Искусственно состарим сессии ---
    now = time.time()
    old_time = now - 2000

    async def patch_created_at(session_id: str, new_time: float):
        key = f"session:{session_id}"
        await redis_store.redis.hset(key, "created_at", str(new_time))
        await redis_store.redis.hset(key, "last_active", str(new_time))

    await patch_created_at(session_id1, old_time)
    await patch_created_at(session_id2, old_time)

    # --- 9. Теперь завершение другой сессии должно быть успешным ---
    await command_service._cmd_terminate_session(protocol, 4, {"session_id": session_id2})

    # Проверяем, что сессия удалена из Redis
    session_data = await gw.get_session(session_id2)
    assert session_data is None

    # Проверяем, что в списке осталась только текущая сессия
    result_after = await command_service._cmd_get_sessions(protocol, 5, {})
    sessions_after = result_after["sessions"]
    assert len(sessions_after) == 1
    assert sessions_after[0]["session_id"] == session_id1

    # Даём время на выполнение асинхронных задач (close_connection вызывается через create_task)
    await asyncio.sleep(0.1)

    # Проверяем, что close_connection был вызван
    proto2.close_connection.assert_awaited_once_with("Session terminated by user")