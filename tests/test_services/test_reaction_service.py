# test_reaction_service.py
import pytest
import asyncio
from services.reaction_service import ReactionService
from protocol import REACTION_RATE_LIMIT, REACTION_RATE_WINDOW


@pytest.mark.asyncio
async def test_add_reaction(reaction_service, test_user, entity_service, message_service):
    """Успешное добавление реакции."""
    email, _ = test_user
    entity = await entity_service.create_entity("group", "Test Group", email)
    msg = await message_service.add_message(email, entity.id, "Hello")
    result = await reaction_service.add_reaction(msg.id, email, "👍", entity.id)
    assert result is True

    reactions = await reaction_service.db_store.get_reactions_for_message(msg.id)
    assert reactions.get("👍") == 1
    user_reaction = await reaction_service.db_store.get_user_reaction(msg.id, email)
    assert user_reaction == "👍"


@pytest.mark.asyncio
async def test_add_reaction_rate_limit(reaction_service, test_user, entity_service, message_service):
    """Превышение лимита реакций должно возвращать False."""
    email, _ = test_user
    entity = await entity_service.create_entity("group", "Group", email)
    msg = await message_service.add_message(email, entity.id, "msg")

    # Делаем REACTION_RATE_LIMIT успешных добавлений (разные реакции, чтобы не заменять)
    for i in range(REACTION_RATE_LIMIT):
        result = await reaction_service.add_reaction(msg.id, email, f"r{i}", entity.id)
        assert result is True

    # Следующий запрос должен быть отклонён
    result = await reaction_service.add_reaction(msg.id, email, "last", entity.id)
    assert result is False


@pytest.mark.asyncio
async def test_remove_reaction(reaction_service, test_user, entity_service, message_service):
    """Успешное удаление существующей реакции."""
    email, _ = test_user
    entity = await entity_service.create_entity("group", "Group", email)
    msg = await message_service.add_message(email, entity.id, "test")
    await reaction_service.add_reaction(msg.id, email, "😂", entity.id)

    result = await reaction_service.remove_reaction(msg.id, email)
    assert result is True

    reactions = await reaction_service.db_store.get_reactions_for_message(msg.id)
    assert reactions.get("😂") is None
    user_reaction = await reaction_service.db_store.get_user_reaction(msg.id, email)
    assert user_reaction is None


@pytest.mark.asyncio
async def test_remove_reaction_not_found(reaction_service, test_user, entity_service, message_service):
    """Удаление несуществующей реакции возвращает False."""
    email, _ = test_user
    entity = await entity_service.create_entity("group", "Group", email)
    msg = await message_service.add_message(email, entity.id, "test")
    result = await reaction_service.remove_reaction(msg.id, email)
    assert result is False


@pytest.mark.asyncio
async def test_get_reactions_for_messages(reaction_service, test_user, entity_service, message_service):
    """Получение реакций для нескольких сообщений с информацией о реакции текущего пользователя."""
    email, _ = test_user
    other_email = "other@example.com"
    await reaction_service.db_store.create_user(other_email, "other_key")

    entity = await entity_service.create_entity("group", "Group", email)
    # ⚠️ ВАЖНО: добавляем other_email в члены сущности
    assert await entity_service.add_user_to_entity(other_email, entity.id) is True

    msg1 = await message_service.add_message(email, entity.id, "msg1")
    msg2 = await message_service.add_message(email, entity.id, "msg2")

    # Добавляем реакции с проверкой успешности
    assert await reaction_service.add_reaction(msg1.id, email, "❤️", entity.id) is True
    assert await reaction_service.add_reaction(msg1.id, other_email, "❤️", entity.id) is True
    assert await reaction_service.add_reaction(msg2.id, email, "😂", entity.id) is True

    # Проверяем напрямую в БД, что у msg1 две реакции ❤️
    raw_counts = await reaction_service.db_store.get_reactions_for_message(msg1.id)
    assert raw_counts.get("❤️") == 2

    result = await reaction_service.get_reactions_for_messages([msg1.id, msg2.id], email)

    assert msg1.id in result
    assert result[msg1.id]["counts"] == {"❤️": 2}
    assert result[msg1.id]["my_reaction"] == "❤️"

    assert msg2.id in result
    assert result[msg2.id]["counts"] == {"😂": 1}
    assert result[msg2.id]["my_reaction"] == "😂"

@pytest.mark.asyncio
async def test_add_reaction_update_existing(reaction_service, test_user, entity_service, message_service):
    """Если пользователь уже поставил реакцию, новая реакция заменяет старую."""
    email, _ = test_user
    entity = await entity_service.create_entity("group", "Group", email)
    msg = await message_service.add_message(email, entity.id, "text")

    # Ставим первую
    await reaction_service.add_reaction(msg.id, email, "😊", entity.id)
    # Ставим другую – должна замениться
    result = await reaction_service.add_reaction(msg.id, email, "😎", entity.id)
    assert result is True

    reactions = await reaction_service.db_store.get_reactions_for_message(msg.id)
    assert reactions.get("😊") is None
    assert reactions.get("😎") == 1
    user_reaction = await reaction_service.db_store.get_user_reaction(msg.id, email)
    assert user_reaction == "😎"


@pytest.mark.asyncio
async def test_add_reaction_rate_limit_with_window(reaction_service, test_user, entity_service, message_service):
    """Проверка, что лимит сбрасывается после окна (используем time.sleep)."""
    email, _ = test_user
    entity = await entity_service.create_entity("group", "Group", email)
    msg = await message_service.add_message(email, entity.id, "msg")

    # Заполняем лимит
    for i in range(REACTION_RATE_LIMIT):
        await reaction_service.add_reaction(msg.id, email, f"r{i}", entity.id)

    # Следующий должен быть отклонён
    assert await reaction_service.add_reaction(msg.id, email, "last", entity.id) is False

    # Ждём окно + небольшой запас
    await asyncio.sleep(REACTION_RATE_WINDOW + 1)

    # Теперь должно быть разрешено
    assert await reaction_service.add_reaction(msg.id, email, "new", entity.id) is True