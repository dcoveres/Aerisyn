import pytest
from services.bot_service import BotService
from protocol import MAX_BOTS_PER_USER


@pytest.mark.asyncio
async def test_create_bot(bot_service, test_user):
    email, _ = test_user
    bot = await bot_service.create_bot(email, "MyBot", "my_bot")
    assert bot is not None
    assert bot.username == "my_bot"
    assert bot.owner_email == email

    # Проверка лимита
    for i in range(MAX_BOTS_PER_USER - 1):
        new_bot = await bot_service.create_bot(email, f"Bot{i}", f"bot{i}")
        assert new_bot is not None

    # Попытка создать 11-го
    bot11 = await bot_service.create_bot(email, "TooMany", "toomany")
    assert bot11 is None


@pytest.mark.asyncio
async def test_delete_bot(bot_service, test_user):
    email, _ = test_user
    bot = await bot_service.create_bot(email, "ToDelete", "deletebot")
    assert await bot_service.delete_bot(bot.id, email) is True
    # Нельзя удалить чужого
    other = "other@example.com"
    await bot_service.db_store.create_user(other, "key")
    bot2 = await bot_service.create_bot(other, "OtherBot", "otherbot")
    assert await bot_service.delete_bot(bot2.id, email) is False


@pytest.mark.asyncio
async def test_get_bot_by_token(bot_service, test_user):
    email, _ = test_user
    bot = await bot_service.create_bot(email, "TokenBot", "tokenbot")
    fetched = await bot_service.get_bot_by_token(bot.token)
    assert fetched.id == bot.id