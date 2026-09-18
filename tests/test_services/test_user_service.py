import pytest
from services.user_service import UserService

@pytest.mark.asyncio
async def test_create_user(postgres_store):
    service = UserService(postgres_store)
    email = "test@example.com"
    pub_key = "pub123"
    user = await service.get_or_create_user(email, pub_key)
    assert user.email == email
    assert user.public_key == pub_key
    # Повторный вызов должен вернуть существующего
    user2 = await service.get_or_create_user(email, "new_key")
    assert user2.public_key == pub_key  # не изменился

@pytest.mark.asyncio
async def test_set_username(postgres_store, test_user):
    email, pub_key = test_user
    service = UserService(postgres_store)
    # Установка корректного юзернейма
    assert await service.set_username(email, "new_user") is True
    user = await postgres_store.get_user(email)
    assert user.username == "new_user"

    # Попытка установить слишком короткий
    assert await service.set_username(email, "abc") is False
    # Попытка установить занятый юзернейм
    await postgres_store.create_user("other@example.com", "other_key")
    await service.set_username("other@example.com", "taken")
    assert await service.set_username(email, "taken") is False  # занят

@pytest.mark.asyncio
async def test_set_first_name(postgres_store, test_user):
    email, _ = test_user
    service = UserService(postgres_store)
    assert await service.set_first_name(email, "Alice")
    user = await postgres_store.get_user(email)
    assert user.first_name == "Alice"

@pytest.mark.asyncio
async def test_get_user_by_identifier(postgres_store):
    service = UserService(postgres_store)
    email = "john@example.com"
    pub = "key"
    await postgres_store.create_user(email, pub)
    # По email
    user = await service.get_user_by_identifier(email)
    assert user.email == email
    # По username
    await service.set_username(email, "john_doe")
    user2 = await service.get_user_by_identifier("@john_doe")
    assert user2.email == email

@pytest.mark.asyncio
async def test_get_display_name(postgres_store):
    service = UserService(postgres_store)
    email = "alice@example.com"
    await postgres_store.create_user(email, "key")
    # По умолчанию email
    assert await service.get_display_name(email) == email
    # С установленным username
    await service.set_username(email, "alice")
    assert await service.get_display_name(email) == "alice"

@pytest.mark.asyncio
async def test_get_user_info(postgres_store):
    service = UserService(postgres_store)
    email = "bob@example.com"
    await postgres_store.create_user(email, "key")
    await service.set_first_name(email, "Bob")
    info = await service.get_user_info(email)
    assert info["username"] is None
    assert info["first_name"] == "Bob"
    assert info["email"] == email  # show_email по умолчанию True
    # Скрываем email
    await service.set_show_email(email, False)
    info2 = await service.get_user_info(email, requester_email="other@example.com")
    assert "email" not in info2 or info2["email"] is None