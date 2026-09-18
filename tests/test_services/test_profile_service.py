import pytest
from services.models import User

@pytest.mark.asyncio
async def test_set_username(profile_service, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    assert await profile_service.set_username("test@example.com", "newuser") is True
    user = await postgres_store.get_user("test@example.com")
    assert user.username == "newuser"

@pytest.mark.asyncio
async def test_set_first_name(profile_service, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    assert await profile_service.set_first_name("test@example.com", "Alice") is True
    user = await postgres_store.get_user("test@example.com")
    assert user.first_name == "Alice"

@pytest.mark.asyncio
async def test_set_bio(profile_service, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    assert await profile_service.set_bio("test@example.com", "My bio") is True
    user = await postgres_store.get_user("test@example.com")
    assert user.bio == "My bio"

@pytest.mark.asyncio
async def test_set_show_email(profile_service, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    assert await profile_service.set_show_email("test@example.com", False) is True
    user = await postgres_store.get_user("test@example.com")
    assert user.show_email is False

@pytest.mark.asyncio
async def test_get_user_info_own(profile_service, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    user = await postgres_store.get_user("test@example.com")
    user.username = "testuser"
    user.first_name = "Test"
    user.bio = "bio"
    await postgres_store.update_user(user)
    info = await profile_service.get_user_info("test@example.com", "test@example.com")
    assert info["username"] == "testuser"
    assert info["first_name"] == "Test"
    assert info["email"] == "test@example.com"

@pytest.mark.asyncio
async def test_get_user_info_other_hidden_email(profile_service, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    user = await postgres_store.get_user("test@example.com")
    user.show_email = False
    await postgres_store.update_user(user)
    info = await profile_service.get_user_info("test@example.com", "other@example.com")
    assert info["email"] is None