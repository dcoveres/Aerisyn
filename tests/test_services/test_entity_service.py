import pytest
from services.entity_service import EntityService
from protocol import ENTITY_TYPE_CHAT, ENTITY_TYPE_GROUP


@pytest.mark.asyncio
async def test_create_entity(entity_service, test_user):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_CHAT, "Chat with Alice", email, "alice@example.com")
    assert entity.id is not None
    assert entity.type == ENTITY_TYPE_CHAT
    members = await entity_service.db_store.get_entity_members(entity.id)
    assert set(members) == {email, "alice@example.com"}

    entity2 = await entity_service.create_entity(ENTITY_TYPE_GROUP, "My Group", email, target_email=None)
    assert entity2.type == ENTITY_TYPE_GROUP
    members2 = await entity_service.db_store.get_entity_members(entity2.id)
    assert members2 == [email]


@pytest.mark.asyncio
async def test_join_leave_entity(entity_service, test_user):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    other_email = "other@example.com"
    await entity_service.db_store.create_user(other_email, "other_key")
    assert await entity_service.add_user_to_entity(other_email, entity.id) is True
    members = await entity_service.db_store.get_entity_members(entity.id)
    assert other_email in members

    assert await entity_service.remove_user_from_entity(other_email, entity.id) is True
    members2 = await entity_service.db_store.get_entity_members(entity.id)
    assert other_email not in members2

    # Владелец не может выйти
    assert await entity_service.remove_user_from_entity(email, entity.id) is False


@pytest.mark.asyncio
async def test_promote_demote_admin(entity_service, test_user):
    email, _ = test_user
    other = "other@example.com"
    await entity_service.db_store.create_user(other, "other_key")
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    await entity_service.add_user_to_entity(other, entity.id)

    assert await entity_service.promote_admin(entity.id, other, email) is True
    admins = await entity_service.db_store.get_entity_admins(entity.id)
    assert other in admins

    assert await entity_service.demote_admin(entity.id, other, email) is True
    admins2 = await entity_service.db_store.get_entity_admins(entity.id)
    assert other not in admins2


@pytest.mark.asyncio
async def test_set_entity_username(entity_service, test_user):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    assert await entity_service.set_entity_username(entity.id, "coolgroup", email) is True
    entity_db = await entity_service.db_store.get_entity(entity.id)
    assert entity_db.username == "coolgroup"

    # Попытка установить занятый
    entity2 = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group2", email)
    await entity_service.set_entity_username(entity2.id, "taken", email)
    assert await entity_service.set_entity_username(entity.id, "taken", email) is False

    # Для чата нельзя
    chat = await entity_service.create_entity(ENTITY_TYPE_CHAT, "Chat", email, "alice@example.com")
    assert await entity_service.set_entity_username(chat.id, "chatname", email) is False