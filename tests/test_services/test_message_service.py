import pytest
from services.message_service import MessageService
from protocol import ENTITY_TYPE_CHAT, ENTITY_TYPE_GROUP, ENTITY_TYPE_CHANNEL


@pytest.mark.asyncio
async def test_add_message(message_service, test_user, entity_service):
    email, _ = test_user
    other = "alice@example.com"
    await message_service.db_store.create_user(other, "other_key")
    entity = await entity_service.create_entity(ENTITY_TYPE_CHAT, "Chat", email, other)
    msg = await message_service.add_message(email, entity.id, "Hello")
    assert msg is not None
    assert msg.content == "Hello"

    outsider = "outsider@example.com"
    await message_service.db_store.create_user(outsider, "key")
    msg2 = await message_service.add_message(outsider, entity.id, "hi")
    assert msg2 is None


@pytest.mark.asyncio
async def test_add_file_message(message_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    msg = await message_service.add_file_message(email, entity.id, "file.txt", 1024, "checksum", "/path")
    assert msg.type == "file"
    assert msg.filename == "file.txt"


@pytest.mark.asyncio
async def test_get_messages(message_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    await message_service.add_message(email, entity.id, "msg1")
    await message_service.add_message(email, entity.id, "msg2")
    msgs = await message_service.get_messages(email, entity.id, 10)
    assert len(msgs) == 2
    assert msgs[0].content == "msg1"
    assert msgs[1].content == "msg2"


@pytest.mark.asyncio
async def test_delete_message(message_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    msg = await message_service.add_message(email, entity.id, "delete me")
    assert await message_service.delete_message(email, entity.id, msg.id) is True
    assert await message_service.delete_message(email, entity.id, msg.id) is False


@pytest.mark.asyncio
async def test_delete_message_permissions(message_service, test_user, entity_service):
    email, _ = test_user
    other = "other@example.com"
    await message_service.db_store.create_user(other, "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    await entity_service.add_user_to_entity(other, entity.id)
    msg = await message_service.add_message(other, entity.id, "other msg")
    # Владелец может удалить любое сообщение в своей сущности
    assert await message_service.delete_message(email, entity.id, msg.id) is True