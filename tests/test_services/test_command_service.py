import pytest
import time
import secrets
import os
from unittest.mock import AsyncMock
from services.command_service import CommandService
from services.models import User, Entity, Message, Bot, Sticker
from protocol import ENTITY_TYPE_CHAT, ENTITY_TYPE_GROUP, ENTITY_TYPE_CHANNEL
import asyncio

# ---------- Вспомогательный протокол мок ----------
@pytest.fixture
def protocol_mock():
    proto = AsyncMock()
    proto.email = "test@example.com"
    proto.is_bot = False
    proto.bot = None
    proto.session_key = b"key"
    proto.send_success = AsyncMock()
    proto.send_error = AsyncMock()
    proto.send_raw = AsyncMock()
    return proto

# ---------- Тесты ----------
@pytest.mark.asyncio
async def test_cmd_create_entity_chat(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("target@example.com", "key")
    params = {"type": ENTITY_TYPE_CHAT, "name": "", "target": "target@example.com"}
    result = await command_service._cmd_create_entity(protocol_mock, 1, params)
    assert "entity_id" in result
    entity_id = result["entity_id"]
    entity = await postgres_store.get_entity(entity_id)
    assert entity is not None
    members = await postgres_store.get_entity_members(entity_id)
    assert "test@example.com" in members
    assert "target@example.com" in members

@pytest.mark.asyncio
async def test_cmd_create_entity_group(command_service, protocol_mock, postgres_store):
    params = {"type": ENTITY_TYPE_GROUP, "name": "MyGroup"}
    result = await command_service._cmd_create_entity(protocol_mock, 1, params)
    assert result["entity_id"] is not None
    entity = await postgres_store.get_entity(result["entity_id"])
    assert entity.name == "MyGroup"
    assert entity.type == ENTITY_TYPE_GROUP

@pytest.mark.asyncio
async def test_cmd_join_entity_public(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "PublicGroup", "owner@example.com")
    await postgres_store.create_user("test@example.com", "key")
    await postgres_store.update_entity_private(entity.id, False)
    params = {"entity_id": entity.id}
    result = await command_service._cmd_join_entity(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    members = await postgres_store.get_entity_members(entity.id)
    assert "test@example.com" in members

@pytest.mark.asyncio
async def test_cmd_join_entity_private_request(command_service, protocol_mock, postgres_store, entity_service, gateway_mock):
    admin_email = "owner@example.com"
    await postgres_store.create_user(admin_email, "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "PrivateGroup", admin_email)
    await postgres_store.create_user("test@example.com", "key")
    await postgres_store.update_entity_private(entity.id, True)

    # Добавляем мок-протокол для админа, чтобы уведомление могло отправиться
    admin_proto = AsyncMock()
    admin_proto.session_key = secrets.token_bytes(32)
    gateway_mock.connections[admin_email] = admin_proto

    params = {"entity_id": entity.id}
    result = await command_service._cmd_join_entity(protocol_mock, 1, params)
    assert result["status"] == "request_submitted"

@pytest.mark.asyncio
async def test_cmd_leave_entity_group(command_service, protocol_mock, postgres_store, entity_service):
    await postgres_store.create_user("owner@example.com", "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "owner@example.com")
    await postgres_store.add_member(entity.id, "test@example.com")
    protocol_mock.email = "test@example.com"
    params = {"entity_id": entity.id}
    result = await command_service._cmd_leave_entity(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    members = await postgres_store.get_entity_members(entity.id)
    assert "test@example.com" not in members

@pytest.mark.asyncio
async def test_cmd_leave_entity_chat_deletes(command_service, protocol_mock, postgres_store, entity_service):
    await postgres_store.create_user("other@example.com", "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_CHAT, "", "test@example.com", "other@example.com")
    params = {"entity_id": entity.id}
    result = await command_service._cmd_leave_entity(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    assert await postgres_store.get_entity(entity.id) is None

@pytest.mark.asyncio
async def test_cmd_delete_entity(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "ToDelete", "test@example.com")
    params = {"entity_id": entity.id}
    result = await command_service._cmd_delete_entity(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    assert await postgres_store.get_entity(entity.id) is None

@pytest.mark.asyncio
async def test_cmd_send_message(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    params = {"entity_id": entity.id, "content": "Hello"}
    result = await command_service._cmd_send_message(protocol_mock, 1, params)
    assert result["message_id"] is not None
    msgs = await postgres_store.get_messages(entity.id, 10)
    assert len(msgs) == 1
    assert msgs[0].content == "Hello"

@pytest.mark.asyncio
async def test_cmd_send_message_with_reply(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    msg1 = await command_service._cmd_send_message(protocol_mock, 1, {"entity_id": entity.id, "content": "Original"})
    params = {"entity_id": entity.id, "content": "Reply", "reply_to": {"entity_id": entity.id, "message_id": msg1["message_id"]}}
    result = await command_service._cmd_send_message(protocol_mock, 2, params)
    assert result["message_id"] is not None
    msg2 = await postgres_store.get_message(entity.id, result["message_id"])
    assert msg2.reply_to_message_id == msg1["message_id"]

@pytest.mark.asyncio
async def test_cmd_get_messages(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    for i in range(3):
        await command_service._cmd_send_message(protocol_mock, i, {"entity_id": entity.id, "content": f"msg{i}"})
    params = {"entity_id": entity.id, "limit": 2}
    result = await command_service._cmd_get_messages(protocol_mock, 1, params)
    assert "messages" in result
    assert len(result["messages"]) == 2
    # Сообщения возвращаются в обратном порядке (сначала новые)
    assert result["messages"][0]["content"] == "msg0"
    assert result["messages"][1]["content"] == "msg1"

@pytest.mark.asyncio
async def test_cmd_delete_message(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    msg = await command_service._cmd_send_message(protocol_mock, 1, {"entity_id": entity.id, "content": "delete me"})
    params = {"entity_id": entity.id, "message_id": msg["message_id"]}
    result = await command_service._cmd_delete_message(protocol_mock, 2, params)
    assert result["message_id"] == msg["message_id"]
    assert await postgres_store.get_message(entity.id, msg["message_id"]) is None

@pytest.mark.asyncio
async def test_cmd_promote_admin(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.create_user("admin@example.com", "key")
    await postgres_store.add_member(entity.id, "admin@example.com")
    params = {"entity_id": entity.id, "email": "admin@example.com"}
    result = await command_service._cmd_promote_admin(protocol_mock, 1, params)
    assert result["email"] == "admin@example.com"
    admins = await postgres_store.get_entity_admins(entity.id)
    assert "admin@example.com" in admins

@pytest.mark.asyncio
async def test_cmd_demote_admin(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.create_user("admin@example.com", "key")
    await postgres_store.add_member(entity.id, "admin@example.com")
    await postgres_store.add_admin(entity.id, "admin@example.com")
    params = {"entity_id": entity.id, "email": "admin@example.com"}
    result = await command_service._cmd_demote_admin(protocol_mock, 1, params)
    assert result["email"] == "admin@example.com"
    admins = await postgres_store.get_entity_admins(entity.id)
    assert "admin@example.com" not in admins

@pytest.mark.asyncio
async def test_cmd_get_user_info_user(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("target@example.com", "key")
    user = await postgres_store.get_user("target@example.com")
    user.username = "tuser"
    user.first_name = "Target"
    await postgres_store.update_user(user)
    params = {"target": "@tuser"}
    result = await command_service._cmd_get_user_info(protocol_mock, 1, params)
    assert result["username"] == "tuser"
    assert result["first_name"] == "Target"

@pytest.mark.asyncio
async def test_cmd_get_user_info_bot(command_service, protocol_mock, postgres_store, bot_service):
    await postgres_store.create_user("owner@example.com", "key")
    bot = await bot_service.create_bot("owner@example.com", "TestBot", "testbot")
    params = {"target": "@testbot"}
    result = await command_service._cmd_get_user_info(protocol_mock, 1, params)
    assert result["is_bot"] is True
    assert result["first_name"] == "TestBot"

@pytest.mark.asyncio
async def test_cmd_set_username(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    params = {"username": "newuser"}
    result = await command_service._cmd_set_username(protocol_mock, 1, params)
    assert result["username"] == "newuser"
    user = await postgres_store.get_user("test@example.com")
    assert user.username == "newuser"

@pytest.mark.asyncio
async def test_cmd_set_first_name(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    params = {"name": "Alice"}
    result = await command_service._cmd_set_first_name(protocol_mock, 1, params)
    assert result["first_name"] == "Alice"
    user = await postgres_store.get_user("test@example.com")
    assert user.first_name == "Alice"

@pytest.mark.asyncio
async def test_cmd_set_bio(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    params = {"bio": "My bio"}
    result = await command_service._cmd_set_bio(protocol_mock, 1, params)
    assert result["bio"] == "My bio"
    user = await postgres_store.get_user("test@example.com")
    assert user.bio == "My bio"

@pytest.mark.asyncio
async def test_cmd_set_show_email(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    params = {"show": False}
    result = await command_service._cmd_set_show_email(protocol_mock, 1, params)
    assert result["show_email"] is False
    user = await postgres_store.get_user("test@example.com")
    assert user.show_email is False

@pytest.mark.asyncio
async def test_cmd_set_entity_username(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    params = {"entity_id": entity.id, "username": "groupname"}
    result = await command_service._cmd_set_entity_username(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    assert result["username"] == "groupname"
    entity_after = await postgres_store.get_entity(entity.id)
    assert entity_after.username == "groupname"

@pytest.mark.asyncio
async def test_cmd_create_bot(command_service, protocol_mock, postgres_store):
    await postgres_store.create_user("test@example.com", "key")
    params = {"name": "MyBot", "username": "mybot"}
    result = await command_service._cmd_create_bot(protocol_mock, 1, params)
    assert result["bot_id"] is not None
    assert result["username"] == "mybot"
    bot = await postgres_store.get_bot(result["bot_id"])
    assert bot is not None
    assert bot.owner_email == "test@example.com"

@pytest.mark.asyncio
async def test_cmd_delete_bot(command_service, protocol_mock, postgres_store, bot_service):
    await postgres_store.create_user("test@example.com", "key")
    bot = await bot_service.create_bot("test@example.com", "ToDelete", "deletebot")
    params = {"bot_id": bot.id}
    result = await command_service._cmd_delete_bot(protocol_mock, 1, params)
    assert result["bot_id"] == bot.id
    assert await postgres_store.get_bot(bot.id) is None

@pytest.mark.asyncio
async def test_cmd_list_bots(command_service, protocol_mock, postgres_store, bot_service):
    await postgres_store.create_user("test@example.com", "key")
    await bot_service.create_bot("test@example.com", "Bot1", "bot1")
    await bot_service.create_bot("test@example.com", "Bot2", "bot2")
    params = {}
    result = await command_service._cmd_list_bots(protocol_mock, 1, params)
    assert "bots" in result
    assert len(result["bots"]) == 2
    names = sorted([b["name"] for b in result["bots"]])
    assert names == ["Bot1", "Bot2"]

@pytest.mark.asyncio
async def test_cmd_get_bot_token(command_service, protocol_mock, postgres_store, bot_service):
    await postgres_store.create_user("test@example.com", "key")
    bot = await bot_service.create_bot("test@example.com", "TokenBot", "tokenbot")
    params = {"bot_id": bot.id}
    result = await command_service._cmd_get_bot_token(protocol_mock, 1, params)
    assert result["token"] == bot.token

@pytest.mark.asyncio
async def test_cmd_invite_bot(command_service, protocol_mock, postgres_store, entity_service, bot_service):
    await postgres_store.create_user("test@example.com", "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    bot = await bot_service.create_bot("owner@example.com", "Bot", "botuser")
    params = {"entity_id": entity.id, "bot": str(bot.id)}
    result = await command_service._cmd_invite_bot(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    assert result["bot_id"] == bot.id
    members = await postgres_store.get_entity_members(entity.id)
    assert f"bot_{bot.id}" in members

@pytest.mark.asyncio
async def test_cmd_get_entity_info_group(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.update_entity_username(entity.id, "group")
    params = {"entity_id": entity.id}
    result = await command_service._cmd_get_entity_info(protocol_mock, 1, params)
    assert result["name"] == "Group"
    assert result["type"] == ENTITY_TYPE_GROUP
    assert result["username"] == "group"

@pytest.mark.asyncio
async def test_cmd_get_entity_info_chat(command_service, protocol_mock, postgres_store, entity_service):
    await postgres_store.create_user("other@example.com", "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_CHAT, "", "test@example.com", "other@example.com")
    params = {"entity_id": entity.id}
    result = await command_service._cmd_get_entity_info(protocol_mock, 1, params)
    assert result["type"] == ENTITY_TYPE_CHAT
    assert result["target"] == "other@example.com"

@pytest.mark.asyncio
async def test_cmd_add_reaction(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    msg = await command_service._cmd_send_message(protocol_mock, 1, {"entity_id": entity.id, "content": "test"})
    params = {"message_id": msg["message_id"], "reaction_type": "❤️", "entity_id": entity.id}
    result = await command_service._cmd_add_reaction(protocol_mock, 1, params)
    assert result["message_id"] == msg["message_id"]
    reactions = await postgres_store.get_reactions_for_message(msg["message_id"])
    assert reactions.get("❤️") == 1

@pytest.mark.asyncio
async def test_cmd_remove_reaction(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    msg = await command_service._cmd_send_message(protocol_mock, 1, {"entity_id": entity.id, "content": "test"})
    await command_service._cmd_add_reaction(protocol_mock, 1, {"message_id": msg["message_id"], "reaction_type": "😂", "entity_id": entity.id})
    params = {"message_id": msg["message_id"], "entity_id": entity.id}
    result = await command_service._cmd_remove_reaction(protocol_mock, 1, params)
    assert result["message_id"] == msg["message_id"]
    reactions = await postgres_store.get_reactions_for_message(msg["message_id"])
    assert reactions.get("😂") is None

@pytest.mark.asyncio
async def test_cmd_get_stickers(command_service, protocol_mock, postgres_store, sticker_service):
    sticker = Sticker(id=1, name="funny", file_path=os.path.join(sticker_service.sticker_dir, "1.webp"), tags=["fun"])
    await postgres_store.create_sticker(sticker)
    params = {"query": "fun", "limit": 10}
    result = await command_service._cmd_get_stickers(protocol_mock, 1, params)
    assert "stickers" in result
    assert len(result["stickers"]) == 1
    assert result["stickers"][0]["name"] == "funny"

@pytest.mark.asyncio
async def test_cmd_send_sticker(command_service, protocol_mock, postgres_store, entity_service, sticker_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    sticker = Sticker(id=2, name="cool", file_path=os.path.join(sticker_service.sticker_dir, "2.webp"), tags=[])
    await postgres_store.create_sticker(sticker)
    params = {"entity_id": entity.id, "sticker_id": sticker.id}
    result = await command_service._cmd_send_sticker(protocol_mock, 1, params)
    assert result["message_id"] is not None
    msgs = await postgres_store.get_messages(entity.id, 10)
    assert len(msgs) == 1
    assert msgs[0].type == "sticker"
    assert msgs[0].filename == "sticker_2.webp"

@pytest.mark.asyncio
async def test_cmd_get_sticker_file(command_service, protocol_mock, postgres_store, sticker_service):
    sticker_dir = sticker_service.sticker_dir
    os.makedirs(sticker_dir, exist_ok=True)
    file_path = os.path.join(sticker_dir, "test.webp")
    with open(file_path, "wb") as f:
        f.write(b"fake webp")
    sticker = Sticker(id=3, name="test", file_path=file_path, tags=[])
    await postgres_store.create_sticker(sticker)
    params = {"sticker_id": sticker.id}
    result = await command_service._cmd_get_sticker_file(protocol_mock, 1, params)
    assert "data" in result
    assert result["content_type"] == "image/webp"

@pytest.mark.asyncio
async def test_cmd_set_entity_private(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    params = {"entity_id": entity.id, "is_private": True}
    result = await command_service._cmd_set_entity_private(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    assert result["is_private"] is True
    entity_after = await postgres_store.get_entity(entity.id)
    assert entity_after.is_private is True

@pytest.mark.asyncio
async def test_cmd_get_join_requests(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.create_user("requester@example.com", "key")
    await postgres_store.create_join_request(entity.id, "requester@example.com", time.time() + 3600)
    params = {"entity_id": entity.id}
    result = await command_service._cmd_get_join_requests(protocol_mock, 1, params)
    assert "requests" in result
    assert len(result["requests"]) == 1
    assert result["requests"][0]["email"] == "requester@example.com"

@pytest.mark.asyncio
async def test_cmd_approve_join_request(command_service, protocol_mock, postgres_store, entity_service, gateway_mock):
    await postgres_store.create_user("test@example.com", "key")  # владелец — test@example.com
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.create_user("requester@example.com", "key")
    await postgres_store.create_join_request(entity.id, "requester@example.com", time.time() + 3600)

    requester_proto = AsyncMock()
    requester_proto.session_key = secrets.token_bytes(32)
    gateway_mock.connections["requester@example.com"] = requester_proto

    params = {"entity_id": entity.id, "email": "requester@example.com"}
    result = await command_service._cmd_approve_join_request(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    members = await postgres_store.get_entity_members(entity.id)
    assert "requester@example.com" in members

@pytest.mark.asyncio
async def test_cmd_reject_join_request(command_service, protocol_mock, postgres_store, entity_service, gateway_mock):
    await postgres_store.create_user("test@example.com", "key")
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.create_user("requester@example.com", "key")
    await postgres_store.create_join_request(entity.id, "requester@example.com", time.time() + 3600)

    requester_proto = AsyncMock()
    requester_proto.session_key = secrets.token_bytes(32)
    gateway_mock.connections["requester@example.com"] = requester_proto

    params = {"entity_id": entity.id, "email": "requester@example.com"}
    result = await command_service._cmd_reject_join_request(protocol_mock, 1, params)
    assert result["entity_id"] == entity.id
    req = await postgres_store.get_join_request(entity.id, "requester@example.com")
    assert req is None

@pytest.mark.asyncio
async def test_cmd_callback_query(command_service, protocol_mock, postgres_store, entity_service, redis_store, gateway_mock):
    # создаём владельца бота
    await postgres_store.create_user("owner@example.com", "key")
    bot = await command_service.bot_service.create_bot("owner@example.com", "Bot", f"bot_{secrets.token_hex(4)}")
    assert bot is not None
    bot_email = f"bot_{bot.id}"
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    await postgres_store.add_member(entity.id, bot_email)
    msg = await command_service.message_service.add_message(bot_email, entity.id, "Click me")
    # сохраняем callback данные
    reply_markup = {"inline_keyboard": [[{"text": "Click", "callback_data": "data"}]]}
    await redis_store.save_callback_data(entity.id, msg.id, reply_markup)
    params = {"entity_id": entity.id, "message_id": msg.id, "callback_data": "data"}
    result = await command_service._cmd_callback_query(protocol_mock, 1, params)
    assert result["status"] == "sent"

@pytest.mark.asyncio
async def test_cmd_get_messages(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    for i in range(3):
        await command_service._cmd_send_message(protocol_mock, i, {"entity_id": entity.id, "content": f"msg{i}"})
        await asyncio.sleep(0.01)  # гарантируем разные timestamp
    params = {"entity_id": entity.id, "limit": 2}
    result = await command_service._cmd_get_messages(protocol_mock, 1, params)
    assert "messages" in result
    assert len(result["messages"]) == 2
    # Сообщения возвращаются в хронологическом порядке (сначала старые)
    assert result["messages"][0]["content"] == "msg1"
    assert result["messages"][1]["content"] == "msg2"
    
@pytest.mark.asyncio
async def test_cmd_answer_callback(command_service, protocol_mock):
    params = {}
    result = await command_service._cmd_answer_callback(protocol_mock, 1, params)
    assert result["status"] == "ok"

@pytest.mark.asyncio
async def test_cmd_get_analytics(command_service, protocol_mock, postgres_store, entity_service):
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", "test@example.com")
    for i in range(3):
        await command_service._cmd_send_message(protocol_mock, i, {"entity_id": entity.id, "content": f"msg{i}"})
    params = {"entity_id": entity.id}
    result = await command_service._cmd_get_analytics(protocol_mock, 1, params)
    assert "member_count" in result
    assert result["member_count"] == 1
    assert "messages_7d" in result
    assert result["messages_7d"] >= 3
