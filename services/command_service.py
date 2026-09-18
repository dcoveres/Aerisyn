# services/command_service.py
import asyncio
import base64
import logging
import os
import re
import time
from typing import Optional, Dict, Any, List

from protocol import (
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
    CMD_REQUEST_UPLOAD_STREAM,
    CMD_SET_ENTITY_USERNAME,
    CMD_CREATE_BOT,
    CMD_DELETE_BOT,
    CMD_LIST_BOTS,
    CMD_GET_BOT_TOKEN,
    CMD_INVITE_BOT,
    CMD_REFRESH_SESSION,
    CMD_GET_ENTITY_INFO,
    CMD_ADD_REACTION,
    CMD_REMOVE_REACTION,
    CMD_GET_STICKERS,
    CMD_SEND_STICKER,
    CMD_GET_STICKER_FILE,
    CMD_SET_ENTITY_PRIVATE,
    CMD_GET_JOIN_REQUESTS,
    CMD_APPROVE_JOIN_REQUEST,
    CMD_REJECT_JOIN_REQUEST,
    CMD_CALLBACK_QUERY,
    CMD_EDIT_MESSAGE,
    CMD_ANSWER_CALLBACK,
    CMD_GET_ANALYTICS,
    NOTIFICATION_REACTION_UPDATE,
    NOTIFICATION_NEW_JOIN_REQUEST,
    STREAM_ID_CONTROL,
    encrypt_control,
    derive_stream_key,
    encrypt_stream,
    serialize,
    CMD_CREATE_POLL,
    CMD_VOTE_POLL,
    CMD_CLOSE_POLL,
    CMD_GET_SESSIONS,
    CMD_TERMINATE_SESSION,
    CMD_SEARCH,
    CMD_BLOCK_USER,
    CMD_UNBLOCK_USER,
    CMD_GET_BLACKLIST,
)
from services.models import Message
from .poll_service import PollService

logger = logging.getLogger(__name__)


class CommandService:
    def __init__(
        self,
        db_store,
        redis_store,
        user_service,
        entity_service,
        message_service,
        file_service,
        auth_service,
        profile_service,
        security_service,
        bot_service,
        reaction_service,
        sticker_service,
        analytics_service,
        poll_service,
        gateway,
        spam_service=None,
    ):
        self.db_store = db_store
        self.redis_store = redis_store
        self.user_service = user_service
        self.entity_service = entity_service
        self.message_service = message_service
        self.file_service = file_service
        self.auth_service = auth_service
        self.profile_service = profile_service
        self.security_service = security_service
        self.bot_service = bot_service
        self.reaction_service = reaction_service
        self.sticker_service = sticker_service
        self.analytics_service = analytics_service
        self.poll_service = poll_service
        self.gateway = gateway
        self.spam_service = spam_service

    async def handle_command(self, protocol, obj: dict):
        """Обрабатывает команду и отправляет ответ через protocol."""
        command = obj.get("command")
        request_id = obj.get("request_id", 0)
        params = {k: v for k, v in obj.items() if k not in ("command", "request_id", "signature")}

        # Проверка, что пользователь установил имя (кроме set_first_name)
        if command != CMD_SET_FIRST_NAME:
            if not protocol.is_bot:
                user = await self.db_store.get_user(protocol.email)
                if user and not user.first_name:
                    await protocol.send_error(request_id, "Сначала установите имя с помощью команды set_first_name")
                    return

        try:
            if command == CMD_CREATE_ENTITY:
                result = await self._cmd_create_entity(protocol, request_id, params)
            elif command == CMD_JOIN_ENTITY:
                result = await self._cmd_join_entity(protocol, request_id, params)
            elif command == CMD_LEAVE_ENTITY:
                result = await self._cmd_leave_entity(protocol, request_id, params)
            elif command == CMD_DELETE_ENTITY:
                result = await self._cmd_delete_entity(protocol, request_id, params)
            elif command == CMD_SEND_MESSAGE:
                result = await self._cmd_send_message(protocol, request_id, params)
            elif command == CMD_GET_MESSAGES:
                result = await self._cmd_get_messages(protocol, request_id, params)
            elif command == CMD_DELETE_MESSAGE:
                result = await self._cmd_delete_message(protocol, request_id, params)
            elif command == CMD_PROMOTE_ADMIN:
                result = await self._cmd_promote_admin(protocol, request_id, params)
            elif command == CMD_DEMOTE_ADMIN:
                result = await self._cmd_demote_admin(protocol, request_id, params)
            elif command == CMD_GET_USER_INFO:
                result = await self._cmd_get_user_info(protocol, request_id, params)
            elif command == CMD_DOWNLOAD_FILE:
                result = await self._cmd_download_file(protocol, request_id, params)
            elif command == CMD_SET_USERNAME:
                result = await self._cmd_set_username(protocol, request_id, params)
            elif command == CMD_SET_BIO:
                result = await self._cmd_set_bio(protocol, request_id, params)
            elif command == CMD_SET_SHOW_EMAIL:
                result = await self._cmd_set_show_email(protocol, request_id, params)
            elif command == CMD_SET_FIRST_NAME:
                result = await self._cmd_set_first_name(protocol, request_id, params)
            elif command == CMD_REQUEST_UPLOAD_STREAM:
                stream_id = await self.file_service.allocate_stream_id(protocol.email)
                await protocol.send_success(request_id, {"stream_id": stream_id})
                return
            elif command == CMD_SET_ENTITY_USERNAME:
                result = await self._cmd_set_entity_username(protocol, request_id, params)
            elif command == CMD_CREATE_BOT:
                result = await self._cmd_create_bot(protocol, request_id, params)
            elif command == CMD_DELETE_BOT:
                result = await self._cmd_delete_bot(protocol, request_id, params)
            elif command == CMD_LIST_BOTS:
                result = await self._cmd_list_bots(protocol, request_id, params)
            elif command == CMD_GET_BOT_TOKEN:
                result = await self._cmd_get_bot_token(protocol, request_id, params)
            elif command == CMD_INVITE_BOT:
                result = await self._cmd_invite_bot(protocol, request_id, params)
            elif command == CMD_REFRESH_SESSION:
                await self.gateway.update_session_ttl(protocol.session_id)
                await protocol.send_success(request_id, {})
                return
            elif command == CMD_GET_ENTITY_INFO:
                result = await self._cmd_get_entity_info(protocol, request_id, params)
            elif command == CMD_ADD_REACTION:
                result = await self._cmd_add_reaction(protocol, request_id, params)
            elif command == CMD_REMOVE_REACTION:
                result = await self._cmd_remove_reaction(protocol, request_id, params)
            elif command == CMD_GET_STICKERS:
                result = await self._cmd_get_stickers(protocol, request_id, params)
            elif command == CMD_SEND_STICKER:
                result = await self._cmd_send_sticker(protocol, request_id, params)
            elif command == CMD_GET_STICKER_FILE:
                result = await self._cmd_get_sticker_file(protocol, request_id, params)
            elif command == CMD_SET_ENTITY_PRIVATE:
                result = await self._cmd_set_entity_private(protocol, request_id, params)
            elif command == CMD_GET_JOIN_REQUESTS:
                result = await self._cmd_get_join_requests(protocol, request_id, params)
            elif command == CMD_APPROVE_JOIN_REQUEST:
                result = await self._cmd_approve_join_request(protocol, request_id, params)
            elif command == CMD_REJECT_JOIN_REQUEST:
                result = await self._cmd_reject_join_request(protocol, request_id, params)
            elif command == CMD_CALLBACK_QUERY:
                result = await self._cmd_callback_query(protocol, request_id, params)
            elif command == CMD_EDIT_MESSAGE:
                result = await self._cmd_edit_message(protocol, request_id, params)
            elif command == CMD_ANSWER_CALLBACK:
                result = await self._cmd_answer_callback(protocol, request_id, params)
            elif command == CMD_GET_ANALYTICS:
                result = await self._cmd_get_analytics(protocol, request_id, params)
            elif command == CMD_CREATE_POLL:
                result = await self._cmd_create_poll(protocol, request_id, params)
            elif command == CMD_VOTE_POLL:
                result = await self._cmd_vote_poll(protocol, request_id, params)
            elif command == CMD_CLOSE_POLL:
                result = await self._cmd_close_poll(protocol, request_id, params)
            elif command == CMD_GET_SESSIONS:
                result = await self._cmd_get_sessions(protocol, request_id, params)
            elif command == CMD_TERMINATE_SESSION:
                result = await self._cmd_terminate_session(protocol, request_id, params)
            elif command == CMD_SEARCH:
                result = await self._cmd_search(protocol, request_id, params)
            elif command == CMD_BLOCK_USER:
                result = await self._cmd_block_user(protocol, request_id, params)
            elif command == CMD_UNBLOCK_USER:
                result = await self._cmd_unblock_user(protocol, request_id, params)
            elif command == CMD_GET_BLACKLIST:
                result = await self._cmd_get_blacklist(protocol, request_id, params)
            else:
                await protocol.send_error(request_id, f"Неизвестная команда: {command}")
                return
        except ValueError as e:
            # ValueError используется во всех обработчиках команд как
            # управляемая, безопасная для показа пользователю ошибка.
            logger.info(f"Command {command} rejected: {e}")
            await protocol.send_error(request_id, str(e))
            return
        except Exception as e:
            # Непредвиденные ошибки (баги, сбои БД/Redis и т.п.) не должны
            # утекать во внешний ответ — там может быть чувствительная
            # информация о внутреннем устройстве сервера.
            logger.error(f"Command {command} error: {e}", exc_info=True)
            await protocol.send_error(request_id, "Внутренняя ошибка сервера")
            return

        if result is not None:
            await protocol.send_success(request_id, result)

    # ---- Вспомогательные методы ----
    async def _cmd_create_poll(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        question = params.get("question")
        options = params.get("options")
        if not entity_id or not question or not options:
            raise ValueError("entity_id, question, options required")
        if not isinstance(options, list) or len(options) < 2 or len(options) > 8:
            raise ValueError("options must be list of 2-8 strings")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")
        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("Not a member")
        banned = await self.db_store.get_entity_banned(entity_id)
        if protocol.email in banned:
            raise ValueError("You are banned")
        if entity.type == ENTITY_TYPE_CHANNEL:
            admins = await self.db_store.get_entity_admins(entity_id)
            if protocol.email != entity.owner and protocol.email not in admins:
                raise ValueError("No permission")
 
        msg_id = await self.redis_store.get_next_message_id(entity_id)
        msg = Message(
            id=msg_id,
            from_email=protocol.email,
            content=question,
            timestamp=time.time(),
            type="poll",
            entity_id=entity_id,
        )
        await self.db_store.add_message(msg)
        poll = await self.poll_service.create_poll(entity_id, msg_id, question, options, protocol.email)
        if not poll:
            raise ValueError("Failed to create poll")

        display_name = await self._get_sender_name(protocol.email)
        notification = {
            "type": "new_message",
            "entity_id": entity_id,
            "message_id": msg_id,
            "from": display_name,
            "content": question,
            "timestamp": msg.timestamp,
            "poll": {
                "id": poll.id,
                "question": question,
                "options": options,
                "total_votes": 0,
                "is_closed": False,
                "user_vote": None,
            },
        }
        await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=protocol.email)
        return {"message_id": msg_id, "poll_id": poll.id}
    
    async def _cmd_search(self, protocol, request_id: int, params: dict):
        query = params.get("query", "").strip()
        if not query:
            raise ValueError("Query cannot be empty")

        raw_limit = params.get("limit", 50)
        if not isinstance(raw_limit, int) or isinstance(raw_limit, bool):
            raw_limit = 50
        limit = max(1, min(raw_limit, 100))

        users = await self.db_store.search_users(query, limit)
        entities = await self.db_store.search_entities(query, limit)
        bots = await self.db_store.search_bots(query, limit)

        logger.info(f"Search results for '{query}': users={len(users)}, entities={len(entities)}, bots={len(bots)}")

        results = users + entities + bots
        return {"results": results}
        
    async def _cmd_vote_poll(self, protocol, request_id: int, params: dict):
        poll_id = params.get("poll_id")
        option_index = params.get("option_index")
        if poll_id is None or option_index is None:
            raise ValueError("poll_id and option_index required")
        poll = await self.db_store.get_poll(poll_id)
        if not poll:
            raise ValueError("Poll not found")
        entity_id = poll.entity_id
        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("Not a member")
        banned = await self.db_store.get_entity_banned(entity_id)
        if protocol.email in banned:
            raise ValueError("You are banned")
        success = await self.poll_service.vote(poll_id, protocol.email, option_index)
        if not success:
            raise ValueError("Cannot vote (already voted or poll closed)")

        poll_updated = await self.poll_service.get_results(poll_id)
        user_vote = await self.poll_service.get_user_vote(poll_id, protocol.email)
        notification = {
            "type": "poll_update",
            "entity_id": entity_id,
            "message_id": poll.message_id,
            "poll_id": poll.id,
            "poll_data": poll_updated,
            "user_vote": user_vote,
        }
        await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=None)
        return {"status": "ok"}

    async def _cmd_close_poll(self, protocol, request_id: int, params: dict):
        poll_id = params.get("poll_id")
        if poll_id is None:
            raise ValueError("poll_id required")
        poll = await self.db_store.get_poll(poll_id)
        if not poll:
            raise ValueError("Poll not found")
        entity = await self.entity_service.get_entity(poll.entity_id)
        if not entity:
            raise ValueError("Entity not found")
        if protocol.email != poll.created_by and protocol.email != entity.owner:
            raise ValueError("Only creator or owner can close poll")
        success = await self.poll_service.close_poll(poll_id)
        if not success:
            raise ValueError("Failed to close poll")

        poll_updated = await self.poll_service.get_results(poll_id)
        notification = {
            "type": "poll_update",
            "entity_id": poll.entity_id,
            "message_id": poll.message_id,
            "poll_id": poll.id,
            "poll_data": poll_updated,
            "user_vote": None,
        }
        await self.gateway.send_control_to_entity(poll.entity_id, notification, exclude_email=None)
        return {"status": "ok"}
    
    @staticmethod
    def _read_file_bytes(path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    async def _get_sender_name(self, from_email: str) -> str:
        if from_email.startswith("bot_"):
            bot_id = int(from_email.split("_", 1)[1])
            bot = await self.db_store.get_bot(bot_id)
            return bot.name if bot else from_email
        else:
            return await self.user_service.get_display_name(from_email)

    async def _build_reply_to(self, message: Message) -> Optional[dict]:
        if not message.reply_to_message_id:
            return None
        orig = await self.db_store.get_message(
            message.reply_to_entity_id, message.reply_to_message_id
        )
        if not orig:
            return {
                "deleted": True,
                "from": "неизвестен",
                "content": "Сообщение удалено",
                "entity_id": message.reply_to_entity_id,
                "message_id": message.reply_to_message_id,
            }
        display_name = await self._get_sender_name(orig.from_email)
        if orig.type == "text":
            content = orig.content[:200] + ("..." if len(orig.content) > 200 else "")
        elif orig.type == "file":
            content = f"Файл: {orig.filename}"
        elif orig.type == "sticker":
            content = "Стикер"
        else:
            content = "Сообщение"
        return {
            "deleted": False,
            "from": display_name,
            "content": content,
            "entity_id": message.reply_to_entity_id,
            "message_id": message.reply_to_message_id,
        }

    async def _broadcast_reaction_update(self, entity_id: int, message_id: int, exclude_email: str):
        reactions = await self.reaction_service.get_reactions_for_message(message_id)
        notification = {
            "type": NOTIFICATION_REACTION_UPDATE,
            "entity_id": entity_id,
            "message_id": message_id,
            "reactions": reactions,
        }
        await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=exclude_email)

    # ---- Команды ----
    
    async def _cmd_create_entity(self, protocol, request_id: int, params: dict):
        entity_type = params.get("type")
        name = params.get("name")
        target = params.get("target")
        if entity_type not in (ENTITY_TYPE_CHAT, ENTITY_TYPE_GROUP, ENTITY_TYPE_CHANNEL):
            raise ValueError("Некорректный тип сущности")
        if entity_type != ENTITY_TYPE_CHAT and not name:
            raise ValueError("Не указано имя")
        target_email = None
        if entity_type == ENTITY_TYPE_CHAT:
            if not target:
                raise ValueError("Для чата необходимо указать target")
            user = await self.user_service.get_user_by_identifier(target)
            if user:
                target_email = user.email
                name = await self.user_service.get_display_name(target_email)
            else:
                username = target[1:] if target.startswith('@') else target
                bot = await self.bot_service.get_bot_by_username(username)
                if not bot:
                    if target.startswith('bot_'):
                        try:
                            bot_id = int(target.split('_')[1])
                            bot = await self.db_store.get_bot(bot_id)
                        except Exception:
                            pass
                if bot:
                    target_email = f"bot_{bot.id}"
                    name = bot.name
                else:
                    raise ValueError("Пользователь или бот не найден")

            # ===== НОВЫЙ БЛОК (добавьте эти строки) =====
            # Проверка блокировки
            if await self.db_store.is_blocked(protocol.email, target_email):
                raise ValueError("Вы заблокировали этого пользователя")
            if await self.db_store.is_blocked(target_email, protocol.email):
                raise ValueError("Этот пользователь заблокировал вас")

            # Поиск существующего чата
            existing = await self.entity_service.find_chat_between(protocol.email, target_email)
            if existing:
                return {"entity_id": existing.id, "key": existing.key, "target": target_email}

            # Анти-спам: если нас недавно заблокировали без ответа за то, что
            # мы первыми написали незнакомцу, временно нельзя начинать новые
            # личные чаты. Ботов (включая @spambot) это не касается.
            if self.spam_service and not target_email.startswith("bot_"):
                await self.spam_service.check_can_write_first(protocol.email)
            # ============================================
        else:
            target_email = None

        entity = await self.entity_service.create_entity(
            entity_type, name, protocol.email, target_email
        )
        if entity is None:
            raise ValueError("Не удалось создать сущность (некорректные данные или чат уже существует)")
        result = {"entity_id": entity.id, "key": entity.key}
        if entity_type == ENTITY_TYPE_CHAT and target_email:
            result["target"] = target_email
        return result

    async def _cmd_join_entity(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        if entity_id is None:
            raise ValueError("Не указан entity_id")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Сущность не найдена")
        if entity.type == ENTITY_TYPE_CHAT:
            raise ValueError("Нельзя присоединиться к личному чату")
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if protocol.email in banned:
            raise ValueError("Вы забанены в этой сущности")
        if protocol.email in members:
            raise ValueError("Вы уже участник")
        if entity.is_private:
            success, msg = await self.entity_service.request_join(entity_id, protocol.email)
            if not success:
                raise ValueError(msg)
            admins = await self.db_store.get_entity_admins(entity_id)
            admins.append(entity.owner)
            notification = {
                "type": NOTIFICATION_NEW_JOIN_REQUEST,
                "entity_id": entity_id,
                "email": protocol.email,
                "display_name": await self.user_service.get_display_name(protocol.email),
            }
            payload = serialize(notification)
            for admin_email in admins:
                proto = self.gateway.connections.get(admin_email)
                if proto:
                    encrypted = encrypt_control(payload, proto.session_key)
                    packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
                    asyncio.create_task(proto.safe_send(packet))
            return {"status": "request_submitted", "message": msg}
        else:
            if not await self.entity_service.add_user_to_entity(protocol.email, entity_id):
                raise ValueError("Не удалось добавить пользователя")
            return {"entity_id": entity_id}

    async def _cmd_leave_entity(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        if entity_id is None:
            raise ValueError("Не указан entity_id")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Сущность не найдена")
        if entity.type == ENTITY_TYPE_CHAT:
            if not await self.entity_service.delete_entity(entity_id):
                raise ValueError("Ошибка при удалении чата")
            return {"entity_id": entity_id}
        if protocol.email == entity.owner:
            raise ValueError("Владелец не может покинуть сущность, только удалить")
        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("Вы не участник")
        if not await self.entity_service.remove_user_from_entity(protocol.email, entity_id):
            raise ValueError("Ошибка при выходе")
        return {"entity_id": entity_id}

    async def _cmd_delete_entity(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        if entity_id is None:
            raise ValueError("Не указан entity_id")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Сущность не найдена")
        if protocol.email != entity.owner:
            raise ValueError("Только владелец может удалить сущность")
        if not await self.entity_service.delete_entity(entity_id):
            raise ValueError("Ошибка при удалении")
        return {"entity_id": entity_id}

    async def _cmd_send_message(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        content = params.get("content")
        reply_to = params.get("reply_to")
        if entity_id is None or not content:
            raise ValueError("entity_id and content required")

        # === ПОЛУЧАЕМ СУЩНОСТЬ ===
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")

        if protocol.is_bot:
            from_email = f"bot_{protocol.bot.id}"
            members = await self.db_store.get_entity_members(entity_id)
            if from_email not in members:
                raise ValueError("Bot is not a member of this entity")
            banned = await self.db_store.get_entity_banned(entity_id)
            if from_email in banned:
                raise ValueError("Bot is banned")
            if entity.type == ENTITY_TYPE_CHANNEL:
                admins = await self.db_store.get_entity_admins(entity_id)
                if from_email != entity.owner and from_email not in admins:
                    raise ValueError("Bot has no permission to send in channel")
        else:
            from_email = protocol.email
            members = await self.db_store.get_entity_members(entity_id)
            banned = await self.db_store.get_entity_banned(entity_id)
            if from_email not in members:
                raise ValueError("You are not a member")
            if from_email in banned:
                raise ValueError("You are banned")
            if entity.type == ENTITY_TYPE_CHANNEL:
                admins = await self.db_store.get_entity_admins(entity_id)
                if from_email != entity.owner and from_email not in admins:
                    raise ValueError("No permission to send in channel")

            # Проверка блокировки для чатов
            if entity.type == ENTITY_TYPE_CHAT:
                other = await self.db_store.get_other_participant_in_chat(entity_id, from_email)
                if other:
                    if await self.db_store.is_blocked(from_email, other):
                        raise ValueError("Вы заблокированы этим пользователем")
                    if await self.db_store.is_blocked(other, from_email):
                        raise ValueError("Вы заблокировали этого пользователя")

            # Защита от угона: деактивируем код, если он отправлен в чат
            codes = re.findall(r'\b(\d{6})\b', content)
            for code in codes:
                stored = await self.redis_store.get_auth_code(from_email)
                if stored and stored.get("code") == code:
                    await self.redis_store.delete_auth_code(from_email)
                    logger.warning(f"Код подтверждения для {from_email} деактивирован из-за отправки в чате")
                    break

        msg = await self.message_service.add_message(from_email, entity_id, content, reply_to=reply_to)
        if msg is None:
            raise ValueError("Failed to send message")

        display_name = await self._get_sender_name(from_email)
        notification = {
            "type": "new_message",
            "entity_id": entity_id,
            "message_id": msg.id,
            "from": display_name,
            "content": content,
            "timestamp": msg.timestamp,
        }

        reply_markup = params.get("reply_markup")
        if reply_markup and protocol.is_bot:
            await self.redis_store.save_callback_data(entity_id, msg.id, reply_markup, ttl=86400)
            notification["reply_markup"] = reply_markup

        await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=from_email)

        # Если сообщение отправлено системному боту @spambot — сразу же
        # отвечаем от его имени статусом анти-спам ограничений отправителя.
        if (not protocol.is_bot and entity.type == ENTITY_TYPE_CHAT
                and self.spam_service and self.gateway.spambot_bot_id is not None):
            other = await self.db_store.get_other_participant_in_chat(entity_id, from_email)
            if other == f"bot_{self.gateway.spambot_bot_id}":
                await self._send_spambot_reply(entity_id, from_email)

        return {"message_id": msg.id}

    async def _send_spambot_reply(self, entity_id: int, to_email: str):
        """Генерирует и рассылает автоматический ответ @spambot со статусом ограничений."""
        try:
            status_text = await self.spam_service.get_status_text(to_email)
            bot_email = f"bot_{self.gateway.spambot_bot_id}"
            reply = await self.message_service.add_message(bot_email, entity_id, status_text)
            if reply is None:
                return
            display_name = await self._get_sender_name(bot_email)
            notification = {
                "type": "new_message",
                "entity_id": entity_id,
                "message_id": reply.id,
                "from": display_name,
                "content": status_text,
                "timestamp": reply.timestamp,
            }
            await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=bot_email)
        except Exception as e:
            logger.error(f"Не удалось отправить ответ @spambot пользователю {to_email}: {e}", exc_info=True)

    async def _cmd_get_messages(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        limit = params.get("limit", 50)
        if entity_id is None:
            raise ValueError("Не указан entity_id")
        if not isinstance(limit, int) or isinstance(limit, bool):
            limit = 50
        limit = max(1, min(limit, 200))

        msgs = await self.message_service.get_messages(protocol.email, entity_id, limit)
        msg_ids = [m.id for m in msgs]

        reactions_data = await self.reaction_service.get_reactions_for_messages(msg_ids, protocol.email)

        messages_list = []
        for m in msgs:
            # Базовая структура сообщения
            sticker_id = None
            if m.type == "sticker" and m.filename and m.filename.startswith("sticker_"):
                try:
                    sticker_id = int(m.filename.split("_")[1].split(".")[0])
                except (IndexError, ValueError):
                    sticker_id = None

            msg_dict = {
                "id": m.id,
                "from": await self._get_sender_name(m.from_email),
                "content": m.content,
                "timestamp": m.timestamp,
                "type": m.type,
                "file": {
                    "filename": m.filename,
                    "size": m.file_size,
                    "checksum": m.file_checksum,
                } if m.type == "file" else None,
                "sticker_id": sticker_id,
                "reactions": reactions_data.get(m.id, {"counts": {}, "my_reaction": None}),
                "reply_to": await self._build_reply_to(m),
            }

            # --- ДОБАВЛЯЕМ ДАННЫЕ ОПРОСА ---
            if m.type == "poll":
                poll = await self.db_store.get_poll_by_message_id(entity_id, m.id)
                if poll:
                    results = await self.poll_service.get_results(poll.id)
                    user_vote = await self.poll_service.get_user_vote(poll.id, protocol.email)
                    msg_dict["poll"] = {
                        "id": poll.id,
                        "question": poll.question,
                        "options": poll.options,
                        "total_votes": poll.total_votes,
                        "is_closed": poll.is_closed,
                        "results": results,          # содержит percentages, counts
                        "user_vote": user_vote,
                        "created_by": poll.created_by,
                    }

            # --- КНОПКИ (если есть) ---
            reply_markup = await self.redis_store.get_callback_data(entity_id, m.id)
            if reply_markup:
                msg_dict["reply_markup"] = reply_markup

            messages_list.append(msg_dict)

        return {"entity_id": entity_id, "messages": messages_list}

    async def _cmd_delete_message(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        message_id = params.get("message_id")
        if entity_id is None or message_id is None:
            raise ValueError("Не указан entity_id или message_id")
        if not await self.message_service.delete_message(protocol.email, entity_id, message_id):
            raise ValueError("Не удалось удалить сообщение (нет прав или не найдено)")
        return {"message_id": message_id}

    async def _cmd_promote_admin(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        target_email = params.get("email")
        if entity_id is None or not target_email:
            raise ValueError("Не указан entity_id или email")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")
        if entity.type == ENTITY_TYPE_CHAT:
            raise ValueError("Невозможно назначить администратора в личном чате")
        if not await self.entity_service.promote_admin(entity_id, target_email, protocol.email):
            raise ValueError("Ошибка назначения администратора")
        return {"email": target_email}

    async def _cmd_demote_admin(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        target_email = params.get("email")
        if entity_id is None or not target_email:
            raise ValueError("Не указан entity_id или email")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")
        if entity.type == ENTITY_TYPE_CHAT:
            raise ValueError("Невозможно снять администратора в личном чате")
        if not await self.entity_service.demote_admin(entity_id, target_email, protocol.email):
            raise ValueError("Ошибка снятия администратора")
        return {"email": target_email}

    async def _cmd_get_user_info(self, protocol, request_id: int, params: dict):
        target = params.get("target")
        if not target:
            raise ValueError("Не указан target (юзернейм или почта)")
        user = await self.user_service.get_user_by_identifier(target)
        if user:
            info = await self.profile_service.get_user_info(user.email, protocol.email)
            return info
        bot = None
        if target.startswith('@'):
            username = target[1:]
            bot = await self.bot_service.get_bot_by_username(username)
        else:
            if target.startswith('bot_'):
                try:
                    bot_id = int(target.split('_')[1])
                    bot = await self.db_store.get_bot(bot_id)
                except ValueError:
                    pass
            else:
                try:
                    bot_id = int(target)
                    bot = await self.db_store.get_bot(bot_id)
                except ValueError:
                    pass
        if not bot:
            raise ValueError("Пользователь или бот не найден")
        info = {
            "username": bot.username,
            "first_name": bot.name,
            "bio": "BOT",
            "public_key": bot.public_key,
            "tags": ["bot"],
            "entities": [],
            "email": None,
            "is_bot": True,
            "bot_id": bot.id,
        }
        return info

    async def _cmd_set_username(self, protocol, request_id: int, params: dict):
        username = params.get("username")
        if username == "":
            username = None
        if username is not None and not isinstance(username, str):
            raise ValueError("Некорректный юзернейм")
        if not await self.profile_service.set_username(protocol.email, username):
            raise ValueError("Не удалось установить юзернейм (недопустим или занят)")
        return {"username": username}

    async def _cmd_set_first_name(self, protocol, request_id: int, params: dict):
        name = params.get("name", "")
        if not isinstance(name, str):
            raise ValueError("Имя должно быть строкой")
        name = name[:64]
        if not await self.profile_service.set_first_name(protocol.email, name):
            raise ValueError("Не удалось установить имя")
        return {"first_name": name}

    async def _cmd_set_bio(self, protocol, request_id: int, params: dict):
        bio = params.get("bio", "")
        if not isinstance(bio, str):
            raise ValueError("Био должно быть строкой")
        if not await self.profile_service.set_bio(protocol.email, bio):
            raise ValueError("Не удалось установить био")
        return {"bio": bio}

    async def _cmd_set_show_email(self, protocol, request_id: int, params: dict):
        show = params.get("show")
        if not isinstance(show, bool):
            raise ValueError("Параметр show должен быть boolean")
        if not await self.profile_service.set_show_email(protocol.email, show):
            raise ValueError("Не удалось изменить настройку")
        return {"show_email": show}

    async def _cmd_download_file(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        message_id = params.get("message_id")
        if entity_id is None or message_id is None:
            raise ValueError("Не указан entity_id или message_id")
        file_info = await self.file_service.get_file_info(protocol.email, entity_id, message_id)
        if file_info is None:
            raise ValueError("Файл не найден или недоступен")
        if not os.path.isfile(file_info["file_path"]):
            logger.error(f"File {file_info['file_path']} not found for message {message_id}")
            raise ValueError("Файл отсутствует на сервере")
        logger.info(f"Sending file {file_info['filename']} ({file_info['size']} bytes) to {protocol.email}")
        await protocol.send_success(
            request_id,
            {
                "filename": file_info["filename"],
                "size": file_info["size"],
                "checksum": file_info["checksum"],
            },
        )
        stream_id = await self.redis_store.get_next_stream_id()
        stream_key = derive_stream_key(protocol.session_key, protocol.email, stream_id)
        try:
            file_data = await asyncio.to_thread(self._read_file_bytes, file_info["file_path"])
        except OSError:
            logger.exception(f"Failed to read file {file_info['file_path']} for download")
            raise ValueError("Не удалось прочитать файл")
        header = {
            "type": "file_header",
            "filename": file_info["filename"],
            "size": file_info["size"],
            "entity_id": entity_id,
            "checksum": file_info["checksum"],
            "message_id": message_id,
        }
        header_data = serialize(header)
        encrypted_header = encrypt_stream(header_data, stream_key)
        packet = stream_id.to_bytes(4, "big") + encrypted_header
        await protocol.send_raw(packet)
        encrypted_data = encrypt_stream(file_data, stream_key)
        packet = stream_id.to_bytes(4, "big") + encrypted_data
        await protocol.send_raw(packet)
        packet = stream_id.to_bytes(4, "big")
        await protocol.send_raw(packet)
        return None

    async def _cmd_set_entity_username(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        username = params.get("username")
        if entity_id is None:
            raise ValueError("Не указан entity_id")
        if username == "":
            username = None
        if username is not None and not isinstance(username, str):
            raise ValueError("Некорректный юзернейм")
        if not await self.entity_service.set_entity_username(entity_id, username, protocol.email):
            raise ValueError("Не удалось установить юзернейм для сущности (недостаточно прав, недопустимый или занят)")
        return {"entity_id": entity_id, "username": username}

    async def _cmd_create_bot(self, protocol, request_id: int, params: dict):
        name = params.get("name")
        username = params.get("username")
        if not name:
            raise ValueError("Bot name required")
        bot = await self.bot_service.create_bot(protocol.email, name, username)
        if not bot:
            raise ValueError("Failed to create bot (limit reached or username taken)")
        return {"bot_id": bot.id, "username": bot.username, "name": bot.name, "token": bot.token}

    async def _cmd_delete_bot(self, protocol, request_id: int, params: dict):
        bot_id = params.get("bot_id")
        if bot_id is None:
            raise ValueError("bot_id required")
        if not await self.bot_service.delete_bot(bot_id, protocol.email):
            raise ValueError("Bot not found or not owned")
        return {"bot_id": bot_id}

    async def _cmd_list_bots(self, protocol, request_id: int, params: dict):
        bots = await self.bot_service.get_bots_for_user(protocol.email)
        return {
            "bots": [
                {
                    "id": b.id,
                    "username": b.username,
                    "name": b.name,
                    "is_active": b.is_active,
                }
                for b in bots
            ]
        }

    async def _cmd_get_bot_token(self, protocol, request_id: int, params: dict):
        bot_id = params.get("bot_id")
        if bot_id is None:
            raise ValueError("bot_id required")
        bot = await self.db_store.get_bot(bot_id)
        if not bot or bot.owner_email != protocol.email:
            raise ValueError("Bot not found or not owned")
        return {"token": bot.token}

    async def _cmd_invite_bot(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        bot_identifier = params.get("bot")
        if entity_id is None or not bot_identifier:
            raise ValueError("entity_id and bot required")
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")
        if entity.type == ENTITY_TYPE_CHAT:
            raise ValueError("Невозможно пригласить бота в личный чат")
        bot = None
        if bot_identifier.startswith('@'):
            bot = await self.bot_service.get_bot_by_username(bot_identifier[1:])
        else:
            try:
                bot_id = int(bot_identifier)
                bot = await self.db_store.get_bot(bot_id)
            except ValueError:
                raise ValueError("Invalid bot identifier")
        if not bot or not bot.is_active:
            raise ValueError("Bot not found or inactive")
        admins = await self.db_store.get_entity_admins(entity_id)
        if protocol.email != entity.owner and protocol.email not in admins:
            raise ValueError("Insufficient permissions")
        bot_email = f"bot_{bot.id}"
        members = await self.db_store.get_entity_members(entity_id)
        if bot_email in members:
            raise ValueError("Bot already in entity")
        banned = await self.db_store.get_entity_banned(entity_id)
        if bot_email in banned:
            raise ValueError("Bot is banned in this entity")
        await self.db_store.add_member(entity_id, bot_email)
        return {"entity_id": entity_id, "bot_id": bot.id}

    async def _cmd_get_entity_info(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        if entity_id is None:
            raise ValueError("entity_id required")

        requester = protocol.email if not protocol.is_bot else f"bot_{protocol.bot.id}"
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")

        members = await self.db_store.get_entity_members(entity_id)
        is_member = requester in members

        # Для личных чатов доступ только у участников
        if entity.type == ENTITY_TYPE_CHAT:
            if not is_member:
                raise ValueError("Not a member of this chat")
            # Можно показывать информацию, т.к. участник подтверждён
            other = [m for m in members if m != requester]
            target_email = other[0] if other else None
            if target_email and target_email.startswith("bot_"):
                try:
                    bot_id = int(target_email.split("_", 1)[1])
                    bot = await self.db_store.get_bot(bot_id)
                    display_name = bot.name if bot else target_email
                except (ValueError, IndexError):
                    display_name = target_email
            else:
                display_name = (
                    await self.user_service.get_display_name(target_email)
                    if target_email
                    else entity.name
                )
            return {
                "type": entity.type,
                "name": display_name,
                "username": entity.username or "",
                "owner": entity.owner,
                "is_private": entity.is_private,
                "is_member": True,
                "target": target_email,
            }

        # Для групп и каналов
        if entity.type in (ENTITY_TYPE_GROUP, ENTITY_TYPE_CHANNEL):
            # Приватные сущности доступны только участникам
            if entity.is_private and not is_member:
                raise ValueError("This entity is private, you are not a member")

            result = {
                "type": entity.type,
                "username": entity.username or "",
                "is_private": entity.is_private,
                "is_member": is_member,
            }
            # Для публичных сущностей или участников — показываем имя и владельца
            if not entity.is_private or is_member:
                result["name"] = entity.name
                result["owner"] = entity.owner
            else:
                # На случай, если когда-нибудь понадобится показывать только существование
                result["name"] = None
                result["owner"] = None

            return result

        # На всякий случай, если тип неизвестен
        raise ValueError("Unknown entity type")

    async def _cmd_add_reaction(self, protocol, request_id: int, params: dict):
        message_id = params.get("message_id")
        reaction_type = params.get("reaction_type")
        entity_id = params.get("entity_id")
        if message_id is None or not reaction_type or entity_id is None:
            raise ValueError("message_id, reaction_type and entity_id required")
        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("You are not a member of this entity")
        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg:
            raise ValueError("Message not found")
        success = await self.reaction_service.add_reaction(
            message_id, protocol.email, reaction_type, entity_id
        )
        if not success:
            raise ValueError("Reaction limit exceeded or error")
        await self._broadcast_reaction_update(entity_id, message_id, protocol.email)
        return {"message_id": message_id, "reaction_type": reaction_type}

    async def _cmd_remove_reaction(self, protocol, request_id: int, params: dict):
        message_id = params.get("message_id")
        entity_id = params.get("entity_id")
        if message_id is None or entity_id is None:
            raise ValueError("message_id and entity_id required")
        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("You are not a member of this entity")
        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg:
            raise ValueError("Message not found")
        success = await self.reaction_service.remove_reaction(message_id, protocol.email)
        if not success:
            raise ValueError("Reaction not found or already removed")
        await self._broadcast_reaction_update(entity_id, message_id, protocol.email)
        return {"message_id": message_id}

    async def _cmd_get_stickers(self, protocol, request_id: int, params: dict):
        query = params.get("query")
        limit = params.get("limit", 50)
        offset = params.get("offset", 0)
        if not isinstance(limit, int) or isinstance(limit, bool):
            limit = 50
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            offset = 0
        limit = max(1, min(limit, 100))
        stickers = await self.sticker_service.get_stickers(query, limit, offset)
        return {"stickers": stickers}

    async def _cmd_send_sticker(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        sticker_id = params.get("sticker_id")
        if entity_id is None or sticker_id is None:
            raise ValueError("entity_id and sticker_id required")
        if protocol.is_bot:
            from_email = f"bot_{protocol.bot.id}"
        else:
            from_email = protocol.email
        entity = await self.entity_service.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if from_email not in members:
            raise ValueError("Not a member")
        if from_email in banned:
            raise ValueError("Banned")
        if entity.type == ENTITY_TYPE_CHANNEL:
            admins = await self.db_store.get_entity_admins(entity_id)
            if from_email != entity.owner and from_email not in admins:
                raise ValueError("No permission")
        sticker_data = await self.sticker_service.send_sticker(from_email, entity_id, sticker_id)
        if not sticker_data:
            raise ValueError("Sticker not found")
        msg = await self.message_service.add_sticker_message(
            from_email,
            entity_id,
            sticker_id,
            sticker_data["sticker_name"],
            sticker_data["file_path"],
        )
        if not msg:
            raise ValueError("Failed to send sticker")
        display_name = await self._get_sender_name(from_email)
        notification = {
            "type": "new_message",
            "entity_id": entity_id,
            "message_id": msg.id,
            "from": display_name,
            "content": sticker_data["sticker_name"],
            "timestamp": msg.timestamp,
            "sticker": {
                "id": sticker_id,
                "name": sticker_data["sticker_name"],
                "file_path": sticker_data["file_path"],
                "reply_to": await self._build_reply_to(msg),
            },
        }
        await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=from_email)
        return {"message_id": msg.id}

    async def _cmd_get_sticker_file(self, protocol, request_id: int, params: dict):
        sticker_id = params.get("sticker_id")
        if sticker_id is None:
            raise ValueError("sticker_id required")
        file_path = await self.sticker_service.get_sticker_file_path(sticker_id)
        if not file_path or not os.path.isfile(file_path):
            raise ValueError("Sticker file not found")
        try:
            data = await asyncio.to_thread(self._read_file_bytes, file_path)
        except OSError:
            logger.exception(f"Failed to read sticker file {file_path}")
            raise ValueError("Sticker file not found")
        return {"data": base64.b64encode(data).decode(), "content_type": "image/webp"}

    async def _cmd_set_entity_private(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        is_private = params.get("is_private")
        if entity_id is None or is_private is None:
            raise ValueError("entity_id and is_private required")
        if not isinstance(is_private, bool):
            raise ValueError("is_private must be boolean")
        success = await self.entity_service.set_entity_private(entity_id, is_private, protocol.email)
        if not success:
            raise ValueError("Failed to set private mode (not owner or entity not found)")
        return {"entity_id": entity_id, "is_private": is_private}

    async def _cmd_get_join_requests(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        if entity_id is None:
            raise ValueError("entity_id required")
        requests = await self.entity_service.get_join_requests(entity_id, protocol.email)
        if requests is None:
            raise ValueError("No permission or entity not found")
        enriched = []
        for req in requests:
            user = await self.db_store.get_user(req["email"])
            username = user.username if user else None
            first_name = user.first_name if user else ""
            enriched.append(
                {
                    "email": req["email"],
                    "username": username,
                    "first_name": first_name,
                    "created_at": req["created_at"],
                }
            )
        return {"requests": enriched}

    async def _cmd_approve_join_request(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        email = params.get("email")
        if entity_id is None or not email:
            raise ValueError("entity_id and email required")
        success, msg = await self.entity_service.approve_join_request(entity_id, email, protocol.email)
        if not success:
            raise ValueError(msg)
        proto = self.gateway.connections.get(email)
        if proto:
            notification = {
                "type": "join_request_approved",
                "entity_id": entity_id,
                "message": "Your join request has been approved.",
            }
            payload = serialize(notification)
            encrypted = encrypt_control(payload, proto.session_key)
            packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
            asyncio.create_task(proto.safe_send(packet))
            refresh_notification = {"type": "refresh_entities"}
            refresh_payload = serialize(refresh_notification)
            refresh_encrypted = encrypt_control(refresh_payload, proto.session_key)
            refresh_packet = STREAM_ID_CONTROL.to_bytes(4, "big") + refresh_encrypted
            asyncio.create_task(proto.safe_send(refresh_packet))
        return {"entity_id": entity_id, "email": email}

    async def _cmd_reject_join_request(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        email = params.get("email")
        if entity_id is None or not email:
            raise ValueError("entity_id and email required")
        success, msg = await self.entity_service.reject_join_request(entity_id, email, protocol.email)
        if not success:
            raise ValueError(msg)
        proto = self.gateway.connections.get(email)
        if proto:
            notification = {
                "type": "join_request_rejected",
                "entity_id": entity_id,
                "message": "Your join request has been rejected.",
            }
            payload = serialize(notification)
            encrypted = encrypt_control(payload, proto.session_key)
            packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
            asyncio.create_task(proto.safe_send(packet))
        return {"entity_id": entity_id, "email": email}

    async def _cmd_callback_query(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        message_id = params.get("message_id")
        callback_data = params.get("callback_data")
        if entity_id is None or message_id is None or not callback_data:
            raise ValueError("Недостаточно данных")

        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("Вы не участник сущности")

        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg:
            raise ValueError("Сообщение не найдено")
        if not msg.from_email.startswith("bot_"):
            raise ValueError("Сообщение не от бота")

        saved = await self.redis_store.get_callback_data(entity_id, message_id)
        if not saved:
            raise ValueError("Кнопки устарели или не найдены")
        valid = False
        for row in saved.get("inline_keyboard", []):
            for btn in row:
                if btn.get("callback_data") == callback_data:
                    valid = True
                    break
            if valid:
                break
        if not valid:
            raise ValueError("Некорректный callback_data")

        bot_email = msg.from_email
        notification = {
            "type": "callback_notification",
            "entity_id": entity_id,
            "message_id": message_id,
            "from": protocol.email,
            "callback_data": callback_data,
        }

        bot_proto = self.gateway.connections.get(bot_email)
        if bot_proto:
            try:
                payload = serialize(notification)
                encrypted = encrypt_control(payload, bot_proto.session_key)
                packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
                await bot_proto.safe_send(packet)
                logger.info(f"Callback notification sent to bot {bot_email}")
            except Exception as e:
                logger.error(f"Ошибка отправки callback боту {bot_email}: {e}")
        else:
            logger.warning(f"Bot {bot_email} не в сети, callback не доставлен")

        return {"status": "sent"}

    async def _cmd_edit_message(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        message_id = params.get("message_id")
        text = params.get("text")
        reply_markup = params.get("reply_markup")
        if entity_id is None or message_id is None or text is None:
            raise ValueError("entity_id, message_id, text required")
        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg:
            raise ValueError("Message not found")
        if not protocol.is_bot or msg.from_email != f"bot_{protocol.bot.id}":
            raise ValueError("Нельзя редактировать чужое сообщение")
        msg.content = text
        await self.db_store.update_message(msg)
        if reply_markup is not None:
            await self.redis_store.save_callback_data(entity_id, message_id, reply_markup, ttl=86400)
        else:
            await self.redis_store.delete_callback_data(entity_id, message_id)
        notification = {
            "type": "edit_message",
            "entity_id": entity_id,
            "message_id": message_id,
            "content": text,
            "reply_markup": reply_markup,
        }
        await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=msg.from_email)
        return {"status": "ok"}

    async def _cmd_answer_callback(self, protocol, request_id: int, params: dict):
        return {"status": "ok"}

    async def _cmd_get_analytics(self, protocol, request_id: int, params: dict):
        entity_id = params.get("entity_id")
        if entity_id is None:
            raise ValueError("Missing entity_id")

        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            raise ValueError("Entity not found")

        if entity.type == ENTITY_TYPE_CHAT:
            raise ValueError("Analytics is only available for groups and channels")

        members = await self.db_store.get_entity_members(entity_id)
        if protocol.email not in members:
            raise ValueError("Not a member")

        admins = await self.db_store.get_entity_admins(entity_id)
        if protocol.email != entity.owner and protocol.email not in admins:
            raise ValueError("Only owner or admin can view analytics")

        data = await self.analytics_service.get_analytics(entity_id)
        return data
    
    async def _cmd_get_sessions(self, protocol, request_id: int, params: dict):
        """Возвращает список сессий текущего пользователя."""
        if protocol.is_bot:
            raise ValueError("Боты не поддерживают управление сессиями")
        email = protocol.email
        session_ids = await self.redis_store.get_user_sessions(email)
        sessions = []
        now = time.time()
        for sid in session_ids:
            data = await self.gateway.get_session(sid)
            if not data:
                continue
            # Преобразуем в читаемый формат
            created = data.get("created_at", 0)
            last_active = data.get("last_active", 0)
            is_current = (sid == protocol.session_id)
            sessions.append({
                "session_id": sid,
                "ip": data.get("ip", ""),
                "user_agent": data.get("user_agent", ""),
                "created_at": created,
                "last_active": last_active,
                "is_current": is_current,
                "is_bot": data.get("is_bot", False),
            })
        # Сортируем по времени создания (сначала новые)
        sessions.sort(key=lambda x: x["created_at"], reverse=True)
        return {"sessions": sessions}

    async def _cmd_terminate_session(self, protocol, request_id: int, params: dict):
        """Завершает указанную сессию (кроме текущей)."""
        if protocol.is_bot:
            raise ValueError("Боты не поддерживают управление сессиями")
        target_session_id = params.get("session_id")
        if not target_session_id:
            raise ValueError("session_id обязателен")
        if target_session_id == protocol.session_id:
            raise ValueError("Нельзя завершить текущую сессию")

        # Проверяем, что текущая сессия существует и старше 30 минут
        current_session = await self.gateway.get_session(protocol.session_id)
        if not current_session:
            raise ValueError("Текущая сессия не найдена")
        created = current_session.get("created_at", 0)
        if time.time() - created < 1800:  # 30 минут
            raise ValueError("Текущая сессия должна быть активна более 30 минут")

        # Проверяем, что целевая сессия принадлежит этому пользователю
        target_data = await self.gateway.get_session(target_session_id)
        if not target_data:
            raise ValueError("Сессия не найдена")
    
        # проверяем email из данных сессии
        if target_data.get("email") != protocol.email:
            raise ValueError("Нет прав на завершение этой сессии")

        # Удаляем сессию из Redis
        await self.gateway.delete_session(target_session_id)

        # Если соединение активно, закрываем его
        proto = self.gateway.connections.get(target_session_id)
        if proto:
            asyncio.create_task(proto.close_connection("Session terminated by user"))

        logger.info(f"Сессия {target_session_id} завершена пользователем {protocol.email}")
        return {"session_id": target_session_id}
    
    async def _cmd_get_blacklist(self, protocol, request_id: int, params: dict):
        if protocol.is_bot:
            raise ValueError("Боты не используют черный список")
        blacklist = await self.db_store.get_blacklist(protocol.email)
        return {"blacklist": blacklist}

    async def _cmd_block_user(self, protocol, request_id: int, params: dict):
        if protocol.is_bot:
            raise ValueError("Боты не могут блокировать пользователей")
        target = params.get("target")
        if not target:
            raise ValueError("Не указан target")

        target_email = None
        target_type = None
        target_identifier = None

        # 1. Ищем пользователя
        user = await self.user_service.get_user_by_identifier(target)
        if user:
            target_email = user.email
            target_type = 'user'
            target_identifier = target_email
        else:
            # 2. Ищем бота
            bot = None
            if target.startswith('bot_'):
                try:
                    bot_id = int(target.split('_')[1])
                    bot = await self.db_store.get_bot(bot_id)
                except (IndexError, ValueError):
                    pass
            elif target.startswith('@'):
                username = target[1:]
                bot = await self.bot_service.get_bot_by_username(username)
            else:
                try:
                    bot_id = int(target)
                    bot = await self.db_store.get_bot(bot_id)
                except ValueError:
                    pass

            if bot:
                target_email = f"bot_{bot.id}"
                target_type = 'bot'
                target_identifier = target_email
            else:
                raise ValueError("Пользователь или бот не найден")

        if target_email == protocol.email:
            raise ValueError("Нельзя заблокировать самого себя")

        if await self.db_store.is_blocked(protocol.email, target_identifier):
            raise ValueError("Уже заблокирован")

        if not await self.db_store.add_blacklist(protocol.email, target_identifier, target_type):
            raise ValueError("Не удалось добавить в черный список")

        # Анти-спам: если target_email написал нам первым (создал этот чат),
        # а мы ни разу ему не ответили и теперь блокируем — ограничиваем
        # ему возможность первым писать незнакомцам на 14 дней.
        # Делаем это ДО удаления чата, пока ещё доступна история сообщений.
        if self.spam_service and target_type == 'user':
            chat = await self.entity_service.find_chat_between(protocol.email, target_email)
            if chat:
                await self.spam_service.register_unanswered_block(target_email, chat, protocol.email)

        deleted = await self.entity_service.delete_all_chats_between(protocol.email, target_email)

        await self.gateway.send_refresh_entities(protocol.email)
        await self.gateway.send_refresh_entities(target_email)

        logger.info(f"Пользователь {protocol.email} заблокировал {target_email}, удалено чатов: {deleted}")
        return {"blocked": target_email, "deleted_chats": deleted}

    async def _cmd_unblock_user(self, protocol, request_id: int, params: dict):
        if protocol.is_bot:
            raise ValueError("Боты не могут управлять черным списком")
        target = params.get("target")
        if not target:
            raise ValueError("Не указан target")

        target_identifier = None

        # Ищем пользователя
        user = await self.user_service.get_user_by_identifier(target)
        if user:
            target_identifier = user.email
        else:
            # Ищем бота
            bot = None
            if target.startswith('bot_'):
                try:
                    bot_id = int(target.split('_')[1])
                    bot = await self.db_store.get_bot(bot_id)
                except (IndexError, ValueError):
                    pass
            elif target.startswith('@'):
                username = target[1:]
                bot = await self.bot_service.get_bot_by_username(username)
            else:
                try:
                    bot_id = int(target)
                    bot = await self.db_store.get_bot(bot_id)
                except ValueError:
                    pass

            if bot:
                target_identifier = f"bot_{bot.id}"
            else:
                raise ValueError("Пользователь или бот не найден")

        if not await self.db_store.remove_blacklist(protocol.email, target_identifier):
            raise ValueError("Не удалось разблокировать (возможно, не был заблокирован)")

        await self.gateway.send_refresh_entities(protocol.email)

        logger.info(f"Пользователь {protocol.email} разблокировал {target_identifier}")
        return {"unblocked": target_identifier}
