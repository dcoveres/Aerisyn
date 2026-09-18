import logging
from typing import List, Optional
from .models import Message
from protocol import MAX_MESSAGE_SIZE, ENTITY_TYPE_CHANNEL, MAX_HISTORY
import time

logger = logging.getLogger(__name__)


class MessageService:
    def __init__(self, db_store, redis_store, gateway):
        self.db_store = db_store          # экземпляр PostgresStore
        self.redis_store = redis_store
        self.gateway = gateway

    async def add_message(self, email: str, entity_id: int, content: str, reply_to: Optional[dict] = None) -> Optional[Message]:
        if not content or not content.strip():
            return None
        if len(content) > MAX_MESSAGE_SIZE:
            logger.warning(f"Message from {email} exceeds MAX_MESSAGE_SIZE, rejected")
            return None
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return None
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if email not in members:
            return None
        if email in banned:
            return None

        if entity.type == ENTITY_TYPE_CHANNEL:
            admins = await self.db_store.get_entity_admins(entity_id)
            if email != entity.owner and email not in admins:
                return None

        reply_entity_id = None
        reply_message_id = None
        if reply_to:
            reply_entity_id = reply_to.get("entity_id")
            reply_message_id = reply_to.get("message_id")
            if reply_entity_id is not None and not isinstance(reply_entity_id, int):
                logger.warning(f"Invalid reply_to.entity_id from {email}: {reply_entity_id!r}")
                return None
            if reply_message_id is not None and not isinstance(reply_message_id, int):
                logger.warning(f"Invalid reply_to.message_id from {email}: {reply_message_id!r}")
                return None

        try:
            msg_id = await self.redis_store.get_next_message_id(entity_id)
            msg = Message(
                id=msg_id,
                from_email=email,
                content=content,
                timestamp=time.time(),
                type="text",
                entity_id=entity_id,
                reply_to_entity_id=reply_entity_id,
                reply_to_message_id=reply_message_id,
            )
            await self.db_store.add_message(msg)
        except Exception as e:
            logger.error(f"Failed to add message from {email} to entity {entity_id}: {e}", exc_info=True)
            return None
        return msg

    async def add_file_message(self, email: str, entity_id: int, filename: str, file_size: int,
                         file_checksum: str, file_path: str) -> Optional[Message]:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return None
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if email not in members or email in banned:
            return None
        if entity.type == ENTITY_TYPE_CHANNEL:
            admins = await self.db_store.get_entity_admins(entity_id)
            if email != entity.owner and email not in admins:
                return None

        try:
            msg_id = await self.redis_store.get_next_message_id(entity_id)
            msg = Message(
                id=msg_id,
                from_email=email,
                content=filename,
                timestamp=time.time(),
                type="file",
                filename=filename,
                file_size=file_size,
                file_checksum=file_checksum,
                file_path=file_path,
                entity_id=entity_id
            )
            await self.db_store.add_message(msg)
        except Exception as e:
            logger.error(f"Failed to add file message from {email} to entity {entity_id}: {e}", exc_info=True)
            return None
        return msg

    async def get_messages(self, email: str, entity_id: int, limit: int) -> List[Message]:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return []
        members = await self.db_store.get_entity_members(entity_id)
        if email not in members:
            return []
        limit = max(1, min(limit, MAX_HISTORY))
        try:
            return await self.db_store.get_messages(entity_id, limit)
        except Exception as e:
            logger.error(f"Failed to fetch messages for entity {entity_id}: {e}", exc_info=True)
            return []

    async def delete_message(self, email: str, entity_id: int, message_id: int) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg:
            return False
        # Проверка прав: владелец, автор, админ
        admins = await self.db_store.get_entity_admins(entity_id)
        if email != msg.from_email and email != entity.owner and email not in admins:
            return False
        try:
            return await self.db_store.delete_message(entity_id, message_id)
        except Exception as e:
            logger.error(f"Failed to delete message {message_id} in entity {entity_id}: {e}", exc_info=True)
            return False

    async def add_sticker_message(self, from_email: str, entity_id: int, sticker_id: int, sticker_name: str, file_path: str) -> Optional[Message]:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return None
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if from_email not in members or from_email in banned:
            return None
        if entity.type == ENTITY_TYPE_CHANNEL:
            admins = await self.db_store.get_entity_admins(entity_id)
            if from_email != entity.owner and from_email not in admins:
                return None

        try:
            msg_id = await self.redis_store.get_next_message_id(entity_id)
            msg = Message(
                id=msg_id,
                from_email=from_email,
                content=sticker_name,
                timestamp=time.time(),
                type="sticker",
                filename=f"sticker_{sticker_id}.webp",
                file_path=file_path,
                file_size=None,
                file_checksum=None,
                entity_id=entity_id
            )
            await self.db_store.add_message(msg)
        except Exception as e:
            logger.error(f"Failed to add sticker message from {from_email} to entity {entity_id}: {e}", exc_info=True)
            return None
        return msg
