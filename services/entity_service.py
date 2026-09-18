import logging
from typing import Optional, List
from .redis_store import RedisStore
from .models import Entity
from protocol import ENTITY_TYPE_CHAT, ENTITY_TYPE_GROUP, ENTITY_TYPE_CHANNEL, validate_username
import time

logger = logging.getLogger(__name__)

VALID_ENTITY_TYPES = {ENTITY_TYPE_CHAT, ENTITY_TYPE_GROUP, ENTITY_TYPE_CHANNEL}
MAX_ENTITY_NAME_LENGTH = 128


class EntityService:
    def __init__(self, db_store, redis_store: RedisStore):
        self.db_store = db_store  # экземпляр PostgresStore
        self.redis_store = redis_store

    async def create_entity(self, entity_type: str, name: str, owner_email: str, target_email: Optional[str] = None) -> Optional[Entity]:
        if entity_type not in VALID_ENTITY_TYPES:
            logger.warning(f"Invalid entity type {entity_type!r} requested by {owner_email}")
            return None

        if entity_type == ENTITY_TYPE_CHAT:
            if not target_email or target_email == owner_email:
                logger.warning(f"Invalid target_email for chat creation by {owner_email}: {target_email!r}")
                return None
        else:
            name = (name or "").strip()
            if not name or len(name) > MAX_ENTITY_NAME_LENGTH:
                logger.warning(f"Invalid entity name from {owner_email}: {name!r}")
                return None

        entity_id = await self.redis_store.get_next_entity_id()
        members = [owner_email]
        if entity_type == ENTITY_TYPE_CHAT:
            members.append(target_email)
        entity = Entity(
            id=entity_id,
            type=entity_type,
            name=name,
            owner=owner_email,
            admins=[],
            members=members,
            banned=[],
        )
        try:
            await self.db_store.create_entity(entity)
            for email in members:
                await self.db_store.add_member(entity_id, email)
        except Exception as e:
            logger.error(f"Failed to create entity {entity_id} ({entity_type}) for {owner_email}: {e}", exc_info=True)
            return None
        logger.info(f"Entity {entity_id} ({entity_type}) created by {owner_email}")
        return entity

    async def get_entity(self, entity_id: int) -> Optional[Entity]:
        return await self.db_store.get_entity(entity_id)

    async def add_user_to_entity(self, email: str, entity_id: int) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if email in members or email in banned:
            return False
        try:
            await self.db_store.add_member(entity_id, email)
        except Exception as e:
            logger.error(f"Failed to add member {email} to entity {entity_id}: {e}", exc_info=True)
            return False
        return True

    async def remove_user_from_entity(self, email: str, entity_id: int) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        if email == entity.owner:
            return False
        members = await self.db_store.get_entity_members(entity_id)
        if email not in members:
            return False
        try:
            await self.db_store.remove_member(entity_id, email)
        except Exception as e:
            logger.error(f"Failed to remove member {email} from entity {entity_id}: {e}", exc_info=True)
            return False
        return True

    async def delete_entity(self, entity_id: int) -> bool:
        try:
            return await self.db_store.delete_entity(entity_id)
        except Exception as e:
            logger.error(f"Failed to delete entity {entity_id}: {e}", exc_info=True)
            return False

    async def promote_admin(self, entity_id: int, target_email: str, owner_email: str) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        if owner_email != entity.owner:
            return False
        members = await self.db_store.get_entity_members(entity_id)
        if target_email not in members:
            return False
        admins = await self.db_store.get_entity_admins(entity_id)
        if target_email in admins:
            return False
        try:
            await self.db_store.add_admin(entity_id, target_email)
        except Exception as e:
            logger.error(f"Failed to promote {target_email} to admin in entity {entity_id}: {e}", exc_info=True)
            return False
        return True

    async def demote_admin(self, entity_id: int, target_email: str, owner_email: str) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        if owner_email != entity.owner:
            return False
        admins = await self.db_store.get_entity_admins(entity_id)
        if target_email not in admins:
            return False
        try:
            await self.db_store.remove_admin(entity_id, target_email)
        except Exception as e:
            logger.error(f"Failed to demote {target_email} in entity {entity_id}: {e}", exc_info=True)
            return False
        return True
    
    async def set_entity_username(self, entity_id: int, username: Optional[str], actor_email: str) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        if entity.owner != actor_email:
            return False
        if entity.type == ENTITY_TYPE_CHAT:
            # Личные чаты не могут иметь username
            return False

        if username is not None:
            if len(username) > 16:
                return False
            if not validate_username(username):
                return False
            # Глобальная проверка уникальности (пользователи, боты, сущности)
            if await self.db_store.username_exists_anywhere(username):
                existing = await self.db_store.get_entity_by_username(username)
                if not existing or existing.id != entity_id:
                    return False

        try:
            return await self.db_store.update_entity_username(entity_id, username)
        except Exception as e:
            logger.error(f"Failed to update entity {entity_id} username: {e}")
            return False
    
    async def set_entity_private(self, entity_id: int, is_private: bool, actor_email: str) -> bool:
        """Устанавливает приватность сущности. Только владелец."""
        entity = await self.db_store.get_entity(entity_id)
        if not entity or entity.owner != actor_email:
            return False
        return await self.db_store.update_entity_private(entity_id, is_private)

    async def request_join(self, entity_id: int, email: str) -> tuple[bool, str]:
        """
        Подача заявки на вступление в приватную сущность.
        Возвращает (успех, сообщение).
        """
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False, "Entity not found"
        if not entity.is_private:
            return False, "Entity is public, use join directly"

        members = await self.db_store.get_entity_members(entity_id)
        if email in members:
            return False, "You are already a member"
        banned = await self.db_store.get_entity_banned(entity_id)
        if email in banned:
            return False, "You are banned from this entity"

        # Проверяем, есть ли уже активная заявка
        existing = await self.db_store.get_join_request(entity_id, email)
        if existing:
            return False, "Join request already pending"

        expires_at = time.time() + 86400  # 24 часа
        try:
            await self.db_store.create_join_request(entity_id, email, expires_at)
        except Exception as e:
            logger.error(f"Failed to create join request for {email} in entity {entity_id}: {e}", exc_info=True)
            return False, "Internal error"
        return True, "Join request submitted, waiting for approval"

    async def get_join_requests(self, entity_id: int, actor_email: str) -> Optional[List[dict]]:
        """Возвращает список заявок, если actor_email является владельцем или админом."""
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return None
        admins = await self.db_store.get_entity_admins(entity_id)
        if actor_email != entity.owner and actor_email not in admins:
            return None
        return await self.db_store.get_pending_join_requests(entity_id)

    async def approve_join_request(self, entity_id: int, target_email: str, actor_email: str) -> tuple[bool, str]:
        """Подтверждение заявки. Только владелец или админ."""
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False, "Entity not found"
        admins = await self.db_store.get_entity_admins(entity_id)
        if actor_email != entity.owner and actor_email not in admins:
            return False, "Insufficient permissions"

        # Проверяем, что заявка существует
        req = await self.db_store.get_join_request(entity_id, target_email)
        if not req:
            return False, "Join request not found"

        # Добавляем пользователя в члены
        members = await self.db_store.get_entity_members(entity_id)
        try:
            if target_email in members:
                # уже есть, удаляем заявку
                await self.db_store.reject_join_request(entity_id, target_email)
                return True, "User is already a member"
            await self.db_store.add_member(entity_id, target_email)
            # Одобряем заявку (меняем статус на approved)
            await self.db_store.approve_join_request(entity_id, target_email)
            # Удаляем заявку (можно оставить для истории, но мы удалим)
            await self.db_store.reject_join_request(entity_id, target_email)  # удаляем запись
        except Exception as e:
            logger.error(f"Failed to approve join request for {target_email} in entity {entity_id}: {e}", exc_info=True)
            return False, "Internal error"
        return True, "Join request approved"

    async def reject_join_request(self, entity_id: int, target_email: str, actor_email: str) -> tuple[bool, str]:
        """Отклонение заявки. Только владелец или админ."""
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False, "Entity not found"
        admins = await self.db_store.get_entity_admins(entity_id)
        if actor_email != entity.owner and actor_email not in admins:
            return False, "Insufficient permissions"

        req = await self.db_store.get_join_request(entity_id, target_email)
        if not req:
            return False, "Join request not found"

        try:
            await self.db_store.reject_join_request(entity_id, target_email)
        except Exception as e:
            logger.error(f"Failed to reject join request for {target_email} in entity {entity_id}: {e}", exc_info=True)
            return False, "Internal error"
        return True, "Join request rejected"
    
    async def find_chat_between(self, email1: str, email2: str) -> Optional[Entity]:
        return await self.db_store.find_chat_between(email1, email2)

    async def delete_all_chats_between(self, email1: str, email2: str) -> int:
        
        deleted = 0
        while True:
            chat = await self.find_chat_between(email1, email2)
            if not chat:
                break
            if await self.delete_entity(chat.id):
                deleted += 1
            else:
                break
        return deleted