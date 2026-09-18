from typing import Optional
from .models import User
from protocol import validate_username
import logging

logger = logging.getLogger(__name__)

class UserService:
    def __init__(self, db_store):  # ожидает PostgresStore (асинхронный)
        self.db_store = db_store

    async def get_or_create_user(self, email: str, public_key: str) -> User:
        user = await self.db_store.get_user(email)
        if user:
            return user
        # Если пользователя нет, создаём
        return await self.db_store.create_user(email, public_key)

    async def set_username(self, email: str, username: Optional[str]) -> bool:
        user = await self.db_store.get_user(email)
        if not user:
            return False

        if username is not None:
            if len(username) > 16:
                logger.warning(f"Username too long ({len(username)}) for {email}, max 16")
                return False
            if not validate_username(username):
                logger.warning(f"Invalid username format for {email}: {username}")
                return False

            # Глобальная проверка уникальности
            if await self.db_store.username_exists_anywhere(username):
                # Проверяем, не занят ли этот username самим пользователем (если уже установлен)
                existing_user = await self.db_store.get_user_by_username(username)
                if existing_user and existing_user.email != email:
                    logger.warning(f"Username {username} already taken by user {existing_user.email}")
                    return False
                existing_entity = await self.db_store.get_entity_by_username(username)
                if existing_entity:
                    logger.warning(f"Username {username} already taken by entity {existing_entity.id}")
                    return False

        user.username = username
        try:
            await self.db_store.update_user(user)
        except Exception as e:
            logger.error(f"Failed to update username for {email}: {e}")
            return False
        return True

    async def set_first_name(self, email: str, name: str) -> bool:
        user = await self.db_store.get_user(email)
        if not user:
            return False
        name = (name or "").strip()
        if len(name) > 64:
            logger.warning(f"First name too long ({len(name)}) for {email}, max 64")
            return False
        user.first_name = name[:64]
        try:
            await self.db_store.update_user(user)
        except Exception as e:
            logger.error(f"Failed to update first_name for {email}: {e}")
            return False
        return True

    async def set_bio(self, email: str, bio: str) -> bool:
        user = await self.db_store.get_user(email)
        if not user:
            return False
        bio = (bio or "").strip()
        if len(bio) > 200:
            logger.warning(f"Bio too long ({len(bio)}) for {email}, max 200")
            return False
        user.bio = bio[:200]
        try:
            await self.db_store.update_user(user)
        except Exception as e:
            logger.error(f"Failed to update bio for {email}: {e}")
            return False
        return True

    async def set_show_email(self, email: str, show: bool) -> bool:
        user = await self.db_store.get_user(email)
        if not user:
            return False
        user.show_email = show
        try:
            await self.db_store.update_user(user)
        except Exception as e:
            logger.error(f"Failed to update show_email for {email}: {e}")
            return False
        return True

    async def get_user_by_identifier(self, identifier: str) -> Optional[User]:
        if not identifier:
            return None
        if identifier.startswith('@'):
            username = identifier[1:]
            return await self.db_store.get_user_by_username(username)
        else:
            return await self.db_store.get_user(identifier)

    async def get_display_name(self, email: str) -> str:
        user = await self.db_store.get_user(email)
        if not user:
            return email
        if user.username:
            return user.username
        if user.first_name:
            return user.first_name
        return user.email

    async def get_user_info(self, target_email: str, requester_email: Optional[str] = None) -> Optional[dict]:
        user = await self.db_store.get_user(target_email)
        if not user:
            return None

        # Список сущностей показываем ТОЛЬКО владельцу профиля
        entities = []
        if requester_email == target_email:
            entities = await self.db_store.get_entities_for_user(target_email)

        info = {
            "username": user.username,
            "first_name": user.first_name,
            "bio": user.bio,
            "public_key": user.public_key,
            "tags": user.tags,
            "entities": entities,
            "show_email": user.show_email,
        }
        if user.show_email or (requester_email == target_email):
            info["email"] = user.email
        else:
            info["email"] = None
        return info

    async def set_user_tag(self, target_email: str, tag: str) -> bool:
        tag = (tag or "").strip()
        if not tag or len(tag) > 32:
            return False
        user = await self.db_store.get_user(target_email)
        if not user:
            return False
        if tag not in user.tags:
            user.tags.append(tag)
            try:
                await self.db_store.update_user(user)
            except Exception as e:
                logger.error(f"Failed to add tag for {target_email}: {e}")
                user.tags.remove(tag)
                return False
        return True

    async def remove_user_tag(self, target_email: str, tag: str) -> bool:
        user = await self.db_store.get_user(target_email)
        if not user:
            return False
        if tag in user.tags:
            user.tags.remove(tag)
            try:
                await self.db_store.update_user(user)
            except Exception as e:
                logger.error(f"Failed to remove tag for {target_email}: {e}")
                user.tags.append(tag)
                return False
        return True