import secrets
import base64
import logging
import time
from typing import Optional, List
from axiso import EdDSA
from .models import Bot
from .redis_store import RedisStore
from protocol import MAX_BOTS_PER_USER, validate_username

logger = logging.getLogger(__name__)

MAX_BOT_NAME_LENGTH = 64


class BotService:
    def __init__(self, db_store, redis_store: RedisStore):  # PostgresStore
        self.db_store = db_store
        self.redis_store = redis_store

    async def create_bot(self, owner_email: str, name: str, username: Optional[str] = None) -> Optional[Bot]:
        name = (name or "").strip()
        if not name or len(name) > MAX_BOT_NAME_LENGTH:
            logger.warning(f"Invalid bot name from {owner_email}: {name!r}")
            return None
        bots = await self.db_store.get_bots_by_owner(owner_email)
        if len(bots) >= MAX_BOTS_PER_USER:
            logger.warning(f"User {owner_email} exceeded bot limit ({MAX_BOTS_PER_USER})")
            return None
        if username is not None:
            if not validate_username(username):
                return None
            if await self.db_store.username_exists_anywhere(username):
                return None
        try:
            ed_keys = EdDSA.generate_keypair()
            public_key = ed_keys["public_key"]
            private_key = ed_keys["private_key"]
            token = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('utf-8')
            bot_id = await self.redis_store.get_next_bot_id()
            bot = Bot(
                id=bot_id,
                username=username,
                name=name,
                owner_email=owner_email,
                token=token,
                public_key=public_key,
                private_key=private_key,
                is_active=True,
                created_at=time.time()
            )
            await self.db_store.create_bot(bot)
        except Exception as e:
            logger.error(f"Failed to create bot for {owner_email}: {e}", exc_info=True)
            return None
        logger.info(f"Bot {bot_id} created by {owner_email}")
        return bot

    async def delete_bot(self, bot_id: int, owner_email: str) -> bool:
        bot = await self.db_store.get_bot(bot_id)
        if not bot or bot.owner_email != owner_email:
            return False
        try:
            return await self.db_store.delete_bot(bot_id)
        except Exception as e:
            logger.error(f"Failed to delete bot {bot_id} for {owner_email}: {e}", exc_info=True)
            return False

    async def set_bot_active(self, bot_id: int, owner_email: str, is_active: bool) -> bool:
        bot = await self.db_store.get_bot(bot_id)
        if not bot or bot.owner_email != owner_email:
            return False
        bot.is_active = is_active
        try:
            await self.db_store.update_bot(bot)
        except Exception as e:
            logger.error(f"Failed to update active state for bot {bot_id}: {e}", exc_info=True)
            return False
        return True

    async def get_bot_by_token(self, token: str) -> Optional[Bot]:
        return await self.db_store.get_bot_by_token(token)

    async def get_bots_for_user(self, owner_email: str) -> List[Bot]:
        return await self.db_store.get_bots_by_owner(owner_email)

    async def get_bot_by_username(self, username: str) -> Optional[Bot]:
        return await self.db_store.get_bot_by_username(username)

    async def set_bot_username(self, bot_id: int, owner_email: str, username: Optional[str]) -> bool:
        bot = await self.db_store.get_bot(bot_id)
        if not bot or bot.owner_email != owner_email:
            return False
        if username is not None:
            if not validate_username(username):
                return False
            if await self.db_store.username_exists_anywhere(username):
                return False
        bot.username = username
        try:
            await self.db_store.update_bot(bot)
        except Exception as e:
            logger.error(f"Failed to update username for bot {bot_id}: {e}", exc_info=True)
            return False
        return True