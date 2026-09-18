# services/reaction_service.py
import time
import logging
from typing import Optional, Dict

from .models import Reaction
from .redis_store import RedisStore
from .postgres_store import PostgresStore
from protocol import REACTION_RATE_LIMIT, REACTION_RATE_WINDOW


logger = logging.getLogger(__name__)

class ReactionService:
    def __init__(self, db_store: PostgresStore, redis_store: RedisStore):
        self.db_store = db_store
        self.redis_store = redis_store

    async def add_reaction(self, message_id: int, email: str, reaction_type: str, entity_id: int) -> bool:
        if not reaction_type or len(reaction_type) > 32:
            return False

        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return False
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if email not in members or email in banned:
            return False
        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg:
            return False

        if not await self.redis_store.check_reaction_rate(email):
            logger.warning(f"Reaction rate limit exceeded for {email}")
            return False

        existing = await self.db_store.get_user_reaction(message_id, email)
        if existing:
            if existing == reaction_type:
                return True
            await self.db_store.remove_reaction(message_id, email)

        reaction = Reaction(
            message_id=message_id,
            email=email,
            reaction_type=reaction_type,
            entity_id=entity_id,
            timestamp=time.time()
        )
        try:
            await self.db_store.add_reaction(reaction)
        except Exception as e:
            logger.error(f"Failed to add reaction for {email} on message {message_id}: {e}", exc_info=True)
            return False
        return True

    async def remove_reaction(self, message_id: int, email: str) -> bool:
        if not await self.redis_store.check_reaction_rate(email):
            logger.warning(f"Reaction rate limit exceeded for {email} (remove)")
            return False
        try:
            return await self.db_store.remove_reaction(message_id, email)
        except Exception as e:
            logger.error(f"Failed to remove reaction for {email} on message {message_id}: {e}", exc_info=True)
            return False

    async def get_reactions_for_message(self, message_id: int) -> Dict[str, int]:
        return await self.db_store.get_reactions_for_message(message_id)

    async def get_user_reaction(self, message_id: int, email: str) -> Optional[str]:
        return await self.db_store.get_user_reaction(message_id, email)

    async def get_reactions_for_messages(self, message_ids: list, email: str) -> Dict[int, dict]:
        if not message_ids:
            return {}
        # Прямой вызов PostgresStore, который уже возвращает правильную структуру
        return await self.db_store.get_reactions_for_messages(message_ids, email)