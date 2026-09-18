# services/poll_service.py
import time
import logging
from typing import Optional, List, Dict, Any
from .models import Poll
from .postgres_store import PostgresStore
from .redis_store import RedisStore

logger = logging.getLogger(__name__)


class PollService:
    def __init__(self, db_store: PostgresStore, redis_store: RedisStore):
        self.db_store = db_store
        self.redis_store = redis_store

    async def create_poll(
        self, entity_id: int, message_id: int, question: str, options: List[str], created_by: str
    ) -> Optional[Poll]:
        question = (question or "").strip()
        if not question or len(question) > 300:
            return None
        if len(options) < 2 or len(options) > 8:
            return None
        cleaned_options = [opt.strip() for opt in options]
        if any(not opt or len(opt) > 100 for opt in cleaned_options):
            return None
        if len(set(cleaned_options)) != len(cleaned_options):
            return None

        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return None
        members = await self.db_store.get_entity_members(entity_id)
        if created_by not in members:
            return None

        try:
            poll_id = await self.redis_store.get_next_poll_id()
            poll = Poll(
                id=poll_id,
                message_id=message_id,
                entity_id=entity_id,
                question=question,
                options=cleaned_options,
                total_votes=0,
                created_by=created_by,
                created_at=time.time(),
                is_closed=False,
            )
            await self.db_store.create_poll(poll)
        except Exception as e:
            logger.error(f"Failed to create poll in entity {entity_id} by {created_by}: {e}", exc_info=True)
            return None
        return poll

    async def get_poll_by_message(self, entity_id: int, message_id: int) -> Optional[Poll]:
        return await self.db_store.get_poll_by_message_id(entity_id, message_id)

    async def vote(self, poll_id: int, user_email: str, option_index: int) -> bool:
        poll = await self.db_store.get_poll(poll_id)
        if not poll or poll.is_closed:
            return False
        if option_index < 0 or option_index >= len(poll.options):
            return False
        members = await self.db_store.get_entity_members(poll.entity_id)
        if user_email not in members:
            logger.warning(f"{user_email} tried to vote in poll {poll_id} without entity membership")
            return False
        try:
            return await self.db_store.add_vote(poll_id, user_email, option_index)
        except Exception as e:
            logger.error(f"Failed to record vote for poll {poll_id} by {user_email}: {e}", exc_info=True)
            return False

    async def get_results(self, poll_id: int) -> Dict[str, Any]:
        poll = await self.db_store.get_poll(poll_id)
        if not poll:
            return {}
        counts = await self.db_store.get_poll_results(poll_id)
        total = poll.total_votes
        counts_str = {str(k): v for k, v in counts.items()}
        percentages = []
        for i, opt in enumerate(poll.options):
            cnt = counts.get(i, 0)
            pct = (cnt / total * 100) if total > 0 else 0
            percentages.append(round(pct, 1))
        return {
            "question": poll.question,
            "options": poll.options,
            "counts": counts_str,
            "total_votes": total,
            "percentages": percentages,
            "is_closed": poll.is_closed,
        }

    async def close_poll(self, poll_id: int) -> bool:
        try:
            return await self.db_store.close_poll(poll_id)
        except Exception as e:
            logger.error(f"Failed to close poll {poll_id}: {e}", exc_info=True)
            return False

    async def get_user_vote(self, poll_id: int, user_email: str) -> Optional[int]:
        return await self.db_store.get_user_vote(poll_id, user_email)