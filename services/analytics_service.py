import logging
from typing import Dict, Any
from .postgres_store import PostgresStore

logger = logging.getLogger(__name__)


class AnalyticsService:
    def __init__(self, db_store: PostgresStore):
        self.db_store = db_store

    async def get_analytics(self, entity_id: int) -> Dict[str, Any]:
        """
        Собирает аналитику по сущности. Если сущность не найдена или один из
        запросов к БД падает, возвращает нулевые значения вместо необработанного
        исключения, чтобы не обрушивать вызывающий обработчик.
        """
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            logger.warning(f"get_analytics: entity {entity_id} not found")
            return {
                "member_count": 0,
                "messages_7d": 0,
                "messages_30d": 0,
                "reactions_7d": 0,
                "reactions_30d": 0,
                "activity": [],
            }

        try:
            member_count = await self.db_store.get_entity_members_count(entity_id)
            messages_7d = await self.db_store.get_messages_count_by_days(entity_id, 7)
            messages_30d = await self.db_store.get_messages_count_by_days(entity_id, 30)
            reactions_7d = await self.db_store.get_reactions_count_by_days(entity_id, 7)
            reactions_30d = await self.db_store.get_reactions_count_by_days(entity_id, 30)
            activity = await self.db_store.get_activity_by_day(entity_id, 30)
        except Exception as e:
            logger.error(f"Failed to collect analytics for entity {entity_id}: {e}", exc_info=True)
            return {
                "member_count": 0,
                "messages_7d": 0,
                "messages_30d": 0,
                "reactions_7d": 0,
                "reactions_30d": 0,
                "activity": [],
            }

        return {
            "member_count": member_count,
            "messages_7d": messages_7d,
            "messages_30d": messages_30d,
            "reactions_7d": reactions_7d,
            "reactions_30d": reactions_30d,
            "activity": activity,
        }
