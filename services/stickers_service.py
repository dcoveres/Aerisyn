import os
import time
import logging
from typing import List, Optional, Dict, Any
from .models import Sticker
from .postgres_store import PostgresStore
from .redis_store import RedisStore
from protocol import MAX_STICKERS_PER_REQUEST

logger = logging.getLogger(__name__)

class StickerService:
    def __init__(self, db_store: PostgresStore, redis_store: RedisStore, sticker_dir: str = "storage/stickers"):
        self.db_store = db_store
        self.redis_store = redis_store
        self.sticker_dir = sticker_dir
        os.makedirs(self.sticker_dir, exist_ok=True)

    async def get_stickers(self, query: Optional[str] = None, limit: int = MAX_STICKERS_PER_REQUEST, offset: int = 0) -> List[Dict[str, Any]]:
        """Возвращает список стикеров с учётом поиска по имени/тегам."""
        limit = max(1, min(limit, MAX_STICKERS_PER_REQUEST))
        offset = max(0, offset)
        try:
            stickers = await self.db_store.get_stickers(query, limit, offset)
        except Exception as e:
            logger.error(f"Failed to fetch stickers (query={query!r}): {e}", exc_info=True)
            return []
        return [{
            "id": s.id,
            "name": s.name,
            "tags": s.tags,
            "file_path": s.file_path,   # клиент будет запрашивать файл по id
        } for s in stickers]

    async def get_sticker_by_id(self, sticker_id: int) -> Optional[Dict[str, Any]]:
        sticker = await self.db_store.get_sticker(sticker_id)
        if not sticker:
            return None
        return {
            "id": sticker.id,
            "name": sticker.name,
            "tags": sticker.tags,
            "file_path": sticker.file_path,
        }

    async def get_sticker_file_path(self, sticker_id: int) -> Optional[str]:
        sticker = await self.db_store.get_sticker(sticker_id)
        if not sticker:
            return None
        return sticker.file_path

    async def send_sticker(self, from_email: str, entity_id: int, sticker_id: int) -> Optional[Dict[str, Any]]:
        """
        Создаёт сообщение-стикер и возвращает данные для сохранения.
        ВАЖНО: этот метод не проверяет права пользователя на отправку в entity_id —
        вызывающий код (MessageService/командный обработчик) обязан проверить
        членство/бан/права канала перед вызовом.
        """
        try:
            sticker = await self.db_store.get_sticker(sticker_id)
        except Exception as e:
            logger.error(f"Failed to fetch sticker {sticker_id} for {from_email}: {e}", exc_info=True)
            return None
        if not sticker:
            return None
        return {
            "type": "sticker",
            "sticker_id": sticker_id,
            "sticker_name": sticker.name,
            "file_path": sticker.file_path,
        }