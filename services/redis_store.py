import time
import json
from typing import Optional, Dict, Any
import redis.asyncio as redis
from redis.asyncio.client import Redis
import logging
from protocol import CODE_REQUEST_COOLDOWN, ATTEMPT_WINDOW, BLOCK_DURATION, REACTION_RATE_WINDOW, REACTION_RATE_LIMIT
logger = logging.getLogger(__name__)

class RedisStore:
    """Хранилище для временных данных и счётчиков в Redis."""

    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis: Optional[Redis] = None
        self.redis_url = redis_url

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        logger.info("RedisStore подключён")

    # ---------- Счётчики ----------
    async def get_next_entity_id(self) -> int:
        return await self.redis.incr("counter:entity")

    async def get_next_stream_id(self) -> int:
        sid = await self.redis.incr("counter:stream")
        if sid >= 2**32:
            await self.redis.set("counter:stream", 1)
            sid = 1
        return sid

    async def get_next_message_id(self, entity_id: int) -> int:
        """Возвращает следующий ID сообщения для конкретной сущности."""
        key = f"counter:message:{entity_id}"
        return await self.redis.incr(key)

    # ---------- SMS-попытки (защита от брутфорса) ----------
    async def get_sms_attempts(self, email: str) -> Optional[dict]:
        key = f"sms_attempts:{email}"
        data = await self.redis.hgetall(key)
        if not data:
            return None
        return {
            "count": int(data.get("count", 0)),
            "first_attempt": float(data.get("first_attempt", 0)),
            "blocked_until": float(data.get("blocked_until", 0)),
        }

    async def set_sms_attempts(self, email: str, data: dict) -> None:
        key = f"sms_attempts:{email}"
        mapping = {
            "count": str(data.get("count", 0)),
            "first_attempt": str(data.get("first_attempt", 0)),
            "blocked_until": str(data.get("blocked_until", 0)),
        }
        await self.redis.hset(key, mapping=mapping)
        await self.redis.expire(key, ATTEMPT_WINDOW + BLOCK_DURATION + 300)

    async def delete_sms_attempts(self, email: str) -> None:
        await self.redis.delete(f"sms_attempts:{email}")

    async def get_all_attempts(self) -> Dict[str, dict]:
        """Возвращает все записи о попытках (для фоновой очистки)."""
        attempts = {}
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match="sms_attempts:*", count=100)
            for key in keys:
                email = key.split(":", 1)[1]
                data = await self.redis.hgetall(key)
                if data:
                    attempts[email] = {
                        "count": int(data.get("count", 0)),
                        "first_attempt": float(data.get("first_attempt", 0)),
                        "blocked_until": float(data.get("blocked_until", 0)),
                    }
            if cursor == 0:
                break
        return attempts

    # ---------- Коды подтверждения ----------
    async def set_auth_code(self, email: str, code: str, expires: float) -> None:
        key = f"auth_code:{email}"
        await self.redis.hset(key, mapping={"code": code, "expires": str(expires)})
        await self.redis.expire(key, 320)  # 5 мин + запас

    async def get_auth_code(self, email: str) -> Optional[dict]:
        key = f"auth_code:{email}"
        data = await self.redis.hgetall(key)
        if not data:
            return None
        return {"code": data.get("code", ""), "expires": float(data.get("expires", 0))}

    async def delete_auth_code(self, email: str) -> None:
        await self.redis.delete(f"auth_code:{email}")

    # ---------- Время последнего запроса кода ----------
    async def get_last_code_request(self, email: str) -> Optional[float]:
        val = await self.redis.get(f"last_code_request:{email}")
        return float(val) if val else None

    async def set_last_code_request(self, email: str, timestamp: float) -> None:
        key = f"last_code_request:{email}"
        await self.redis.set(f"last_code_request:{email}", str(timestamp))
        await self.redis.expire(key, CODE_REQUEST_COOLDOWN + 120)

    # ---------- Защита от replay (использованные request_id) ----------
    async def mark_request_used(self, email: str, request_id: int) -> bool:
        """Возвращает True, если запрос уже был использован, иначе помечает и возвращает False."""
        key = f"used_request:{email}:{request_id}"
        # Устанавливаем ключ с TTL 60 секунд, если его не было
        # setnx возвращает True, если ключ был установлен (т.е. не существовал)
        result = await self.redis.setnx(key, str(time.time()))
        if result:
            await self.redis.expire(key, 60)  # автоматическое удаление через минуту
            return False
        return True  # ключ уже существовал → replay
    
    async def get_next_bot_id(self) -> int:
        return await self.redis.incr("counter:bot")
    
    async def check_reaction_rate(self, email: str) -> bool:
        key = f"reaction_rate:{email}"
        # Атомарный INCR устраняет гонку между чтением и записью счётчика
        count = await self.redis.incr(key)
        if count == 1:
            await self.redis.expire(key, REACTION_RATE_WINDOW)
        if count > REACTION_RATE_LIMIT:
            return False
        return True
    
    async def get_next_sticker_id(self) -> int:
        return await self.redis.incr("counter:sticker")
        
    async def save_callback_data(self, entity_id: int, message_id: int, reply_markup: dict, ttl: int = 86400):
        key = f"callback:{entity_id}:{message_id}"
        await self.redis.set(key, json.dumps(reply_markup), ex=ttl)

    async def get_callback_data(self, entity_id: int, message_id: int) -> Optional[dict]:
        key = f"callback:{entity_id}:{message_id}"
        data = await self.redis.get(key)
        return json.loads(data) if data else None

    async def delete_callback_data(self, entity_id: int, message_id: int):
        await self.redis.delete(f"callback:{entity_id}:{message_id}")
    
    async def get_next_poll_id(self) -> int:
        return await self.redis.incr("counter:poll")
    
    # В классе RedisStore добавьте следующие методы:

    async def add_user_session(self, email: str, session_id: str):
        """Сохраняет session_id в множество сессий пользователя."""
        key = f"user_sessions:{email}"
        await self.redis.sadd(key, session_id)
        # TTL устанавливать не будем, т.к. сессии сами удаляются по истечении

    async def remove_user_session(self, email: str, session_id: str):
        """Удаляет session_id из множества сессий пользователя."""
        key = f"user_sessions:{email}"
        await self.redis.srem(key, session_id)

    async def get_user_sessions(self, email: str) -> list:
        """Возвращает список session_id для пользователя."""
        key = f"user_sessions:{email}"
        return await self.redis.smembers(key)