import base64
from axiso import EdDSA
from protocol import serialize_sorted
from typing import Optional 

class SecurityService:
    def __init__(self, db_store, redis_store):
        self.db_store = db_store
        self.redis_store = redis_store

    async def verify_signature(self, obj: dict, email: Optional[str], bot_id: Optional[int] = None) -> bool:
        signature_b64 = obj.get("signature")
        if not signature_b64:
            return False
        try:
            signature = base64.urlsafe_b64decode(signature_b64)
        except Exception:
            return False

        public_key = None
        if bot_id is not None:
            bot = await self.db_store.get_bot(bot_id)
            if bot:
                public_key = bot.public_key
        elif email:
            user = await self.db_store.get_user(email)
            if user:
                public_key = user.public_key

        if not public_key:
            return False

        obj_without_sig = {k: v for k, v in obj.items() if k != "signature"}
        try:
            payload = serialize_sorted(obj_without_sig)
            return bool(EdDSA.verify(payload, signature, public_key))
        except Exception:
            return False

    async def is_request_replayed(self, session_id: str, request_id: int) -> bool:
        return await self.redis_store.mark_request_used(session_id, request_id)