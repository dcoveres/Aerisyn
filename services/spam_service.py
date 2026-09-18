# services/spam_service.py
import logging
import math
import time

from protocol import SPAM_BLOCK_DURATION

logger = logging.getLogger(__name__)


class SpamService:
    """
    Анти-спам ограничение по аналогии с Telegram.

    Если пользователь A первым написал незнакомому пользователю B (то есть
    создал с ним новый личный чат) и B заблокировал A, ни разу ему не
    ответив, то A на SPAM_BLOCK_DURATION теряет возможность первым писать
    незнакомым людям (создавать новые личные чаты). Уже существующие чаты
    ограничение не затрагивает.
    """

    def __init__(self, db_store):
        self.db_store = db_store

    async def register_unanswered_block(self, initiator_email: str, chat_entity, blocker_email: str) -> bool:
        """
        Вызывается при блокировке пользователя. Если chat_entity — это личный
        чат, который создал (то есть начал первым) именно initiator_email, и
        blocker_email ни разу не отвечал в этом чате, накладывает на
        initiator_email анти-спам ограничение.

        Возвращает True, если ограничение было наложено.
        """
        if not chat_entity or chat_entity.type != "chat":
            return False
        if chat_entity.owner != initiator_email:
            # Чат начал не тот, кого блокируют — правило не применяется
            return False

        replied = await self.db_store.has_message_from(chat_entity.id, blocker_email)
        if replied:
            return False

        until = time.time() + SPAM_BLOCK_DURATION
        try:
            await self.db_store.set_spam_restriction(initiator_email, until)
        except Exception as e:
            logger.error(f"Не удалось наложить анти-спам ограничение на {initiator_email}: {e}", exc_info=True)
            return False

        logger.info(
            f"Анти-спам ограничение наложено на {initiator_email} до {until} "
            f"(заблокирован пользователем {blocker_email} без ответа)"
        )
        return True

    async def get_restricted_until(self, email: str) -> float:
        user = await self.db_store.get_user(email)
        if not user:
            return 0.0
        return user.spam_restricted_until or 0.0

    async def is_restricted(self, email: str) -> bool:
        return await self.get_restricted_until(email) > time.time()

    async def get_remaining_seconds(self, email: str) -> float:
        until = await self.get_restricted_until(email)
        return max(0.0, until - time.time())

    @staticmethod
    def _days_left(remaining_seconds: float) -> int:
        return max(1, math.ceil(remaining_seconds / 86400))

    async def check_can_write_first(self, email: str) -> None:
        """
        Бросает ValueError, если пользователю сейчас нельзя первым писать
        незнакомым людям (создавать новый личный чат).
        """
        remaining = await self.get_remaining_seconds(email)
        if remaining > 0:
            days_left = self._days_left(remaining)
            raise ValueError(
                f"Вы не можете первым писать незнакомым пользователям ещё {days_left} дн. "
                f"Ограничение наложено из-за жалобы (блокировки без ответа)."
            )

    def format_status_text(self, until: float) -> str:
        now = time.time()
        if until and until > now:
            days_left = self._days_left(until - now)
            return (
                "🚫 На ваш аккаунт наложено временное ограничение.\n"
                f"Вы не можете первым писать незнакомым пользователям ещё {days_left} дн.\n\n"
                "Причина: адресат заблокировал вас, ни разу не ответив на ваше первое "
                "сообщение.\n\n"
                "Уже открытые чаты ограничение не затрагивает — писать в них можно как обычно."
            )
        return (
            "✅ Ваш аккаунт не имеет ограничений.\n"
            "Вы можете свободно писать другим пользователям, в том числе первым."
        )

    async def get_status_text(self, email: str) -> str:
        until = await self.get_restricted_until(email)
        return self.format_status_text(until)
