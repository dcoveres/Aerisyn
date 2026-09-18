from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, delete, func, update
from typing import Optional, List, Dict
from .models import User, Entity, Message, Bot, Reaction, Sticker, Poll, PollVote
from db.models import (
    UserModel, EntityModel, EntityMemberModel, EntityAdminModel,
    EntityBannedModel, MessageModel, BotModel, MessageReactionModel,
    PollModel, PollVoteModel, BlacklistModel
)
from db.models import Base
from sqlalchemy import text
from db.models import StickerModel
from db.models import JoinRequestModel
import time
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresStore:
    def __init__(self, db_url: str):
        self.engine = create_async_engine(db_url, echo=False)
        self.async_session = async_sessionmaker(self.engine, expire_on_commit=False)


    # ----- Пользователи -----
    async def get_user(self, email: str) -> Optional[User]:
        async with self.async_session() as session:
            result = await session.execute(select(UserModel).where(UserModel.email == email))
            model = result.scalar_one_or_none()
            if not model:
                return None
            return self._to_user(model)

    async def get_user_by_username(self, username: str) -> Optional[User]:
        async with self.async_session() as session:
            result = await session.execute(select(UserModel).where(UserModel.username == username))
            model = result.scalar_one_or_none()
            return self._to_user(model) if model else None

    async def create_user(self, email: str, public_key: str) -> User:
        async with self.async_session() as session:
            model = UserModel(email=email, public_key=public_key)
            session.add(model)
            await session.commit()
            return self._to_user(model)

    async def update_user(self, user: User):
        async with self.async_session() as session:
            model = await session.get(UserModel, user.email)
            if model:
                model.public_key = user.public_key
                model.private_key = user.private_key
                model.username = user.username
                model.first_name = user.first_name
                model.bio = user.bio
                model.show_email = user.show_email
                model.tags = user.tags
                model.spam_restricted_until = user.spam_restricted_until
                await session.commit()

    async def set_spam_restriction(self, email: str, until: float) -> bool:
        """Устанавливает время окончания анти-спам ограничения для пользователя."""
        async with self.async_session() as session:
            result = await session.execute(
                update(UserModel).where(UserModel.email == email).values(spam_restricted_until=until)
            )
            await session.commit()
            return result.rowcount > 0

    async def has_message_from(self, entity_id: int, email: str) -> bool:
        """Проверяет, отправлял ли пользователь хотя бы одно сообщение в данной сущности."""
        async with self.async_session() as session:
            result = await session.execute(
                select(MessageModel.id).where(
                    MessageModel.entity_id == entity_id,
                    MessageModel.from_email == email
                ).limit(1)
            )
            return result.scalar_one_or_none() is not None

    # ----- Сущности -----
    async def get_entity(self, entity_id: int) -> Optional[Entity]:
        async with self.async_session() as session:
            entity_model = await session.get(EntityModel, entity_id)
            if not entity_model:
                return None
            members = await self._get_entity_members(session, entity_id)
            admins = await self._get_entity_admins(session, entity_id)
            banned = await self._get_entity_banned(session, entity_id)            
            entity = self._to_entity(entity_model, members, admins, banned)
            entity.is_private = entity_model.is_private
            return entity

    async def create_entity(self, entity: Entity):
        async with self.async_session() as session:
            model = EntityModel(
                id=entity.id,
                type=entity.type,
                name=entity.name,
                owner_email=entity.owner,
                key=entity.key,
                username=entity.username,      # <-- добавить
                is_private=entity.is_private   # <-- добавить
            )
            session.add(model)
            await session.commit()

    async def update_entity(self, entity: Entity):
        async with self.async_session() as session:
            model = await session.get(EntityModel, entity.id)
            if model:
                model.name = entity.name
                model.key = entity.key
                await session.commit()

    async def delete_entity(self, entity_id: int) -> bool:
        async with self.async_session() as session:
            model = await session.get(EntityModel, entity_id)
            if not model:
                return False
            await session.delete(model)
            # Каскадное удаление связей и сообщений
            await session.execute(delete(EntityMemberModel).where(EntityMemberModel.entity_id == entity_id))
            await session.execute(delete(EntityAdminModel).where(EntityAdminModel.entity_id == entity_id))
            await session.execute(delete(EntityBannedModel).where(EntityBannedModel.entity_id == entity_id))
            await session.execute(delete(MessageModel).where(MessageModel.entity_id == entity_id))
            await session.commit()
            return True

    # ----- Члены, админы, баны -----
    async def add_member(self, entity_id: int, email: str):
        async with self.async_session() as session:
            model = EntityMemberModel(entity_id=entity_id, email=email)
            session.add(model)
            await session.commit()

    async def remove_member(self, entity_id: int, email: str):
        async with self.async_session() as session:
            await session.execute(
                delete(EntityMemberModel).where(
                    EntityMemberModel.entity_id == entity_id,
                    EntityMemberModel.email == email
                )
            )
            await session.commit()

    async def add_admin(self, entity_id: int, email: str):
        async with self.async_session() as session:
            model = EntityAdminModel(entity_id=entity_id, email=email)
            session.add(model)
            await session.commit()

    async def remove_admin(self, entity_id: int, email: str):
        async with self.async_session() as session:
            await session.execute(
                delete(EntityAdminModel).where(
                    EntityAdminModel.entity_id == entity_id,
                    EntityAdminModel.email == email
                )
            )
            await session.commit()

    async def add_banned(self, entity_id: int, email: str):
        async with self.async_session() as session:
            model = EntityBannedModel(entity_id=entity_id, email=email)
            session.add(model)
            await session.commit()

    async def remove_banned(self, entity_id: int, email: str):
        async with self.async_session() as session:
            await session.execute(
                delete(EntityBannedModel).where(
                    EntityBannedModel.entity_id == entity_id,
                    EntityBannedModel.email == email
                )
            )
            await session.commit()

    async def get_entity_members(self, entity_id: int) -> List[str]:
        async with self.async_session() as session:
            return await self._get_entity_members(session, entity_id)

    # ----- Сообщения -----
    async def add_message(self, message: Message) -> Message:
        async with self.async_session() as session:
            model = MessageModel(
                entity_id=message.entity_id,  # нужно добавить поле entity_id в Message? В текущей модели нет, надо добавить
                id=message.id,
                from_email=message.from_email,
                content=message.content,
                timestamp=message.timestamp,
                type=message.type,
                filename=message.filename,
                file_size=message.file_size,
                file_checksum=message.file_checksum,
                file_path=message.file_path,
                reply_to_entity_id=message.reply_to_entity_id,
                reply_to_message_id=message.reply_to_message_id,
            )
            session.add(model)
            await session.commit()
            return message

    async def get_messages(self, entity_id: int, limit: int) -> List[Message]:
        async with self.async_session() as session:
            result = await session.execute(
                select(MessageModel)
                .where(MessageModel.entity_id == entity_id)
                .order_by(MessageModel.timestamp.desc())
                .limit(limit)
            )
            models = result.scalars().all()
            return [self._to_message(m) for m in reversed(models)]  # в хронологическом порядке

    async def get_message(self, entity_id: int, message_id: int) -> Optional[Message]:
        async with self.async_session() as session:
            result = await session.execute(
                select(MessageModel).where(
                    MessageModel.entity_id == entity_id,
                    MessageModel.id == message_id
                )
            )
            model = result.scalar_one_or_none()
            return self._to_message(model) if model else None

    async def delete_message(self, entity_id: int, message_id: int) -> bool:
        async with self.async_session() as session:
            result = await session.execute(
                delete(MessageModel).where(
                    MessageModel.entity_id == entity_id,
                    MessageModel.id == message_id
                )
            )
            await session.commit()
            return result.rowcount > 0

    # ----- Вспомогательные методы преобразования -----
    @staticmethod
    def _to_user(model: UserModel) -> User:
        return User(
            email=model.email,
            public_key=model.public_key,
            private_key=model.private_key,
            username=model.username,
            first_name=model.first_name,
            bio=model.bio,
            show_email=model.show_email,
            tags=model.tags or [],
            entities=[],  # не храним, получаем отдельно при необходимости
            spam_restricted_until=model.spam_restricted_until or 0.0,
        )

    @staticmethod
    def _to_entity(model: EntityModel, members: List[str], admins: List[str], banned: List[str]) -> Entity:
        return Entity(
            id=model.id,
            type=model.type,
            name=model.name,
            owner=model.owner_email,
            key=model.key,
            members=members,
            admins=admins,
            banned=banned,
            username=model.username,
            messages=[],  # сообщения загружаются отдельно
            is_private=model.is_private,
        )

    @staticmethod
    def _to_message(model: MessageModel) -> Message:
        return Message(
            id=model.id,
            from_email=model.from_email,
            content=model.content,
            timestamp=model.timestamp,
            type=model.type,
            filename=model.filename,
            file_size=model.file_size,
            file_checksum=model.file_checksum,
            file_path=model.file_path,
            entity_id=model.entity_id,  # TODO
            reply_to_entity_id=model.reply_to_entity_id,
            reply_to_message_id=model.reply_to_message_id,
        )

    async def _get_entity_members(self, session: AsyncSession, entity_id: int) -> List[str]:
        result = await session.execute(
            select(EntityMemberModel.email).where(EntityMemberModel.entity_id == entity_id)
        )
        return [row[0] for row in result.all()]

    async def _get_entity_admins(self, session: AsyncSession, entity_id: int) -> List[str]:
        result = await session.execute(
            select(EntityAdminModel.email).where(EntityAdminModel.entity_id == entity_id)
        )
        return [row[0] for row in result.all()]

    async def _get_entity_banned(self, session: AsyncSession, entity_id: int) -> List[str]:
        result = await session.execute(
            select(EntityBannedModel.email).where(EntityBannedModel.entity_id == entity_id)
        )
        return [row[0] for row in result.all()]
    
    
    async def get_entity_admins(self, entity_id: int) -> List[str]:
        async with self.async_session() as session:
            return await self._get_entity_admins(session, entity_id)

    async def get_entity_banned(self, entity_id: int) -> List[str]:
        async with self.async_session() as session:
            return await self._get_entity_banned(session, entity_id) 
    
    async def get_entity_by_username(self, username: str) -> Optional[Entity]:
        async with self.async_session() as session:
          result = await session.execute(
              select(EntityModel).where(EntityModel.username == username)
          )
          model = result.scalar_one_or_none()
          if not model:
              return None
          members = await self._get_entity_members(session, model.id)
          admins = await self._get_entity_admins(session, model.id)
          banned = await self._get_entity_banned(session, model.id)
          return self._to_entity(model, members, admins, banned)  
      
    async def update_entity_username(self, entity_id: int, username: Optional[str]) -> bool:
        """Обновляет username сущности. Возвращает True при успехе."""
        async with self.async_session() as session:
             model = await session.get(EntityModel, entity_id)
             if not model:
                 return False
             model.username = username
             await session.commit()
             return True
      
    async def username_exists_anywhere(self, username: str) -> bool:
        """Возвращает True, если username занят пользователем или сущностью."""
        if not username:
            return False
        async with self.async_session() as session:
            # Проверка пользователей
            result = await session.execute(select(UserModel).where(UserModel.username == username))
            if result.scalar_one_or_none():
                return True
            # Проверка сущностей
            result = await session.execute(select(EntityModel).where(EntityModel.username == username))
            if result.scalar_one_or_none():
                return True
            
            # Проверка ботов
            result = await session.execute(select(BotModel).where(BotModel.username == username))
            if result.scalar_one_or_none():
                return True
        return False
    
    async def create_bot(self, bot: Bot) -> Bot:
        async with self.async_session() as session:
            model = BotModel(
                id=bot.id,
                username=bot.username,
                name=bot.name,
                owner_email=bot.owner_email,
                token=bot.token,
                public_key=bot.public_key,
                private_key=bot.private_key,
                is_active=bot.is_active,
                created_at=bot.created_at,
            )
            session.add(model)
            await session.commit()
            return bot

    async def get_bot(self, bot_id: int) -> Optional[Bot]:
        async with self.async_session() as session:
            model = await session.get(BotModel, bot_id)
            return self._to_bot(model) if model else None

    async def get_bot_by_token(self, token: str) -> Optional[Bot]:
        async with self.async_session() as session:
            result = await session.execute(select(BotModel).where(BotModel.token == token))
            model = result.scalar_one_or_none()
            return self._to_bot(model) if model else None

    async def get_bot_by_username(self, username: str) -> Optional[Bot]:
        async with self.async_session() as session:
            result = await session.execute(select(BotModel).where(BotModel.username == username))
            model = result.scalar_one_or_none()
            return self._to_bot(model) if model else None

    async def get_bots_by_owner(self, owner_email: str) -> List[Bot]:
        async with self.async_session() as session:
            result = await session.execute(select(BotModel).where(BotModel.owner_email == owner_email))
            models = result.scalars().all()
            return [self._to_bot(m) for m in models]

    async def delete_bot(self, bot_id: int) -> bool:
        async with self.async_session() as session:
            model = await session.get(BotModel, bot_id)
            if not model:
                return False
            await session.delete(model)
            await session.commit()
            return True

    async def update_bot(self, bot: Bot):
        async with self.async_session() as session:
           model = await session.get(BotModel, bot.id)
           if model:
               model.username = bot.username
               model.name = bot.name
               model.is_active = bot.is_active
               await session.commit()

    # вспомогательный метод преобразования
    @staticmethod
    def _to_bot(model: BotModel) -> Bot:
        return Bot(
            id=model.id,
            username=model.username,
            name=model.name,
            owner_email=model.owner_email,
            token=model.token,
            public_key=model.public_key,
            private_key=model.private_key,
            is_active=model.is_active,
            created_at=model.created_at,
        )
    

    async def get_entities_for_user(self, email: str) -> List[int]:
        async with self.async_session() as session:
            # Получаем ID из entity_members
            stmt1 = select(EntityMemberModel.entity_id).where(EntityMemberModel.email == email)
            result1 = await session.execute(stmt1)
            ids1 = [row[0] for row in result1.all()]
            # Получаем ID из entities (владелец)
            stmt2 = select(EntityModel.id).where(EntityModel.owner_email == email)
            result2 = await session.execute(stmt2)
            ids2 = [row[0] for row in result2.all()]
            # Объединяем и убираем дубли
            return list(set(ids1 + ids2))
    # Добавить в PostgresStore:

    # ----- Реакции -----
    async def add_reaction(self, reaction: Reaction) -> None:
        async with self.async_session() as session:
            model = MessageReactionModel(
                message_id=reaction.message_id,
                email=reaction.email,
                reaction_type=reaction.reaction_type,
                entity_id=reaction.entity_id,
                timestamp=reaction.timestamp
            )
            session.add(model)
            await session.commit()

    async def remove_reaction(self, message_id: int, email: str) -> bool:
        async with self.async_session() as session:
            result = await session.execute(
                delete(MessageReactionModel).where(
                    MessageReactionModel.message_id == message_id,
                    MessageReactionModel.email == email
                )
            )
            await session.commit()
            return result.rowcount > 0

    async def get_reactions_for_message(self, message_id: int) -> Dict[str, int]:
        async with self.async_session() as session:
            result = await session.execute(
                select(MessageReactionModel.reaction_type)
                .where(MessageReactionModel.message_id == message_id)
            )
            rows = result.all()
            counts = {}
            for (rt,) in rows:
                counts[rt] = counts.get(rt, 0) + 1
            return counts

    async def get_user_reaction(self, message_id: int, email: str) -> Optional[str]:
        async with self.async_session() as session:
            result = await session.execute(
                select(MessageReactionModel.reaction_type)
                .where(
                    MessageReactionModel.message_id == message_id,
                    MessageReactionModel.email == email
                )
            )
            row = result.scalar_one_or_none()
            return row

    # services/postgres_store.py

    async def get_reactions_for_messages(self, message_ids: list, email: str) -> Dict[int, dict]:
        if not message_ids:
            return {}

        async with self.async_session() as session:
            stmt = select(
                MessageReactionModel.message_id,
                MessageReactionModel.reaction_type,
                MessageReactionModel.email
            ).where(MessageReactionModel.message_id.in_(message_ids))
            rows = await session.execute(stmt)
            results = rows.all()

        result = {}
        for row in results:
            mid = row.message_id
            if mid not in result:
                result[mid] = {'counts': {}, 'my_reaction': None}
            rt = row.reaction_type
            result[mid]['counts'][rt] = result[mid]['counts'].get(rt, 0) + 1
            if row.email == email:
                result[mid]['my_reaction'] = rt

        for mid in message_ids:
            if mid not in result:
                result[mid] = {'counts': {}, 'my_reaction': None}

        return result

    async def get_user_reactions_for_messages(self, message_ids: List[int], email: str) -> Dict[int, str]:
        if not message_ids:
            return {}
        async with self.async_session() as session:
            result = await session.execute(
                select(MessageReactionModel.message_id, MessageReactionModel.reaction_type)
                .where(
                    MessageReactionModel.message_id.in_(message_ids),
                    MessageReactionModel.email == email
                )
            )
            rows = result.all()
            return {mid: rt for mid, rt in rows}

    # Обновить delete_message - добавить удаление реакций
    async def delete_message(self, entity_id: int, message_id: int) -> bool:
        async with self.async_session() as session:
            await session.execute(
                delete(MessageReactionModel).where(MessageReactionModel.message_id == message_id)
            )
            result = await session.execute(
                delete(MessageModel).where(
                    MessageModel.entity_id == entity_id,
                    MessageModel.id == message_id
                )
            )
            await session.commit()
            return result.rowcount > 0

    # Обновить delete_entity - добавить удаление реакций
    async def delete_entity(self, entity_id: int) -> bool:
        async with self.async_session() as session:
            model = await session.get(EntityModel, entity_id)
            if not model:
                return False
            await session.delete(model)
            await session.execute(delete(EntityMemberModel).where(EntityMemberModel.entity_id == entity_id))
            await session.execute(delete(EntityAdminModel).where(EntityAdminModel.entity_id == entity_id))
            await session.execute(delete(EntityBannedModel).where(EntityBannedModel.entity_id == entity_id))
            await session.execute(delete(MessageModel).where(MessageModel.entity_id == entity_id))
            await session.execute(delete(MessageReactionModel).where(MessageReactionModel.entity_id == entity_id))
            await session.commit()
            return True
    
    # ----- Стикеры -----
    async def get_stickers(self, query: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Sticker]:
        from db.models import StickerModel
        async with self.async_session() as session:
            stmt = select(StickerModel)
            if query:
                # поиск по имени или тегам (упрощённо)
                stmt = stmt.where(
                    (StickerModel.name.ilike(f"%{query}%")) |
                    (StickerModel.tags.contains([query]))  # точное совпадение с тегом
                )
            stmt = stmt.offset(offset).limit(limit).order_by(StickerModel.created_at.desc())
            result = await session.execute(stmt)
            models = result.scalars().all()
            return [self._to_sticker(m) for m in models]

    async def get_sticker(self, sticker_id: int) -> Optional[Sticker]:
        from db.models import StickerModel
        async with self.async_session() as session:
            model = await session.get(StickerModel, sticker_id)
            return self._to_sticker(model) if model else None

    @staticmethod
    def _to_sticker(model) -> Sticker:
        from .models import Sticker
        return Sticker(
            id=model.id,
            name=model.name,
            file_path=model.file_path,
            tags=model.tags or [],
            created_at=model.created_at,
        )
    
    async def create_sticker(self, sticker: Sticker) -> Sticker:
        from db.models import StickerModel
        async with self.async_session() as session:
            model = StickerModel(
                id=sticker.id,
                name=sticker.name,
                file_path=sticker.file_path,
                tags=sticker.tags,
                created_at=sticker.created_at
            )
            session.add(model)
            await session.commit()
            return sticker
    
    # ----- Заявки на вступление -----
    async def create_join_request(self, entity_id: int, email: str, expires_at: float) -> None:
        async with self.async_session() as session:
            model = JoinRequestModel(
                entity_id=entity_id,
                email=email,
                status='pending',
                created_at=time.time(),
                expires_at=expires_at
            )
            session.add(model)
            await session.commit()

    async def get_join_request(self, entity_id: int, email: str) -> Optional[dict]:
        async with self.async_session() as session:
            result = await session.execute(
                select(JoinRequestModel).where(
                    JoinRequestModel.entity_id == entity_id,
                    JoinRequestModel.email == email,
                    JoinRequestModel.status == 'pending'
                )
            )
            model = result.scalar_one_or_none()
            if model:
                return {'id': model.id, 'entity_id': model.entity_id, 'email': model.email,
                        'created_at': model.created_at, 'expires_at': model.expires_at}
            return None

    async def get_pending_join_requests(self, entity_id: int) -> List[dict]:
        async with self.async_session() as session:
            result = await session.execute(
                select(JoinRequestModel)
                .where(
                    JoinRequestModel.entity_id == entity_id,
                    JoinRequestModel.status == 'pending'
                )
                .order_by(JoinRequestModel.created_at)
            )
            models = result.scalars().all()
            return [{
                'id': m.id,
                'email': m.email,
                'created_at': m.created_at,
                'expires_at': m.expires_at
            } for m in models]

    async def approve_join_request(self, entity_id: int, email: str) -> bool:
        async with self.async_session() as session:
            result = await session.execute(
                select(JoinRequestModel).where(
                    JoinRequestModel.entity_id == entity_id,
                    JoinRequestModel.email == email,
                    JoinRequestModel.status == 'pending'
                )
            )
            model = result.scalar_one_or_none()
            if not model:
                return False
            model.status = 'approved'
            await session.commit()
            return True

    async def reject_join_request(self, entity_id: int, email: str) -> bool:
        async with self.async_session() as session:
            result = await session.execute(
                delete(JoinRequestModel).where(
                    JoinRequestModel.entity_id == entity_id,
                    JoinRequestModel.email == email,
                    JoinRequestModel.status == 'pending'
                )
            )
            await session.commit()
            return result.rowcount > 0

    async def delete_expired_join_requests(self) -> int:
        """Удаляет все просроченные заявки. Возвращает количество удалённых."""
        async with self.async_session() as session:
            now = time.time()
            result = await session.execute(
                delete(JoinRequestModel).where(
                    JoinRequestModel.status == 'pending',
                    JoinRequestModel.expires_at < now
                )
            )
            await session.commit()
            return result.rowcount
            
    async def update_entity_private(self, entity_id: int, is_private: bool) -> bool:
        async with self.async_session() as session:
            model = await session.get(EntityModel, entity_id)
            if not model:
                return False
            model.is_private = is_private
            await session.commit()
            return True
    
    async def update_message(self, message: Message) -> None:
        async with self.async_session() as session:
            model = await session.get(MessageModel, (message.entity_id, message.id))
            if model:
                model.content = message.content

                await session.commit()
    
    # services/postgres_store.py – внутри класса PostgresStore

    async def get_entity_members_count(self, entity_id: int) -> int:
        async with self.async_session() as session:
            result = await session.execute(
                select(func.count()).select_from(EntityMemberModel)
                .where(EntityMemberModel.entity_id == entity_id)
            )
            return result.scalar() or 0

    async def get_messages_count_by_days(self, entity_id: int, days: int) -> int:
        threshold = time.time() - days * 86400
        async with self.async_session() as session:
            result = await session.execute(
                select(func.count())
                .select_from(MessageModel)
                .where(MessageModel.entity_id == entity_id, MessageModel.timestamp >= threshold)
            )
            return result.scalar() or 0

    async def get_reactions_count_by_days(self, entity_id: int, days: int) -> int:
        threshold = time.time() - days * 86400
        async with self.async_session() as session:
            result = await session.execute(
                select(func.count())
                .select_from(MessageReactionModel)
                .where(MessageReactionModel.entity_id == entity_id, MessageReactionModel.timestamp >= threshold)
            )
            return result.scalar() or 0

    async def get_activity_by_day(self, entity_id: int, days: int) -> list:
        threshold = time.time() - days * 86400
        async with self.async_session() as session:
            # Формируем выражение для преобразования timestamp в дату в зависимости от диалекта
            if self.engine.dialect.name == 'sqlite':
                date_expr = func.date(func.datetime(MessageModel.timestamp, 'unixepoch'))
            else:  # postgresql
                date_expr = func.date(func.to_timestamp(MessageModel.timestamp))

            stmt = select(
                date_expr.label('day'),
                func.count().label('count')
            ).where(
                MessageModel.entity_id == entity_id,
                MessageModel.timestamp >= threshold
            ).group_by(date_expr).order_by('day')

            result = await session.execute(stmt)
            rows = result.all()
            return [{"day": row.day, "count": row.count} for row in rows]
    
    async def create_poll(self, poll: Poll) -> Poll:
        async with self.async_session() as session:
            model = PollModel(
                id=poll.id,
                message_id=poll.message_id,
                entity_id=poll.entity_id,
                question=poll.question,
                options=poll.options,
                total_votes=poll.total_votes,
                created_by=poll.created_by,
                created_at=poll.created_at,
                is_closed=poll.is_closed,
            )
            session.add(model)
            await session.commit()
            return poll

    async def get_poll_by_message_id(self, entity_id: int, message_id: int) -> Optional[Poll]:
        async with self.async_session() as session:
            result = await session.execute(
                select(PollModel).where(
                    PollModel.entity_id == entity_id,
                    PollModel.message_id == message_id
                )
            )
            model = result.scalar_one_or_none()
            if not model:
                return None
            return self._to_poll(model)

    async def get_poll(self, poll_id: int) -> Optional[Poll]:
        async with self.async_session() as session:
            model = await session.get(PollModel, poll_id)
            return self._to_poll(model) if model else None

    async def add_vote(self, poll_id: int, user_email: str, option_index: int) -> bool:
        async with self.async_session() as session:
            existing = await session.execute(
                select(PollVoteModel).where(
                    PollVoteModel.poll_id == poll_id,
                    PollVoteModel.user_email == user_email
                )
            )
            if existing.scalar_one_or_none():
                return False
            vote = PollVoteModel(poll_id=poll_id, user_email=user_email, option_index=option_index)
            session.add(vote)
            await session.execute(
                update(PollModel).where(PollModel.id == poll_id).values(total_votes=PollModel.total_votes + 1)
            )
            await session.commit()
            return True

    async def get_user_vote(self, poll_id: int, user_email: str) -> Optional[int]:
        async with self.async_session() as session:
            result = await session.execute(
                select(PollVoteModel.option_index).where(
                    PollVoteModel.poll_id == poll_id,
                    PollVoteModel.user_email == user_email
                )
            )
            row = result.scalar_one_or_none()
            return row

    async def get_poll_results(self, poll_id: int) -> Dict[int, int]:
        async with self.async_session() as session:
            result = await session.execute(
                select(PollVoteModel.option_index, func.count())
                .where(PollVoteModel.poll_id == poll_id)
                .group_by(PollVoteModel.option_index)
            )
            rows = result.all()
            counts = {idx: count for idx, count in rows}
            poll = await self.get_poll(poll_id)
            if poll:
                for i in range(len(poll.options)):
                    counts.setdefault(i, 0)
            return counts

    async def close_poll(self, poll_id: int) -> bool:
        async with self.async_session() as session:
            result = await session.execute(
                update(PollModel).where(PollModel.id == poll_id).values(is_closed=True)
            )
            await session.commit()
            return result.rowcount > 0

    async def update_poll(self, poll: Poll):
        async with self.async_session() as session:
            model = await session.get(PollModel, poll.id)
            if model:
                model.question = poll.question
                model.options = poll.options
                model.total_votes = poll.total_votes
                model.is_closed = poll.is_closed
                await session.commit()

    @staticmethod
    def _to_poll(model: PollModel) -> Poll:
        return Poll(
            id=model.id,
            message_id=model.message_id,
            entity_id=model.entity_id,
            question=model.question,
            options=model.options,
            total_votes=model.total_votes,
            created_by=model.created_by,
            created_at=model.created_at,
            is_closed=model.is_closed,
        )
    
    async def search_users(self, query: str, limit: int = 50) -> List[dict]:
        """Поиск пользователей по email, username или first_name (подстрока)."""
        async with self.async_session() as session:
            stmt = select(UserModel).where(
                (UserModel.email.ilike(f"%{query}%")) |
                (UserModel.username.ilike(f"%{query}%")) |
                (UserModel.first_name.ilike(f"%{query}%"))
            ).limit(limit)
            result = await session.execute(stmt)
            models = result.scalars().all()
            return [
                {
                    "type": "user",
                    "email": m.email,
                    "username": m.username,
                    "first_name": m.first_name,
                    "public_key": m.public_key,
                    "bio": m.bio,
                    "show_email": m.show_email,
                }
                for m in models
            ]

    async def search_entities(self, query: str, limit: int = 50) -> List[dict]:
        """Поиск ТОЛЬКО ПУБЛИЧНЫХ групп и каналов (НЕ чатов)."""
        async with self.async_session() as session:
            stmt = select(EntityModel).where(
                (EntityModel.name.ilike(f"%{query}%")) |
                (EntityModel.username.ilike(f"%{query}%")),
                EntityModel.is_private == False,
                EntityModel.type != 'chat'  # исключаем только личные чаты, всё остальное ищем
            ).limit(limit)
            result = await session.execute(stmt)
            models = result.scalars().all()
            logger.info(f"Found {len(models)} public entities for query '{query}'")
            return [
                {
                    "type": "entity",
                    "id": m.id,
                    "name": m.name,
                    "username": m.username,
                    "owner_email": m.owner_email,
                    "is_private": m.is_private,
                }
                for m in models
            ]

    async def search_bots(self, query: str, limit: int = 50) -> List[dict]:
        """Поиск ботов по имени или username."""
        async with self.async_session() as session:
            stmt = select(BotModel).where(
                (BotModel.name.ilike(f"%{query}%")) |
                (BotModel.username.ilike(f"%{query}%"))
            ).limit(limit)
            result = await session.execute(stmt)
            models = result.scalars().all()
            return [
                {
                    "type": "bot",
                    "id": m.id,
                    "name": m.name,
                    "username": m.username,
                    "owner_email": m.owner_email,
                    "is_active": m.is_active,
                }
                for m in models
            ]
    
    # ----- Черный список -----
    async def add_blacklist(self, blocker_email: str, blocked_identifier: str, blocked_type: str) -> bool:
        from db.models import BlacklistModel
        async with self.async_session() as session:
            existing = await session.execute(
                select(BlacklistModel).where(
                    BlacklistModel.blocker_email == blocker_email,
                    BlacklistModel.blocked_identifier == blocked_identifier
                )
            )
            if existing.scalar_one_or_none():
                return False
            model = BlacklistModel(
                blocker_email=blocker_email,
                blocked_identifier=blocked_identifier,
                blocked_type=blocked_type,
                created_at=time.time()
            )
            session.add(model)
            await session.commit()
            return True

    async def remove_blacklist(self, blocker_email: str, blocked_identifier: str) -> bool:
        from db.models import BlacklistModel
        async with self.async_session() as session:
            result = await session.execute(
                delete(BlacklistModel).where(
                    BlacklistModel.blocker_email == blocker_email,
                    BlacklistModel.blocked_identifier == blocked_identifier
                )
            )
            await session.commit()
            return result.rowcount > 0

    
    async def get_blacklist(self, blocker_email: str) -> List[dict]:
        from db.models import BlacklistModel, BotModel, UserModel
        async with self.async_session() as session:
            result = await session.execute(
                select(BlacklistModel).where(BlacklistModel.blocker_email == blocker_email)
            )
            models = result.scalars().all()
            items = []
            for m in models:
                display_name = m.blocked_identifier
                if m.blocked_type == 'bot':
                    # Убираем префикс "bot_" и получаем id
                    try:
                        bot_id = int(m.blocked_identifier.split('_')[1])
                        bot = await session.get(BotModel, bot_id)
                        if bot:
                            display_name = bot.name
                    except (IndexError, ValueError):
                        pass
                else:
                    # Для пользователя пробуем получить first_name или username
                    user = await session.get(UserModel, m.blocked_identifier)
                    if user:
                        display_name = user.first_name or user.username or user.email
                items.append({
                    'blocked_identifier': m.blocked_identifier,
                    'blocked_type': m.blocked_type,
                    'created_at': m.created_at,
                    'display_name': display_name
                })
            return items

    async def is_blocked(self, blocker_email: str, blocked_identifier: str) -> bool:
        from db.models import BlacklistModel
        async with self.async_session() as session:
            result = await session.execute(
                select(BlacklistModel).where(
                    BlacklistModel.blocker_email == blocker_email,
                    BlacklistModel.blocked_identifier == blocked_identifier
                )
            )
            return result.scalar_one_or_none() is not None

    async def get_other_participant_in_chat(self, entity_id: int, my_email: str) -> Optional[str]:
        async with self.async_session() as session:
            entity = await session.get(EntityModel, entity_id)
            if not entity or entity.type != 'chat':
                return None
            members = await self._get_entity_members(session, entity_id)
            others = [m for m in members if m != my_email]
            return others[0] if others else None

    async def find_chat_between(self, email1: str, email2: str) -> Optional[Entity]:
        async with self.async_session() as session:
            subq1 = select(EntityMemberModel.entity_id).where(EntityMemberModel.email == email1)
            subq2 = select(EntityMemberModel.entity_id).where(EntityMemberModel.email == email2)
            stmt = select(EntityModel).where(
                EntityModel.type == 'chat',
                EntityModel.id.in_(subq1),
                EntityModel.id.in_(subq2)
            )
            result = await session.execute(stmt)
            entity_model = result.scalar_one_or_none()
            if entity_model:
                members = await self._get_entity_members(session, entity_model.id)
                admins = await self._get_entity_admins(session, entity_model.id)
                banned = await self._get_entity_banned(session, entity_model.id)
                return self._to_entity(entity_model, members, admins, banned)
            return None