# db/models.py
from sqlalchemy import Column, String, Integer, Boolean, Float, ForeignKey, JSON, UniqueConstraint
from sqlalchemy.orm import declarative_base
Base = declarative_base()
from sqlalchemy.orm import relationship

Base = declarative_base()

class UserModel(Base):
    __tablename__ = 'users'
    email = Column(String, primary_key=True)
    public_key = Column(String, nullable=False)
    private_key = Column(String, nullable=False, default='')
    username = Column(String, unique=True, nullable=True)
    first_name = Column(String, default='')
    bio = Column(String, default='')
    show_email = Column(Boolean, default=True)
    tags = Column(JSON, default=list)
    spam_restricted_until = Column(Float, default=0.0)  # анти-спам: до какого времени нельзя первым писать незнакомцам

class EntityModel(Base):
    __tablename__ = 'entities'
    id = Column(Integer, primary_key=True)
    type = Column(String, nullable=False)
    name = Column(String, nullable=False)
    owner_email = Column(String, ForeignKey('users.email'), nullable=False)
    key = Column(String, default='')
    username = Column(String, unique=True, nullable=True)
    is_private = Column(Boolean, default=False) 

class EntityMemberModel(Base):
    __tablename__ = 'entity_members'
    entity_id = Column(Integer, ForeignKey('entities.id'), primary_key=True)
    email = Column(String, ForeignKey('users.email'), primary_key=True)

class EntityAdminModel(Base):
    __tablename__ = 'entity_admins'
    entity_id = Column(Integer, ForeignKey('entities.id'), primary_key=True)
    email = Column(String, ForeignKey('users.email'), primary_key=True)

class EntityBannedModel(Base):
    __tablename__ = 'entity_banned'
    entity_id = Column(Integer, ForeignKey('entities.id'), primary_key=True)
    email = Column(String, ForeignKey('users.email'), primary_key=True)

class MessageModel(Base):
    __tablename__ = 'messages'
    entity_id = Column(Integer, ForeignKey('entities.id'), primary_key=True)
    id = Column(Integer, primary_key=True)
    from_email = Column(String, ForeignKey('users.email'), nullable=False)
    content = Column(String, nullable=False)
    timestamp = Column(Float, nullable=False)
    type = Column(String, default='text')
    filename = Column(String, nullable=True)
    file_size = Column(Integer, nullable=True)
    file_checksum = Column(String, nullable=True)
    file_path = Column(String, nullable=True)
    reply_to_entity_id = Column(Integer, ForeignKey('entities.id'), nullable=True)
    reply_to_message_id = Column(Integer, nullable=True)

class BotModel(Base):
    __tablename__ = 'bots'
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True, nullable=True)
    name = Column(String, nullable=False)
    owner_email = Column(String, ForeignKey('users.email'), nullable=False)
    token = Column(String, unique=True, nullable=False)
    public_key = Column(String, nullable=False)
    private_key = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(Float, nullable=False)

class MessageReactionModel(Base):
    __tablename__ = 'message_reactions'
    message_id = Column(Integer, primary_key=True)       
    email = Column(String, primary_key=True)            
    reaction_type = Column(String, nullable=False)     
    entity_id = Column(Integer, ForeignKey('entities.id'), nullable=False)  
    timestamp = Column(Float, nullable=False)

class StickerModel(Base):
    __tablename__ = 'stickers'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)         
    file_path = Column(String, nullable=False)     
    tags = Column(JSON, default=list)            
    created_at = Column(Float, nullable=False)

class JoinRequestModel(Base):
    __tablename__ = 'join_requests'
    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey('entities.id'), nullable=False)
    email = Column(String, ForeignKey('users.email'), nullable=False)
    status = Column(String, default='pending')   # pending, approved, rejected
    created_at = Column(Float, nullable=False)
    expires_at = Column(Float, nullable=False)
    __table_args__ = (UniqueConstraint('entity_id', 'email', name='uq_join_request'),)

class PollModel(Base):
    __tablename__ = 'polls'
    id = Column(Integer, primary_key=True)
    message_id = Column(Integer, nullable=False)          # связь с сообщением
    entity_id = Column(Integer, ForeignKey('entities.id'), nullable=False)
    question = Column(String, nullable=False)
    options = Column(JSON, nullable=False)                # список строк
    total_votes = Column(Integer, default=0)
    created_by = Column(String, ForeignKey('users.email'), nullable=False)
    created_at = Column(Float, nullable=False)
    is_closed = Column(Boolean, default=False)

class PollVoteModel(Base):
    __tablename__ = 'poll_votes'
    poll_id = Column(Integer, ForeignKey('polls.id'), primary_key=True)
    user_email = Column(String, ForeignKey('users.email'), primary_key=True)
    option_index = Column(Integer, nullable=False)

class BlacklistModel(Base):
    __tablename__ = 'blacklist'
    id = Column(Integer, primary_key=True)
    blocker_email = Column(String, ForeignKey('users.email'), nullable=False)
    blocked_identifier = Column(String, nullable=False)   # email или "bot_<id>"
    blocked_type = Column(String, nullable=False)         # 'user' или 'bot'
    created_at = Column(Float, nullable=False)

    __table_args__ = (UniqueConstraint('blocker_email', 'blocked_identifier', name='uq_blacklist'),)
