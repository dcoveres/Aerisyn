# services/models.py
from dataclasses import dataclass, field
from typing import List, Optional
import time

@dataclass
class User:
    email: str
    public_key: str
    private_key: str
    tags: List[str] = field(default_factory=list)
    entities: List[int] = field(default_factory=list)
    username: Optional[str] = None
    first_name: str = ""
    bio: str = ""
    show_email: bool = True
    spam_restricted_until: float = 0.0  # unix-время окончания анти-спам ограничения (0 = нет ограничения)

@dataclass
class Message:
    id: int
    from_email: str
    content: str
    timestamp: float
    type: str = "text"
    filename: Optional[str] = None
    file_size: Optional[int] = None
    file_checksum: Optional[str] = None
    file_path: Optional[str] = None
    entity_id: Optional[int] = None
    reply_to_entity_id: Optional[int] = None
    reply_to_message_id: Optional[int] = None

@dataclass
class Entity:
    id: int
    type: str
    name: str
    owner: str
    admins: List[str] = field(default_factory=list)
    members: List[str] = field(default_factory=list)
    key: str = ""
    messages: List[Message] = field(default_factory=list)
    banned: List[str] = field(default_factory=list)
    username: Optional[str] = None
    is_private: bool = False   # <-- новое поле

@dataclass
class Bot:
    id: int
    username: Optional[str]
    name: str
    owner_email: str
    token: str
    public_key: str
    private_key: str
    is_active: bool = True
    created_at: float = field(default_factory=time.time)

@dataclass
class Reaction:
    message_id: int
    email: str
    reaction_type: str
    entity_id: int
    timestamp: float = field(default_factory=time.time)

@dataclass
class Sticker:
    id: int
    name: str
    file_path: str
    tags: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)

@dataclass
class Poll:
    id: int
    message_id: int
    entity_id: int
    question: str
    options: List[str]
    total_votes: int = 0
    created_by: str = ""
    created_at: float = 0.0
    is_closed: bool = False

@dataclass
class PollVote:
    poll_id: int
    user_email: str
    option_index: int
