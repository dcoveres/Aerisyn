# protocol.py
import json
import zlib
import hashlib
import re
from typing import Optional
from axiso import ChaCha20, HKDF
import msgpack
import logging
import os
import PIL
import secrets
logger = logging.getLogger(__name__)


# Константы протокола
MAX_MESSAGE_SIZE = 64 * 1024
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
MAX_HISTORY = 10000
SESSION_TIMEOUT = 3600          # 1 час
RATE_LIMIT = 10                 # команд в секунду
DEFAULT_WS_URL = "wss://127.0.0.1:4433"
STREAM_ID_CONTROL = 0
STORAGE_DIR = "storage/files"
MAX_CONCURRENT_UPLOADS = 5
MAX_FILENAME_LENGTH = 103  # 100 символов + 3 на расширение
REQUEST_CODE_TYPE = "request_code"
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9.]+@[a-zA-Z0-9.]+\.[a-zA-Z]+$')
MAX_BOTS_PER_USER = 10
BOT_AUTH_REQUEST = "bot_auth_request"
REFRESH_SESSION_TYPE = "refresh_session"
REFRESH_INTERVAL = 30 * 60  # 30 минут

# Теги пользователей
TAG_VERIFIED = "verified"
TAG_SCAM = "scam"

# Типы сущностей
ENTITY_TYPE_CHAT = "chat"
ENTITY_TYPE_GROUP = "group"
ENTITY_TYPE_CHANNEL = "channel"

# Параметры для защиты от брутфорса
MAX_ATTEMPTS = 5               # максимум неудачных попыток
ATTEMPT_WINDOW = 900           # окно в секундах (15 минут) для сброса счётчика
BLOCK_DURATION = 1800          # время блокировки в секундах (30 минут)
CODE_REQUEST_COOLDOWN = 60   # секунд между запросами кода

# Параметры для рейт-лимита реакций
REACTION_RATE_LIMIT = 30
REACTION_RATE_WINDOW = 60

# Команды для реакций
CMD_ADD_REACTION = "add_reaction"
CMD_REMOVE_REACTION = "remove_reaction"

# Тип уведомления о реакции
NOTIFICATION_REACTION_UPDATE = "reaction_update"

# Команды для стикеров
CMD_GET_STICKERS = "get_stickers"
CMD_SEND_STICKER = "send_sticker"
CMD_GET_STICKER_FILE = "get_sticker_file"

# Тип уведомления о новой заявке
NOTIFICATION_NEW_JOIN_REQUEST = "new_join_request"

# Тип сообщения для стикера
MESSAGE_TYPE_STICKER = "sticker"

# Максимальное количество стикеров в одном запросе
MAX_STICKERS_PER_REQUEST = 50

# Минимальная длина юзернейма
MIN_USERNAME_LENGTH = 4
USERNAME_REGEX = re.compile(r'^[a-zA-Z0-9_]{4,}$')

SIGNATURE_FIELD = "signature"   # поле для подписи в команде send_message

# Команды
CMD_SET_FIRST_NAME = "set_first_name"
CMD_CREATE_ENTITY = "create_entity"
CMD_JOIN_ENTITY = "join_entity"
CMD_LEAVE_ENTITY = "leave_entity"
CMD_DELETE_ENTITY = "delete_entity"
CMD_SEND_MESSAGE = "send_message"
CMD_GET_MESSAGES = "get_messages"
CMD_DELETE_MESSAGE = "delete_message"
CMD_PROMOTE_ADMIN = "promote_admin"
CMD_DEMOTE_ADMIN = "demote_admin"
CMD_GET_USER_INFO = "get_user_info"
CMD_DOWNLOAD_FILE = "download_file"
CMD_REQUEST_UPLOAD_STREAM = "request_upload_stream"
CMD_SET_ENTITY_USERNAME = "set_entity_username"
CMD_REFRESH_SESSION = "refresh_session"

# Новые команды для профиля
CMD_SET_USERNAME = "set_username"
CMD_SET_BIO = "set_bio"
CMD_SET_SHOW_EMAIL = "set_show_email"
CMD_GET_ENTITY_INFO = "get_entity_info"

#команды для ботов
CMD_CREATE_BOT = "create_bot"
CMD_DELETE_BOT = "delete_bot"
CMD_LIST_BOTS = "list_bots"
CMD_GET_BOT_TOKEN = "get_bot_token"
CMD_INVITE_BOT = "invite_bot"  # для добавления бота в сущность

# команды для функции заявок
CMD_SET_ENTITY_PRIVATE = "set_entity_private"
CMD_GET_JOIN_REQUESTS = "get_join_requests"
CMD_APPROVE_JOIN_REQUEST = "approve_join_request"
CMD_REJECT_JOIN_REQUEST = "reject_join_request"

# Инлайн-кнопки
CMD_CALLBACK_QUERY = "callback_query"
NOTIFICATION_CALLBACK = "callback_notification"

# Редактирование сообщений и ответ на callback
CMD_EDIT_MESSAGE = "edit_message"
CMD_ANSWER_CALLBACK = "answer_callback"

def validate_email(email: str) -> bool:
    """Проверяет, соответствует ли строка формату email."""
    if not email:
        return False
    return bool(EMAIL_REGEX.match(email))

def validate_username(username: str) -> bool:
    """Проверяет, что юзернейм допустим (мин. 4 символа, латиница, цифры, _)."""
    if not username:
        return False
    return bool(USERNAME_REGEX.match(username))

def compress(data: bytes) -> bytes:
    return zlib.compress(data, level=6)

def decompress(data: bytes) -> bytes:
    return zlib.decompress(data)

def encrypt_control(data: bytes, key: bytes) -> bytes:
    compressed = compress(data)
    return ChaCha20.encrypt(compressed, key)

def decrypt_control(enc: bytes, key: bytes) -> bytes:
    decrypted = ChaCha20.decrypt(enc, key)
    return decompress(decrypted)

def encrypt_stream(data: bytes, key: bytes) -> bytes:
    return ChaCha20.encrypt(data, key)

def decrypt_stream(enc: bytes, key: bytes) -> bytes:
    return ChaCha20.decrypt(enc, key)

def derive_stream_key(session_key: bytes, email: str, stream_id: int) -> bytes:
    salt = email.encode('utf-8')
    info = f"stream_{stream_id}".encode('utf-8')
    return HKDF.extract_and_expand(salt, session_key, info, 32)

def compute_file_checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def _sort_dict_keys(obj):
    """Рекурсивно сортирует ключи всех словарей в объекте."""
    if isinstance(obj, dict):
        return {k: _sort_dict_keys(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        return [_sort_dict_keys(item) for item in obj]
    else:
        return obj

def serialize_sorted(obj) -> bytes:
    """Сериализация с сортировкой ключей для детерминированной подписи."""
    sorted_obj = _sort_dict_keys(obj)
    return msgpack.packb(sorted_obj, use_bin_type=True)

def clean_metadata(file_path: str) -> None:
    try:
        from PIL import Image
    except ImportError:
        logger.warning("PIL (Pillow) не установлена, очистка метаданных пропущена для %s", file_path)
        return

    # Проверяем расширение – только для изображений
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in ('.jpg', '.jpeg', '.png', '.tiff', '.bmp', '.webp'):
        logger.debug("Очистка метаданных для %s не поддерживается", file_path)
        return

    try:
        with Image.open(file_path) as img:
            # Конвертируем в RGB, чтобы избежать проблем с альфа-каналом при сохранении в JPEG
            if img.mode in ('RGBA', 'LA', 'P'):
                img = img.convert('RGB')
            # Сохраняем без метаданных (по умолчанию PIL не сохраняет EXIF)
            # Для JPEG используем высокое качество
            if ext == '.png':
                img.save(file_path, format='PNG', optimize=True)
            else:
                img.save(file_path, format='JPEG', quality=95, optimize=True)
        logger.info("Метаданные удалены из %s", file_path)
    except Exception as e:
        logger.error("Не удалось очистить метаданные у %s: %s", file_path, e)

def serialize(obj) -> bytes:
    return msgpack.packb(obj, use_bin_type=True)

def deserialize(data: bytes):
    return msgpack.unpackb(data, raw=False)