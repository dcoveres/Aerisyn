import os
import tempfile
import logging
import asyncio
import shutil
import uuid
from typing import Optional, Dict

from protocol import MAX_CONCURRENT_UPLOADS
from .redis_store import RedisStore
from protocol import (
    MAX_FILE_SIZE,
    ENTITY_TYPE_CHANNEL,
    compute_file_checksum,
    clean_metadata,
    deserialize,
    serialize,
    MAX_CONCURRENT_UPLOADS,
    MAX_FILENAME_LENGTH,
    STORAGE_DIR,
)

logger = logging.getLogger(__name__)

class FileService:
    def __init__(self, db_store, redis_store: RedisStore, gateway):
        self.db_store = db_store          # экземпляр PostgresStore
        self.redis_store = redis_store
        self.gateway = gateway

        # Состояние потоков (временное)
        self.file_streams: Dict[int, dict] = {}
        self._timeout_tasks: Dict[int, asyncio.Task] = {}
        self._active_streams = set()
        self._stream_owners: Dict[int, str] = {}
        self.user_uploads: Dict[str, int] = {}
        self._lock = asyncio.Lock()

        os.makedirs(STORAGE_DIR, exist_ok=True)

    async def allocate_stream_id(self, email: str) -> int:
        async with self._lock:
            while True:
                sid = await self.redis_store.get_next_stream_id()
                if sid not in self._active_streams:
                    self._active_streams.add(sid)
                    self._stream_owners[sid] = email
                    return sid

    def release_stream_id(self, stream_id: int):
        self._active_streams.discard(stream_id)
        self._stream_owners.pop(stream_id, None)

    def get_stream_owner(self, stream_id: int) -> Optional[str]:
        return self._stream_owners.get(stream_id)

    async def _check_permissions(self, email: str, entity_id: int) -> bool:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            logger.error(f"Entity {entity_id} not found")
            return False
        members = await self.db_store.get_entity_members(entity_id)
        banned = await self.db_store.get_entity_banned(entity_id)
        if email not in members:
            logger.error(f"User {email} not a member of entity {entity_id}")
            return False
        if email in banned:
            logger.error(f"User {email} is banned in entity {entity_id}")
            return False
        if entity.type == ENTITY_TYPE_CHANNEL:
            admins = await self.db_store.get_entity_admins(entity_id)
            if email != entity.owner and email not in admins:
                logger.error(f"User {email} has no permission to send to channel {entity_id}")
                return False
        return True

    def _release_upload_slot(self, email: str):
        if email:
            current = self.user_uploads.get(email, 0)
            if current > 0:
                self.user_uploads[email] = current - 1
            else:
                self.user_uploads[email] = 0

    def _cleanup_stream_unlocked(self, stream_id: int):
        state = self.file_streams.pop(stream_id, None)
        if state:
            try:
                state["file"].close()
            except Exception:
                pass
            tmp_path = state.get("tmp_path")
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except Exception as e:
                    logger.error(f"Failed to remove temp file {tmp_path}: {e}")
            email = state.get("email")
            if email:
                self._release_upload_slot(email)
        task = self._timeout_tasks.pop(stream_id, None)
        if task:
            task.cancel()
        self.release_stream_id(stream_id)

    def _schedule_timeout(self, stream_id: int, timeout: int = 60):
        async def timeout_coro():
            await asyncio.sleep(timeout)
            async with self._lock:
                if stream_id in self.file_streams:
                    logger.warning(f"Stream {stream_id} timed out after {timeout} seconds")
                    self._cleanup_stream_unlocked(stream_id)
        task = asyncio.create_task(timeout_coro())
        self._timeout_tasks[stream_id] = task

    async def start_upload(self, stream_id: int, header_data: bytes, email: str) -> bool:
        async with self._lock:
            current_uploads = self.user_uploads.get(email, 0)
            if current_uploads >= MAX_CONCURRENT_UPLOADS:
                logger.warning(
                    f"User {email} exceeded concurrent upload limit "
                    f"({current_uploads} >= {MAX_CONCURRENT_UPLOADS})"
                )
                return False
            owner = self._stream_owners.get(stream_id)
            if owner != email:
                logger.error(f"Stream {stream_id} not owned by {email} (owner: {owner})")
                return False
            if stream_id in self.file_streams:
                logger.error(f"Stream {stream_id} already in use")
                return False

            try:
                header = deserialize(header_data)
            except Exception as e:
                logger.error(f"Failed to deserialize header for stream {stream_id}: {e}")
                return False

            if header.get("type") != "file_header":
                logger.error(f"Expected file_header, got {header.get('type')}")
                return False

            filename = header.get("filename")
            size = header.get("size")
            entity_id = header.get("entity_id")
            checksum = header.get("checksum")

            if not filename or size is None or entity_id is None or not checksum:
                logger.error("Incomplete header: missing filename, size, entity_id or checksum")
                return False

            # Убираем потенциально опасные символы (path separators, null bytes) из имени файла
            filename = filename.replace("\x00", "").replace("/", "_").replace("\\", "_").strip()
            if not filename:
                logger.error(f"Empty filename after sanitization from {email}")
                return False

            if len(filename) > MAX_FILENAME_LENGTH:
                logger.warning(f"Filename too long ({len(filename)} chars) from {email}, max {MAX_FILENAME_LENGTH}")
                return False

            if not isinstance(size, int) or size <= 0:
                logger.error(f"Invalid file size {size!r} from {email}")
                return False

            if size > MAX_FILE_SIZE:
                logger.error(f"File too large: {size} bytes (max {MAX_FILE_SIZE})")
                return False

            if not await self._check_permissions(email, entity_id):
                return False

            fd, tmp_path = tempfile.mkstemp(suffix=".upload")
            os.close(fd)

            self.user_uploads[email] = current_uploads + 1

            state = {
                "filename": filename,
                "size": size,
                "entity_id": entity_id,
                "checksum": checksum,
                "received": 0,
                "tmp_path": tmp_path,
                "file": open(tmp_path, "wb"),
                "email": email,
            }
            self.file_streams[stream_id] = state
            self._schedule_timeout(stream_id)
            logger.info(f"Started upload of {filename} ({size} bytes) for entity {entity_id}, stream {stream_id}")
            return True

    async def append_data(self, stream_id: int, data: bytes, email: str) -> bool:
        async with self._lock:
            state = self.file_streams.get(stream_id)
            if not state:
                logger.warning(f"Stream {stream_id} not found for append")
                return False
            if state["email"] != email:
                logger.error(f"User {email} trying to write to foreign stream {stream_id} (owner {state['email']})")
                return False

            task = self._timeout_tasks.pop(stream_id, None)
            if task:
                task.cancel()
            self._schedule_timeout(stream_id)

            await asyncio.to_thread(state["file"].write, data)
            state["received"] += len(data)

            if state["received"] > state["size"]:
                logger.error(f"Received more data than expected for stream {stream_id}: {state['received']} > {state['size']}")
                self._cleanup_stream_unlocked(stream_id)
                return False
            return True

    async def finish_upload(self, stream_id: int, success: bool, email: Optional[str] = None):
        async with self._lock:
            state = self.file_streams.get(stream_id)
            if not state:
                logger.warning(f"Stream {stream_id} not found on finish")
                return

            if email is not None and state["email"] != email:
                logger.error(f"User {email} trying to finish foreign stream {stream_id} (owner {state['email']})")
                return

            tmp_path = state["tmp_path"]
            filename = state["filename"]
            entity_id = state["entity_id"]
            checksum = state["checksum"]
            email_owner = state["email"]
            file_size = state["size"]
            received = state["received"]

            try:
                state["file"].close()
            except Exception as e:
                logger.error(f"Error closing file for stream {stream_id}: {e}")

            if not success or received != file_size:
                if not success:
                    logger.info(f"Upload {stream_id} aborted by user")
                else:
                    logger.error(f"Size mismatch for stream {stream_id}: received {received}, expected {file_size}")
                self._cleanup_stream_unlocked(stream_id)
                return

            # Читаем и проверяем файл
            try:
                file_data = await asyncio.to_thread(lambda: open(tmp_path, "rb").read())
            except Exception as e:
                logger.error(f"Failed to read temp file {filename}: {e}")
                self._cleanup_stream_unlocked(stream_id)
                return

            if compute_file_checksum(file_data) != checksum:
                logger.error(f"Checksum mismatch for stream {stream_id}")
                self._cleanup_stream_unlocked(stream_id)
                return

            # Основной блок сохранения и рассылки
            try:
                await asyncio.to_thread(clean_metadata, tmp_path)

                unique_name = f"{checksum}_{uuid.uuid4().hex[:8]}"
                entity_dir = os.path.join(STORAGE_DIR, str(entity_id))
                os.makedirs(entity_dir, exist_ok=True)
                dest_path = os.path.join(entity_dir, unique_name)
                await asyncio.to_thread(shutil.move, tmp_path, dest_path)

                # Удаляем состояние потока
                self.file_streams.pop(stream_id, None)
                task = self._timeout_tasks.pop(stream_id, None)
                if task:
                    task.cancel()
                self._release_upload_slot(email_owner)
                self.release_stream_id(stream_id)

                # Рассылка уведомлений (вне блокировки)
                msg = await self.gateway.message_service.add_file_message(
                    email_owner, entity_id, filename, file_size, checksum, dest_path
                )
                if msg:
                    notification = {
                        "type": "new_message",
                        "entity_id": entity_id,
                        "message_id": msg.id,
                        "from": email_owner,
                        "content": filename,
                        "timestamp": msg.timestamp,
                        "file": {
                            "filename": filename,
                            "size": file_size,
                            "checksum": checksum,
                        }
                    }
                    await self.gateway.send_control_to_entity(entity_id, notification, exclude_email=email_owner)

                await self.gateway.broadcast_file(entity_id, filename, file_data, email_owner)
                logger.info(f"File {filename} uploaded and distributed in entity {entity_id}")

            except Exception as e:
                logger.error(f"Ошибка при завершении загрузки stream {stream_id}: {e}", exc_info=True)
                # Очищаем ресурсы, если не были очищены
                if stream_id in self.file_streams:
                    self._cleanup_stream_unlocked(stream_id)
                # Удаляем временный файл, если он ещё существует
                if os.path.exists(tmp_path):
                    try:
                        os.unlink(tmp_path)
                    except Exception:
                        pass
            # Не выбрасываем исключение дальше, чтобы не обрывать соединение

    async def get_file_info(self, email: str, entity_id: int, message_id: int) -> Optional[dict]:
        entity = await self.db_store.get_entity(entity_id)
        if not entity:
            return None
        members = await self.db_store.get_entity_members(entity_id)
        if email not in members:
            return None
        banned = await self.db_store.get_entity_banned(entity_id)
        if email in banned:
            return None

        msg = await self.db_store.get_message(entity_id, message_id)
        if not msg or msg.type != "file" or not msg.file_path:
            return None
        if not os.path.isfile(msg.file_path):
            return None

        return {
            "filename": msg.filename,
            "size": msg.file_size,
            "checksum": msg.file_checksum,
            "file_path": msg.file_path,
        }