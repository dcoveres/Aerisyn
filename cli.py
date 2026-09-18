import asyncio
import websockets
import json
import base64
import os
import tempfile
import shutil
import logging
import secrets
import time
from typing import Optional, Dict, Any
from axiso import DH, EdDSA, ChaCha20
import ssl
from protocol import (
    STREAM_ID_CONTROL,
    encrypt_control,
    decrypt_control,
    encrypt_stream,
    decrypt_stream,
    derive_stream_key,
    compute_file_checksum,
    clean_metadata,
    serialize,
    deserialize,
    serialize_sorted,
    DEFAULT_WS_URL,
    CMD_DOWNLOAD_FILE,
    CMD_SET_USERNAME,
    CMD_SET_BIO,
    CMD_SET_FIRST_NAME,
    CMD_REQUEST_UPLOAD_STREAM,
    CMD_SET_SHOW_EMAIL,
    REQUEST_CODE_TYPE,
    CMD_SET_ENTITY_USERNAME,
    CMD_REFRESH_SESSION,
    REFRESH_INTERVAL,

)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChatClient:
    def __init__(self, url: str = DEFAULT_WS_URL):
        self.url = url
        self.websocket = None
        self.email: Optional[str] = None
        self.session_key: Optional[bytes] = None
        self.server_public_key: Optional[bytes] = None
        self.client_priv_key: Optional[bytes] = None
        self.client_pub_key: Optional[bytes] = None
        self.server_fingerprint: Optional[str] = None
        self.saved_fingerprint: Optional[str] = None
        self.request_counter = 0
        self.pending_requests: Dict[int, asyncio.Future] = {}

        # Генерация ключей для DH (X25519) – одноразовые для сессии
        dh_keys = DH.generate_keypair()
        self.client_priv_key = base64.urlsafe_b64decode(dh_keys["private_key"])
        self.client_pub_key = base64.urlsafe_b64decode(dh_keys["public_key"])
        self.client_pub_b64 = dh_keys["public_key"]

        # Ключи EdDSA для подписи – загружаем или создаём
        self.ed_private_key = None
        self.ed_public_key_b64 = None
        self._load_or_generate_ed_keys()

        self.receive_task: Optional[asyncio.Task] = None
        self.running = False
        self.refresh_task: Optional[asyncio.Task] = None  # задача фонового обновления

        self.download_futures: Dict[str, asyncio.Future] = {}
        self.download_results: Dict[str, tuple] = {}
        self.file_streams: Dict[int, dict] = {}

    def _load_or_generate_ed_keys(self):
        """Временные ключи, позже они будут перезаписаны для конкретного email."""
        ed_keys = EdDSA.generate_keypair()
        self.ed_private_key = base64.urlsafe_b64decode(ed_keys["private_key"])
        self.ed_public_key_b64 = ed_keys["public_key"]

    def _save_ed_keys(self, email: str):
        filename = f"eddsa_keys_{email}.json"
        data = {
            "private_key": base64.urlsafe_b64encode(self.ed_private_key).decode(
                "utf-8"
            ),
            "public_key": self.ed_public_key_b64,
        }
        with open(filename, "w") as f:
            json.dump(data, f)
        logger.info(f"EdDSA ключи сохранены в {filename}")

    def _load_ed_keys(self, email: str) -> bool:
        filename = f"eddsa_keys_{email}.json"
        if not os.path.isfile(filename):
            return False
        try:
            with open(filename, "r") as f:
                data = json.load(f)
            self.ed_private_key = base64.urlsafe_b64decode(data["private_key"])
            self.ed_public_key_b64 = data["public_key"]
            logger.info(f"EdDSA ключи загружены из {filename}")
            return True
        except Exception as e:
            logger.warning(f"Не удалось загрузить ключи из {filename}: {e}")
            return False

    async def connect(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self.websocket = await websockets.connect(self.url, ssl=ssl_context, max_size=None)
        logger.info(f"Подключено к {self.url}")

        handshake = {
            "type": "handshake",
            "public_key": self.client_pub_b64,
        }
        await self.websocket.send(b"\x00\x00\x00\x00" + serialize(handshake))

        response = await self.websocket.recv()
        if len(response) < 4:
            raise RuntimeError("Некорректный ответ handshake")
        stream_id = int.from_bytes(response[:4], "big")
        data = response[4:]
        if stream_id != 0:
            raise RuntimeError("Ожидался управляющий поток")
        try:
            resp = deserialize(data)
        except Exception:
            raise RuntimeError("Некорректный JSON в handshake_response")
        if resp.get("type") != "handshake_response":
            raise RuntimeError("Неожиданный тип ответа")

        server_pub_b64 = resp.get("public_key")
        fingerprint = resp.get("fingerprint")
        if not server_pub_b64 or not fingerprint:
            raise RuntimeError("Неполный handshake_response")

        if self.saved_fingerprint is None:
            self.saved_fingerprint = fingerprint
            logger.info(f"Сохранён fingerprint сервера: {fingerprint}")
        else:
            if self.saved_fingerprint != fingerprint:
                raise RuntimeError(
                    f"Fingerprint не совпадает! Ожидалось {self.saved_fingerprint}, получено {fingerprint}"
                )

        self.server_fingerprint = fingerprint
        self.server_public_key = base64.urlsafe_b64decode(server_pub_b64)
        logger.info("Handshake успешен")

    async def request_code(self, email: str):
        """Запрашивает у сервера отправку кода подтверждения на указанный email."""
        req = {"type": REQUEST_CODE_TYPE, "email": email}
        packet = STREAM_ID_CONTROL.to_bytes(4, "big") + serialize(req)
        await self.websocket.send(packet)

        response = await self.websocket.recv()
        if len(response) < 4:
            raise RuntimeError("Некорректный ответ на запрос кода")
        stream_id = int.from_bytes(response[:4], "big")
        data = response[4:]
        if stream_id != 0:
            raise RuntimeError("Ожидался управляющий поток")

        try:
            resp = deserialize(data)
        except Exception as e:
            raise RuntimeError(f"Ошибка парсинга ответа: {e}")

        if resp.get("status") == "error":
            raise RuntimeError(
                f"Ошибка запроса кода: {resp.get('reason', 'Unknown')}"
            )
        logger.info("Код запрошен, ожидайте письмо")

    async def authenticate(self, email: str, sms_code: str):
        """Выполняет аутентификацию с введённым кодом."""
        self.email = email

        if not self._load_ed_keys(email):
            ed_keys = EdDSA.generate_keypair()
            self.ed_private_key = base64.urlsafe_b64decode(ed_keys["private_key"])
            self.ed_public_key_b64 = ed_keys["public_key"]
            self._save_ed_keys(email)

        from cryptography.hazmat.primitives.asymmetric.x25519 import (
            X25519PrivateKey,
            X25519PublicKey,
        )

        priv = X25519PrivateKey.from_private_bytes(self.client_priv_key)
        pub = X25519PublicKey.from_public_bytes(self.server_public_key)
        shared = priv.exchange(pub)

        from axiso import HKDF

        salt = email.encode("utf-8")
        info = b"chat_session"
        self.session_key = HKDF.extract_and_expand(salt, shared, info, 32)

        auth_req = {
            "type": "auth_request",
            "email": email,
            "sms_code": sms_code,
            "eddsa_public_key": self.ed_public_key_b64,
        }
        packet = STREAM_ID_CONTROL.to_bytes(4, "big") + serialize(auth_req)
        await self.websocket.send(packet)

        response = await self.websocket.recv()
        if len(response) < 4:
            raise RuntimeError("Некорректный ответ аутентификации")
        stream_id = int.from_bytes(response[:4], "big")
        data = response[4:]
        if stream_id != 0:
            raise RuntimeError("Ожидался управляющий поток")

        resp = None
        try:
            decrypted = decrypt_control(data, self.session_key)
            resp = deserialize(decrypted)
        except Exception as e:
            try:
                plain = deserialize(data)
                if plain.get("status") == "error":
                    raise RuntimeError(
                        f"Ошибка сервера: {plain.get('reason', 'Unknown')}"
                    )
                else:
                    resp = plain
            except Exception as e2:
                raise RuntimeError(f"Ошибка аутентификации: {e}")

        if resp is None:
            raise RuntimeError("Не удалось получить ответ от сервера")

        if resp.get("status") != "ok":
            raise RuntimeError(
                f"Ошибка аутентификации: {resp.get('reason', 'Unknown')}"
            )

        resp_data = resp.get("data", {})
        self.user_public_key = resp_data.get("public_key")
        logger.info(f"Аутентификация успешна для {email}")

        self.running = True
        self.receive_task = asyncio.create_task(self.receive_loop())

        if resp_data.get("requires_first_name"):
            print("Добро пожаловать! Пожалуйста, установите ваше имя.")
            while True:
                name = input("Введите ваше имя: ").strip()
                if name:
                    break
                print("Имя не может быть пустым.")
            await self.send_command("set_first_name", {"name": name})
            print("Имя установлено.")

        # Запускаем фоновое обновление сессии
        self.refresh_task = asyncio.create_task(self._refresh_loop())

    async def receive_loop(self):
        try:
            async for message in self.websocket:
                await self.process_message(message)
        except websockets.exceptions.ConnectionClosed:
            logger.info("Соединение закрыто")
        finally:
            self.running = False

    async def process_message(self, message: bytes):
        if len(message) < 4:
            logger.warning("Слишком короткое сообщение")
            return
        stream_id = int.from_bytes(message[:4], "big")
        data = message[4:]

        if stream_id == STREAM_ID_CONTROL:
            if self.session_key is None:
                logger.warning(
                    "Получено управляющее сообщение без сессионного ключа"
                )
                return
            try:
                decrypted = decrypt_control(data, self.session_key)
                obj = deserialize(decrypted)
            except Exception as e:
                logger.error(f"Ошибка обработки управляющего сообщения: {e}")
                return

            if "request_id" in obj:
                req_id = obj["request_id"]
                future = self.pending_requests.pop(req_id, None)
                if future:
                    future.set_result(obj)
                else:
                    logger.warning(f"Неизвестный request_id: {req_id}")
            else:
                msg_type = obj.get("type")
                if msg_type == "new_message":
                    self.handle_new_message(obj)
                elif msg_type == "file_stream":
                    self.handle_file_stream(obj)
                elif msg_type == "edit_message":
                    self.handle_edit_message(obj)
                else:
                    logger.info(f"Получено уведомление: {obj}")
        else:
            await self.handle_file_data(stream_id, data)

    def handle_new_message(self, obj: dict):
        entity_id = obj.get("entity_id")
        msg_id = obj.get("message_id")
        from_email = obj.get("from")
        content = obj.get("content")
        timestamp = obj.get("timestamp")
        print(
            f"\n[Новое сообщение в {entity_id} от {from_email}]: {content}"
        )

    def handle_edit_message(self, obj: dict):
        entity_id = obj.get("entity_id")
        msg_id = obj.get("message_id")
        content = obj.get("content")
        print(
            f"\n[Сообщение {msg_id} в {entity_id} отредактировано]: {content}"
        )

    async def handle_file_data(self, stream_id: int, data: bytes):
        if not data:
            state = self.file_streams.pop(stream_id, None)
            if state:
                state["file"].close()
                if state["received"] == state["size"]:
                    with open(state["tmp_path"], "rb") as f:
                        file_content = f.read()
                    if compute_file_checksum(file_content) == state["checksum"]:
                        clean_metadata(state["tmp_path"])
                        key = state.get("download_key")
                        if key:
                            future = self.download_futures.pop(key, None)
                            if future:
                                self.download_results[key] = (
                                    state["tmp_path"],
                                    state["filename"],
                                )
                                future.set_result(True)
                        else:
                            print(
                                f"\nФайл {state['filename']} получен и сохранён: {state['tmp_path']}"
                            )
                    else:
                        print(
                            f"Ошибка: контрольная сумма не совпадает для {state['filename']}"
                        )
                        os.unlink(state["tmp_path"])
                else:
                    print(
                        f"Ошибка: размер не совпадает для {state['filename']}"
                    )
                    os.unlink(state["tmp_path"])
            return

        if stream_id not in self.file_streams:
            try:
                stream_key = derive_stream_key(
                    self.session_key, self.email, stream_id
                )
                header_data = decrypt_stream(data, stream_key)
                header = deserialize(header_data)
            except Exception as e:
                logger.error(
                    f"Ошибка при получении заголовка файлового потока {stream_id}: {e}"
                )
                return

            if header.get("type") != "file_header":
                logger.error(
                    f"Ожидался file_header, получено {header.get('type')}"
                )
                return

            filename = header.get("filename")
            size = header.get("size")
            entity_id = header.get("entity_id")
            checksum = header.get("checksum")
            message_id = header.get("message_id")
            if not filename or size is None or entity_id is None or not checksum:
                logger.error("Неполный заголовок")
                return

            fd, tmp_path = tempfile.mkstemp(suffix=".download")
            os.close(fd)
            state = {
                "filename": filename,
                "size": size,
                "entity_id": entity_id,
                "checksum": checksum,
                "received": 0,
                "tmp_path": tmp_path,
                "file": open(tmp_path, "wb"),
            }
            if message_id:
                key = f"{entity_id}_{message_id}"
                if key in self.download_futures:
                    state["download_key"] = key
            self.file_streams[stream_id] = state
            logger.info(
                f"Начинаем получение файла {filename} (stream {stream_id})"
            )
        else:
            state = self.file_streams[stream_id]
            try:
                stream_key = derive_stream_key(
                    self.session_key, self.email, stream_id
                )
                chunk = decrypt_stream(data, stream_key)
            except Exception as e:
                logger.error(f"Ошибка расшифровки чанка: {e}")
                return

            state["file"].write(chunk)
            state["received"] += len(chunk)

    async def send_command(self, command: str, params: dict) -> dict:
        if not self.running:
            raise RuntimeError("Клиент не запущен")

        self.request_counter += 1
        req_id = self.request_counter
        future = asyncio.get_running_loop().create_future()
        self.pending_requests[req_id] = future

        obj = {
            "command": command,
            "request_id": req_id,
            **params,
        }

        payload_to_sign = serialize_sorted(obj)
        signature = EdDSA.sign(
            payload_to_sign,
            base64.urlsafe_b64encode(self.ed_private_key).decode("utf-8"),
        )
        obj["signature"] = base64.urlsafe_b64encode(signature).decode("utf-8")

        payload_enc = serialize(obj)
        encrypted = encrypt_control(payload_enc, self.session_key)
        packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
        await self.websocket.send(packet)

        try:
            resp = await asyncio.wait_for(future, timeout=30.0)
        except asyncio.TimeoutError:
            self.pending_requests.pop(req_id, None)
            raise TimeoutError("Превышен таймаут ожидания ответа")

        if resp.get("status") == "error":
            raise RuntimeError(resp.get("reason", "Неизвестная ошибка"))
        return resp.get("data", {})

    async def refresh_session(self):
        """Отправляет запрос на обновление сессии."""
        try:
            await self.send_command(CMD_REFRESH_SESSION, {"timestamp": int(time.time())})
            logger.info("Сессия успешно обновлена")
        except Exception as e:
            logger.error(f"Ошибка обновления сессии: {e}")

    async def _refresh_loop(self):
        """Фоновый цикл, отправляющий refresh каждые REFRESH_INTERVAL секунд."""
        while self.running:
            await asyncio.sleep(REFRESH_INTERVAL)
            await self.refresh_session()

    async def request_stream_id(self) -> int:
        result = await self.send_command(CMD_REQUEST_UPLOAD_STREAM, {})
        return result["stream_id"]

    async def download_file(
        self,
        entity_id: int,
        message_id: int,
        save_path: Optional[str] = None,
    ):
        key = f"{entity_id}_{message_id}"
        if key in self.download_futures:
            print("Уже идёт загрузка этого файла")
            return

        future = asyncio.get_running_loop().create_future()
        self.download_futures[key] = future

        try:
            result = await self.send_command(
                "download_file",
                {
                    "entity_id": entity_id,
                    "message_id": message_id,
                },
            )
            filename = result.get("filename", "file")
            expected_size = result.get("size")
            expected_checksum = result.get("checksum")

            print(
                f"Начинается загрузка файла {filename} ({expected_size} байт) ..."
            )

            await asyncio.wait_for(future, timeout=60.0)

            tmp_path, saved_filename = self.download_results.pop(key, (None, None))
            if tmp_path is None:
                print("Ошибка: файл не был получен")
                return

            if save_path is None:
                save_path = saved_filename or filename
            if os.path.isdir(save_path):
                save_path = os.path.join(save_path, saved_filename or filename)

            shutil.move(tmp_path, save_path)
            print(f"Файл сохранён: {save_path}")

        except asyncio.TimeoutError:
            print("Превышено время ожидания файла")
            self.download_futures.pop(key, None)
        except Exception as e:
            print(f"Ошибка загрузки: {e}")
            self.download_futures.pop(key, None)
            future.cancel()

    async def send_file(self, entity_id: int, file_path: str, filename: Optional[str] = None):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        with open(file_path, "rb") as f:
            file_data = f.read()
        
        if filename is None: 
            filename = os.path.basename(file_path)

        # filename = os.path.basename(file_path)
        size = len(file_data)
        checksum = compute_file_checksum(file_data)

        stream_id = await self.request_stream_id()
        stream_key = derive_stream_key(self.session_key, self.email, stream_id)

        header = {
            "type": "file_header",
            "filename": filename,
            "size": size,
            "entity_id": entity_id,
            "checksum": checksum,
        }
        header_data = serialize(header)
        encrypted_header = encrypt_stream(header_data, stream_key)
        packet = stream_id.to_bytes(4, "big") + encrypted_header
        await self.websocket.send(packet)

        encrypted_data = encrypt_stream(file_data, stream_key)
        packet = stream_id.to_bytes(4, "big") + encrypted_data
        await self.websocket.send(packet)

        packet = stream_id.to_bytes(4, "big")
        await self.websocket.send(packet)

        logger.info(f"Файл {filename} отправлен (stream_id={stream_id})")

    async def interactive_loop(self):
        print("Мессенджер запущен. Введите команду:")
        print("  /create <type> <name> [target]   (target = @username или Почта)")
        print("  /join <entity_id>")
        print("  /leave <entity_id>")
        print("  /delete <entity_id>")
        print("  /send <entity_id> <text>")
        print("  /sendfile <entity_id> <filepath>")
        print("  /get <entity_id> [limit]")
        print("  /delete_msg <entity_id> <msg_id>")
        print("  /promote <entity_id> <email>")
        print("  /demote <entity_id> <email>")
        print("  /userinfo <target>   (target = @username или Почта)")
        print("  /download <entity_id> <message_id> [save_path]")
        print("  /setusername <username>   (пусто для сброса)")
        print("  /setname <имя>")
        print("  /setbio <text>")
        print("  /setshowemail <true|false>")
        print("  /profile [@username]   (без аргумента – свой профиль)")
        print("  /setentityusername <entity_id> <username>  (пусто для сброса)")
        print("  /create_bot <name> [@username]")
        print("  /delete_bot <bot_id>")
        print("  /list_bots ")
        print("  /bot_token <bot_id>")
        print("  /invite_bot <entity_id> <bot_identifier> (bot_id или @username)")
        print("  /quit")

        while self.running:
            try:
                line = await asyncio.get_running_loop().run_in_executor(
                    None, input, "> "
                )
            except (EOFError, KeyboardInterrupt):
                break

            if not line:
                continue
            line = line.strip()
            if line == "/quit":
                break

            parts = line.split()
            if not parts:
                continue

            cmd = parts[0]
            try:
                if cmd == "/create":
                    if len(parts) < 3:
                        print("Использование: /create <type> <name> [target]")
                        continue
                    type_ = parts[1]
                    name = parts[2]
                    target = parts[3] if len(parts) > 3 else None
                    if type_ == "chat" and not target:
                        print("Для чата нужно указать target")
                        continue
                    params = {"type": type_, "name": name}
                    if target:
                        params["target"] = target
                    result = await self.send_command("create_entity", params)
                    print(f"Сущность создана: id={result['entity_id']}")

                elif cmd == "/join":
                    if len(parts) < 2:
                        print("Использование: /join <entity_id>")
                        continue
                    entity_id = int(parts[1])
                    result = await self.send_command(
                        "join_entity", {"entity_id": entity_id}
                    )
                    print(f"Присоединение успешно")

                elif cmd == "/leave":
                    if len(parts) < 2:
                        print("Использование: /leave <entity_id>")
                        continue
                    entity_id = int(parts[1])
                    await self.send_command(
                        "leave_entity", {"entity_id": entity_id}
                    )
                    print("Вы вышли из сущности")

                elif cmd == "/delete":
                    if len(parts) < 2:
                        print("Использование: /delete <entity_id>")
                        continue
                    entity_id = int(parts[1])
                    await self.send_command(
                        "delete_entity", {"entity_id": entity_id}
                    )
                    print("Сущность удалена")

                elif cmd == "/send":
                    if len(parts) < 3:
                        print("Использование: /send <entity_id> <text>")
                        continue
                    entity_id = int(parts[1])
                    content = " ".join(parts[2:])
                    result = await self.send_command(
                        "send_message",
                        {"entity_id": entity_id, "content": content},
                    )
                    print(f"Сообщение отправлено (id={result['message_id']})")

                elif cmd == "/sendfile":
                    if len(parts) < 3:
                        print("Использование: /sendfile <entity_id> <filepath>")
                        continue
                    entity_id = int(parts[1])
                    filepath = parts[2]
                    await self.send_file(entity_id, filepath)
                    print(f"Файл отправлен")

                elif cmd == "/get":
                    if len(parts) < 2:
                        print("Использование: /get <entity_id> [limit]")
                        continue
                    entity_id = int(parts[1])
                    limit = int(parts[2]) if len(parts) > 2 else 50
                    result = await self.send_command(
                        "get_messages", {"entity_id": entity_id, "limit": limit}
                    )
                    msgs = result.get("messages", [])
                    if not msgs:
                        print("Нет сообщений")
                    else:
                        for m in msgs:
                            if m.get("type") == "file":
                                file_info = m.get("file", {})
                                print(
                                    f"[{m['id']}] {m['from']} отправил файл: {m['content']} ({file_info.get('size', 0)} байт)"
                                )
                            else:
                                print(
                                    f"[{m['id']}] {m['from']}: {m['content']}"
                                )

                elif cmd == "/delete_msg":
                    if len(parts) < 3:
                        print("Использование: /delete_msg <entity_id> <msg_id>")
                        continue
                    entity_id = int(parts[1])
                    msg_id = int(parts[2])
                    await self.send_command(
                        "delete_message",
                        {"entity_id": entity_id, "message_id": msg_id},
                    )
                    print("Сообщение удалено")

                elif cmd == "/promote":
                    if len(parts) < 3:
                        print("Использование: /promote <entity_id> <email>")
                        continue
                    entity_id = int(parts[1])
                    email = parts[2]
                    await self.send_command(
                        "promote_admin", {"entity_id": entity_id, "email": email}
                    )
                    print("Администратор назначен")

                elif cmd == "/demote":
                    if len(parts) < 3:
                        print("Использование: /demote <entity_id> <email>")
                        continue
                    entity_id = int(parts[1])
                    email = parts[2]
                    await self.send_command(
                        "demote_admin", {"entity_id": entity_id, "email": email}
                    )
                    print("Администратор снят")

                elif cmd == "/userinfo":
                    if len(parts) < 2:
                        print(
                            "Использование: /userinfo <target>  (target = @username или Почта)"
                        )
                        continue
                    target = parts[1]
                    result = await self.send_command(
                        "get_user_info", {"target": target}
                    )
                    print(f"Информация о {target}:")
                    print(
                        f"  Юзернейм: {result.get('username', 'не установлен')}"
                    )
                    print(f"  Имя: {result.get('first_name', '')}")
                    print(f"  Био: {result.get('bio', '')}")
                    print(f"  Публичный ключ: {result['public_key']}")
                    print(
                        f"  Теги: {', '.join(result['tags']) if result['tags'] else 'нет'}"
                    )
                    print(
                        f"  Сущности: {', '.join(map(str, result['entities']))}"
                    )
                    if "email" in result and result["email"]:
                        print(f"  Почта: {result['email']}")
                    else:
                        print("  Почта скрыт")

                elif cmd == "/download":
                    if len(parts) < 3:
                        print(
                            "Использование: /download <entity_id> <message_id> [save_path]"
                        )
                        continue
                    entity_id = int(parts[1])
                    message_id = int(parts[2])
                    save_path = parts[3] if len(parts) > 3 else None
                    await self.download_file(entity_id, message_id, save_path)

                elif cmd == "/setusername":
                    username = parts[1] if len(parts) > 1 else ""
                    await self.send_command(
                        "set_username", {"username": username}
                    )
                    print("Юзернейм обновлён")

                elif cmd == "/setname":
                    name = " ".join(parts[1:]) if len(parts) > 1 else ""
                    await self.send_command(
                        "set_first_name", {"name": name}
                    )
                    print("Имя обновлено")

                elif cmd == "/setbio":
                    bio = " ".join(parts[1:]) if len(parts) > 1 else ""
                    await self.send_command("set_bio", {"bio": bio})
                    print("Био обновлено")

                elif cmd == "/setshowemail":
                    if len(parts) < 2:
                        print("Использование: /setshowemail <true|false>")
                        continue
                    show = parts[1].lower() == "true"
                    await self.send_command(
                        "set_show_email", {"show": show}
                    )
                    print("Настройка видимости Почтаа обновлена")

                elif cmd == "/profile":
                    target = parts[1] if len(parts) > 1 else None
                    if not target:
                        target = self.email
                    result = await self.send_command(
                        "get_user_info", {"target": target}
                    )
                    print(
                        "Ваш профиль:"
                        if target == self.email
                        else f"Профиль {target}:"
                    )
                    print(
                        f"  Юзернейм: {result.get('username', 'не установлен')}"
                    )
                    print(f"  Имя: {result.get('first_name', '')}")
                    print(f"  Био: {result.get('bio', '')}")
                    if "email" in result and result["email"]:
                        print(f"  Почта: {result['email']}")
                    else:
                        print("  Почта скрыт")
                    print(f"  Публичный ключ: {result['public_key']}")
                
                elif cmd == "/setentityusername":
                    if len(parts) < 3:
                        print("Использование: /setentityusername <entity_id> <username>  (пусто для сброса)")
                        continue
                    entity_id = int(parts[1])
                    username = parts[2] if len(parts) > 2 else ""
                    await self.send_command(
                        CMD_SET_ENTITY_USERNAME,
                        {"entity_id": entity_id, "username": username}
                    )
                    print("Юзернейм сущности обновлён")
                
                 # Создание бота
                elif cmd == "/create_bot":
                    if len(parts) < 2:
                        print("Использование: /create_bot <name> [@username]")
                        continue
                    name = parts[1]
                    username = parts[2] if len(parts) > 2 else None
                    if username and not username.startswith('@'):
                        print("Юзернейм должен начинаться с @")
                        continue
                    username = username[1:] if username else None
                    result = await self.send_command("create_bot", {"name": name, "username": username})
                    print(f"Бот создан: id={result['bot_id']}, username={result.get('username')}, token={result['token']}")

                # Удаление бота
                elif cmd == "/delete_bot":
                    if len(parts) < 2:
                        print("Использование: /delete_bot <bot_id>")
                        continue
                    bot_id = int(parts[1])
                    await self.send_command("delete_bot", {"bot_id": bot_id})
                    print("Бот удалён")

                # Список ботов
                elif cmd == "/list_bots":
                    result = await self.send_command("list_bots", {})
                    bots = result.get("bots", [])
                    if not bots:
                        print("Нет ботов")
                    else:
                        print("Ваши боты:")
                    for b in bots:
                        print(f"  {b['id']}: {b['name']} (@{b['username']}) активен: {b['is_active']}")

                # Получение токена бота
                elif cmd == "/bot_token":
                    if len(parts) < 2:
                        print("Использование: /bot_token <bot_id>")
                        continue
                    bot_id = int(parts[1])
                    result = await self.send_command("get_bot_token", {"bot_id": bot_id})
                    print(f"Токен бота: {result['token']}")

                # Приглашение бота в сущность
                elif cmd == "/invite_bot":
                    if len(parts) < 3:
                        print("Использование: /invite_bot <entity_id> <bot_identifier> (bot_id или @username)")
                        continue
                    entity_id = int(parts[1])
                    bot_ident = parts[2]
                    await self.send_command("invite_bot", {"entity_id": entity_id, "bot": bot_ident})
                    print("Бот приглашён в сущность")
                else:
                    print(f"Неизвестная команда: {cmd}")

            except Exception as e:
                print(f"Ошибка: {e}")

    async def run(self, email: str, _sms_code: str = ""):
        try:
            await self.connect()
            print("Запрос кода подтверждения...")
            await self.request_code(email)
            print("Код отправлен на вашу почту.")
            code = input("Введите код из письма: ").strip()
            await self.authenticate(email, code)
            await self.interactive_loop()
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
        finally:
            if self.refresh_task:
                self.refresh_task.cancel()
                try:
                    await self.refresh_task
                except asyncio.CancelledError:
                    pass
            if self.receive_task:
                self.receive_task.cancel()
            if self.websocket:
                await self.websocket.close()
            self.running = False


async def main():
    client = ChatClient()
    email = input("Введите email: ")
    await client.run(email)


if __name__ == "__main__":
    asyncio.run(main())
