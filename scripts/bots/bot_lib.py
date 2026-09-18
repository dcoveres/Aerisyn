# bot_lib.py – future НИКОГДА не удаляется
import asyncio
import base64
import logging
import ssl
import time
from typing import Optional, Dict, Any, Callable, Awaitable

import websockets
from axiso import DH, HKDF
from protocol import (
    STREAM_ID_CONTROL,
    DEFAULT_WS_URL,
    BOT_AUTH_REQUEST,
    CMD_SEND_MESSAGE,
    CMD_GET_MESSAGES,
    CMD_GET_ENTITY_INFO,
    CMD_GET_USER_INFO,
    CMD_SEND_STICKER,
    CMD_GET_STICKERS,
    CMD_EDIT_MESSAGE,
    CMD_ANSWER_CALLBACK,
    NOTIFICATION_CALLBACK,
    NOTIFICATION_REACTION_UPDATE,
    serialize,
    deserialize,
    encrypt_control,
    decrypt_control,
    REFRESH_INTERVAL,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BotClient:
    def __init__(self, token: str, bot_id: int, url: str = DEFAULT_WS_URL):
        self.token = token
        self.bot_id = bot_id
        self.url = url
        self.websocket = None
        self.session_key = None
        self.server_public_key = None
        self.client_priv_key = None
        self.client_pub_b64 = None

        dh_keys = DH.generate_keypair()
        self.client_priv_key = base64.urlsafe_b64decode(dh_keys["private_key"])
        self.client_pub_b64 = dh_keys["public_key"]

        self.running = False
        self.receive_task = None
        self.refresh_task = None
        self._message_handler = None
        self._callback_handler = None
        self._reaction_handler = None
        self.request_counter = 0
        self.pending_requests = {}

    async def connect(self):
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        self.websocket = await websockets.connect(self.url, ssl=ssl_context, max_size=None)
        logger.info(f"Подключено к {self.url}")

        handshake = {"type": "handshake", "public_key": self.client_pub_b64}
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
        if not server_pub_b64:
            raise RuntimeError("Неполный handshake_response")
        self.server_public_key = base64.urlsafe_b64decode(server_pub_b64)
        logger.info("Handshake успешен")

    async def authenticate(self):
        if self.server_public_key is None:
            raise RuntimeError("Сначала вызовите connect()")

        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
        priv = X25519PrivateKey.from_private_bytes(self.client_priv_key)
        pub = X25519PublicKey.from_public_bytes(self.server_public_key)
        shared = priv.exchange(pub)

        salt = f"bot_{self.bot_id}".encode('utf-8')
        info = b"chat_session"
        self.session_key = HKDF.extract_and_expand(salt, shared, info, 32)

        auth_req = {"type": BOT_AUTH_REQUEST, "token": self.token}
        packet = STREAM_ID_CONTROL.to_bytes(4, "big") + serialize(auth_req)
        await self.websocket.send(packet)

        response = await self.websocket.recv()
        if len(response) < 4:
            raise RuntimeError("Некорректный ответ аутентификации")
        stream_id = int.from_bytes(response[:4], "big")
        data = response[4:]
        if stream_id != 0:
            raise RuntimeError("Ожидался управляющий поток")

        try:
            decrypted = decrypt_control(data, self.session_key)
            resp = deserialize(decrypted)
        except Exception as e:
            try:
                plain = deserialize(data)
                if plain.get("status") == "error":
                    raise RuntimeError(f"Ошибка сервера: {plain.get('reason', 'Unknown')}")
                raise RuntimeError(f"Неизвестный ответ: {plain}")
            except Exception:
                raise RuntimeError(f"Ошибка аутентификации: {e}")

        if resp.get("status") != "ok":
            raise RuntimeError(f"Ошибка аутентификации: {resp.get('reason', 'Unknown')}")

        self.bot_id = resp.get("data", {}).get("bot_id", self.bot_id)
        logger.info(f"Бот аутентифицирован, id={self.bot_id}")

        self.running = True
        self.receive_task = asyncio.create_task(self._receive_loop())
        self.refresh_task = asyncio.create_task(self._refresh_loop())

    async def _receive_loop(self):
        try:
            async for message in self.websocket:
                await self._process_message(message)
        except websockets.exceptions.ConnectionClosed:
            logger.info("Соединение закрыто")
        finally:
            self.running = False

    async def _process_message(self, message: bytes):
        if len(message) < 4:
            logger.warning("Слишком короткое сообщение")
            return
        stream_id = int.from_bytes(message[:4], "big")
        data = message[4:]

        if stream_id == STREAM_ID_CONTROL:
            if self.session_key is None:
                return
            try:
                decrypted = decrypt_control(data, self.session_key)
                obj = deserialize(decrypted)
            except Exception as e:
                logger.error(f"Ошибка обработки: {e}")
                return
                
            logger.info(f"📥 [BOT] Получен пакет: {obj}")
            if "request_id" in obj:
                req_id = obj["request_id"]
                future = self.pending_requests.get(req_id)
                if future and not future.done():
                    future.set_result(obj)
                    logger.debug(f"Установлен результат для request_id {req_id}")
                else:
                    logger.warning(f"Неизвестный или уже завершённый request_id: {req_id}, ожидались: {list(self.pending_requests.keys())}")
            else:
                msg_type = obj.get("type")
                # ВАЖНО: обработчики запускаются как отдельные task'и, а не await'ятся
                # прямо здесь. Раньше `await self._message_handler(obj)` блокировал
                # именно эту корутину (_receive_loop), а значит и чтение сокета целиком.
                # Если обработчик сам вызывает bot.send_message(...)/другую команду,
                # он ждёт future, которую может выставить только следующая итерация
                # _receive_loop — а она не наступит, пока не завершится обработчик.
                # Получался самозацикленный дедлок, который снимался только по
                # таймауту (120 сек) в send_command, хотя сервер отвечал мгновенно.
                if msg_type == "new_message" and self._message_handler:
                    asyncio.create_task(self._run_handler(self._message_handler, obj))
                elif msg_type == NOTIFICATION_CALLBACK and self._callback_handler:
                    asyncio.create_task(self._run_handler(self._callback_handler, obj))
                elif msg_type == NOTIFICATION_REACTION_UPDATE and self._reaction_handler:
                    asyncio.create_task(self._run_handler(self._reaction_handler, obj))
                else:
                    logger.debug(f"Получено уведомление: {obj}")
        else:
            logger.debug(f"Файловый поток {stream_id}, игнорируем")

    @staticmethod
    async def _run_handler(handler: Callable[[dict], Awaitable[None]], obj: dict):
        # Отдельная обёртка, чтобы исключение в пользовательском обработчике
        # не пропадало молча (задача без await результата иначе "проглотит" traceback).
        try:
            await handler(obj)
        except Exception:
            logger.exception("Необработанное исключение в обработчике события")

    async def _refresh_loop(self):
        while self.running:
            await asyncio.sleep(REFRESH_INTERVAL)

    # ---------- Базовые команды ----------
    async def send_command(self, command: str, params: dict) -> dict:
        if not self.running:
            raise RuntimeError("Бот не запущен")

        self.request_counter += 1
        req_id = self.request_counter
        future = asyncio.get_running_loop().create_future()
        self.pending_requests[req_id] = future
        logger.info(f"📤 Отправка {command} request_id={req_id}")

        obj = {"command": command, "request_id": req_id, **params}
        payload = serialize(obj)
        encrypted = encrypt_control(payload, self.session_key)
        packet = STREAM_ID_CONTROL.to_bytes(4, "big") + encrypted
        await self.websocket.send(packet)
        
        logger.info(f"📤 [BOT] Команда {command} отправлена, ждём ответ для request_id={req_id}")

        try:
            resp = await asyncio.wait_for(future, timeout=120.0)
            logger.info(f"📥 Получен ответ для {command} request_id={req_id}")
        except asyncio.TimeoutError:
            logger.error(f"⏰ Таймаут для {command} request_id={req_id}")
            raise TimeoutError("Таймаут")

        # future НЕ УДАЛЯЕМ из словаря – оставляем навсегда
        # self.pending_requests.pop(req_id, None)  # <-- НЕ УДАЛЯЕМ

        if resp.get("status") == "error":
            raise RuntimeError(resp.get("reason", "Неизвестная ошибка"))
        return resp.get("data", {})

    # ---------- Все остальные методы без изменений ----------
    async def send_message(self, entity_id: int, content: str,
                           reply_to: Optional[dict] = None,
                           reply_markup: Optional[dict] = None) -> int:
        params = {"entity_id": entity_id, "content": content}
        if reply_to:
            params["reply_to"] = reply_to
        if reply_markup:
            params["reply_markup"] = reply_markup
        result = await self.send_command(CMD_SEND_MESSAGE, params)
        return result["message_id"]

    async def send_sticker(self, entity_id: int, sticker_id: int) -> int:
        result = await self.send_command(CMD_SEND_STICKER, {
            "entity_id": entity_id,
            "sticker_id": sticker_id
        })
        return result["message_id"]

    async def edit_message(self, entity_id: int, message_id: int,
                           text: str, reply_markup: Optional[dict] = None) -> dict:
        params = {
            "entity_id": entity_id,
            "message_id": message_id,
            "text": text,
        }
        if reply_markup is not None:
            params["reply_markup"] = reply_markup
        return await self.send_command(CMD_EDIT_MESSAGE, params)

    async def answer_callback_query(self, callback_query_id: str,
                                    text: Optional[str] = None,
                                    show_alert: bool = False) -> None:
        await self.send_command(CMD_ANSWER_CALLBACK, {
            "message_id": callback_query_id,
            "text": text or "",
            "show_alert": show_alert,
        })

    async def get_stickers(self, query: Optional[str] = None,
                           limit: int = 50, offset: int = 0) -> list:
        result = await self.send_command(CMD_GET_STICKERS, {
            "query": query or "",
            "limit": limit,
            "offset": offset
        })
        return result.get("stickers", [])

    async def get_messages(self, entity_id: int, limit: int = 50) -> list:
        result = await self.send_command(CMD_GET_MESSAGES, {"entity_id": entity_id, "limit": limit})
        return result.get("messages", [])

    async def get_entity_info(self, entity_id: int) -> dict:
        return await self.send_command(CMD_GET_ENTITY_INFO, {"entity_id": entity_id})

    async def get_user_info(self, target: str) -> dict:
        return await self.send_command(CMD_GET_USER_INFO, {"target": target})

    def on_message(self, handler: Callable[[dict], Awaitable[None]]):
        self._message_handler = handler

    def on_callback_query(self, handler: Callable[[dict], Awaitable[None]]):
        self._callback_handler = handler

    def on_reaction(self, handler: Callable[[dict], Awaitable[None]]):
        self._reaction_handler = handler

    async def run(self):
        try:
            await self.connect()
            await self.authenticate()
            logger.info("Бот запущен")
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Остановка")
        finally:
            await self.close()

    async def close(self):
        self.running = False
        if self.refresh_task:
            self.refresh_task.cancel()
        if self.receive_task:
            self.receive_task.cancel()
        if self.websocket:
            await self.websocket.close()
