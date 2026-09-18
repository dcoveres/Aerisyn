# main.py – ПОЛНЫЙ ВЕБ-КЛИЕНТ Aerisyn С ПОДДЕРЖКОЙ ИНЛАЙН-КНОПОК И ОПРОСОВ
import asyncio
import logging
import secrets
import traceback
import os
import tempfile
import json
import time
import base64
from quart import Quart, render_template_string, request, session, jsonify, Response
from cli import ChatClient
from protocol import (
    DEFAULT_WS_URL,
    STREAM_ID_CONTROL,
    REFRESH_SESSION_TYPE,
    serialize,
    deserialize,
    serialize_sorted,
    HKDF,
    decrypt_control,
    encrypt_control,
    CMD_ADD_REACTION,
    CMD_REMOVE_REACTION,
    NOTIFICATION_REACTION_UPDATE,
    CMD_GET_STICKERS,
    CMD_SEND_STICKER,
    CMD_GET_STICKER_FILE,
    MESSAGE_TYPE_STICKER,
    CMD_SET_ENTITY_PRIVATE,
    CMD_GET_JOIN_REQUESTS,
    CMD_APPROVE_JOIN_REQUEST,
    CMD_REJECT_JOIN_REQUEST,
    CMD_CALLBACK_QUERY,
    CMD_GET_ANALYTICS,
    CMD_CREATE_POLL,
    CMD_VOTE_POLL,
    CMD_CLOSE_POLL,
    CMD_GET_SESSIONS,
    CMD_TERMINATE_SESSION,
    CMD_SEARCH, 
    CMD_BLOCK_USER,
    CMD_UNBLOCK_USER,
    CMD_GET_BLACKLIST,
    CMD_DEMOTE_ADMIN,
    CMD_PROMOTE_ADMIN,
)
from axiso import EdDSA, DH
from typing import Optional
import websockets

app = Quart(__name__)
app.secret_key = secrets.token_hex(32)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pending_auth = {}
clients = {}
entity_cache = {}
user_id_to_email = {}
my_display_name_cache = {}

def get_user_id():
    if 'user_id' not in session:
        session['user_id'] = secrets.token_hex(16)
    return session['user_id']

class WebChatClient(ChatClient):
    def __init__(self, url=None):
        if url is None:
            url = DEFAULT_WS_URL
        super().__init__(url)
        self.event_queue = asyncio.Queue()
        self._user_id = None
        self._reconnecting = False
        self._reconnect_lock = asyncio.Lock()
        self._reconnect_task = None
        self._reconnect_attempts = 0
        self._max_reconnect_delay = 60.0
        self._shared_secret = None
        self.requires_first_name = False
        self.stickers_cache = []
        self.sticker_files_cache = {}

    def handle_new_message(self, obj: dict):
        super().handle_new_message(obj)
        asyncio.create_task(self.event_queue.put(('new_message', obj)))

    def handle_edit_message(self, obj: dict):
        super().handle_edit_message(obj)
        asyncio.create_task(self.event_queue.put(('edit_message', obj)))

    def handle_file_stream(self, obj: dict):
        asyncio.create_task(self.event_queue.put(('file_stream', obj)))

    async def get_event(self):
        return await self.event_queue.get()

    async def send_file_from_bytes(self, entity_id: int, filename: str, data: bytes):
        with tempfile.NamedTemporaryFile(delete=False, suffix='.tmp') as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        try:
            await self.send_file(entity_id, tmp_path, filename=filename)
        finally:
            os.unlink(tmp_path)

    async def download_file_to_bytes(self, entity_id: int, message_id: int) -> bytes:
        key = f"{entity_id}_{message_id}"
        future = asyncio.get_running_loop().create_future()
        self.download_futures[key] = future
        tmp_path = None
        try:
            await self.send_command(
                'download_file',
                {'entity_id': entity_id, 'message_id': message_id}
            )
            await asyncio.wait_for(future, timeout=30.0)
            tmp_path, filename = self.download_results.pop(key, (None, None))
            if tmp_path is None:
                raise RuntimeError("Файл не получен")
            with open(tmp_path, 'rb') as f:
                data = f.read()
            return data
        except asyncio.TimeoutError:
            raise RuntimeError("Таймаут загрузки файла")
        except Exception as e:
            raise RuntimeError(f"Ошибка загрузки: {e}")
        finally:
            self.download_futures.pop(key, None)
            self.download_results.pop(key, None)
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    async def get_entity_info(self, entity_id: int) -> dict:
        try:
            result = await self.send_command('get_entity_info', {'entity_id': entity_id})
            return result
        except Exception as e:
            logger.warning(f"Не удалось получить info о сущности {entity_id}: {e}")
            return {'name': f'Сущность {entity_id}', 'type': 'chat', 'username': '', 'owner': ''}

    async def send_file(self, entity_id: int, file_path: str, filename: Optional[str] = None):
        await super().send_file(entity_id, file_path, filename)
        try:
            await self.refresh_session()
        except Exception as e:
            logger.warning(f"Не удалось обновить сессию после отправки файла: {e}")

    async def get_stickers(self, query: Optional[str] = None, limit: int = 50, offset: int = 0) -> list:
        params = {"limit": limit, "offset": offset}
        if query:
            params["query"] = query
        result = await self.send_command(CMD_GET_STICKERS, params)
        return result.get("stickers", [])

    async def download_sticker_file(self, sticker_id: int) -> bytes:
        result = await self.send_command(CMD_GET_STICKER_FILE, {'sticker_id': sticker_id})
        data_b64 = result.get("data")
        if not data_b64:
            raise RuntimeError("No sticker data")
        return base64.b64decode(data_b64)

    async def send_sticker(self, entity_id: int, sticker_id: int):
        await self.send_command(CMD_SEND_STICKER, {'entity_id': entity_id, 'sticker_id': sticker_id})

    async def set_entity_private(self, entity_id: int, is_private: bool):
        await self.send_command(CMD_SET_ENTITY_PRIVATE, {'entity_id': entity_id, 'is_private': is_private})

    async def get_join_requests(self, entity_id: int) -> list:
        result = await self.send_command(CMD_GET_JOIN_REQUESTS, {'entity_id': entity_id})
        return result.get('requests', [])

    async def approve_join_request(self, entity_id: int, email: str):
        await self.send_command(CMD_APPROVE_JOIN_REQUEST, {'entity_id': entity_id, 'email': email})

    async def reject_join_request(self, entity_id: int, email: str):
        await self.send_command(CMD_REJECT_JOIN_REQUEST, {'entity_id': entity_id, 'email': email})

    async def _refresh_session(self):
        if not self.email or not self.ed_private_key:
            raise RuntimeError("Нет email или ed25519 ключа для refresh")
        request_id = self._get_next_request_id()
        future = asyncio.get_running_loop().create_future()
        self.pending_requests[request_id] = future

        obj = {
            "type": REFRESH_SESSION_TYPE,
            "email": self.email,
            "timestamp": int(time.time()),
            "request_id": request_id,
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
            resp = await asyncio.wait_for(future, timeout=10.0)
        except asyncio.TimeoutError:
            self.pending_requests.pop(request_id, None)
            raise TimeoutError("Таймаут refresh")
        if resp.get("status") == "error":
            raise RuntimeError(resp.get("reason", "Ошибка refresh"))
        return resp.get("data", {})

    def _get_next_request_id(self):
        self.request_counter += 1
        return self.request_counter

    async def _reconnect(self):
        if self._reconnect_lock.locked():
            return
        async with self._reconnect_lock:
            self._reconnecting = True
            if not self.email or not self.client_priv_key or not self.server_public_key:
                logger.warning("Переподключение невозможно: нет данных для аутентификации")
                self._reconnecting = False
                return

            current_task = asyncio.current_task()
            if self.receive_task and self.receive_task is not current_task and not self.receive_task.done():
                self.receive_task.cancel()
            if self.refresh_task and not self.refresh_task.done():
                self.refresh_task.cancel()

            try:
                if self.websocket:
                    try:
                        await self.websocket.close()
                    except Exception:
                        pass
                await self.connect()
                from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
                priv = X25519PrivateKey.from_private_bytes(self.client_priv_key)
                pub = X25519PublicKey.from_public_bytes(self.server_public_key)
                shared = priv.exchange(pub)
                salt = self.email.encode('utf-8')
                info = b"chat_session"
                self.session_key = HKDF.extract_and_expand(salt, shared, info, 32)
                await self._refresh_session()

                self.running = True
                self._reconnect_attempts = 0
                self.receive_task = asyncio.create_task(self.receive_loop())
                self.refresh_task = asyncio.create_task(self._refresh_loop())
                logger.info(f"Переподключение успешно для {self.email}")
                asyncio.create_task(self.event_queue.put(('reconnected', {})))
            except Exception as e:
                logger.error(f"Ошибка переподключения (попытка {self._reconnect_attempts + 1}): {e}")
                self._reconnect_attempts += 1
                asyncio.create_task(self._schedule_reconnect())
            finally:
                self._reconnecting = False

    async def _schedule_reconnect(self):
        delay = min(self._max_reconnect_delay, 2 ** min(self._reconnect_attempts, 6))
        delay += secrets.randbelow(1000) / 1000.0
        await asyncio.sleep(delay)
        if not self.running and not self._reconnecting:
            await self._reconnect()

    async def receive_loop(self):
        try:
            async for message in self.websocket:
                try:
                    await self.process_message(message)
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
        except websockets.exceptions.ConnectionClosed:
            logger.info("Соединение закрыто, инициируем переподключение")
        except Exception as e:
            logger.error(f"Ошибка в receive_loop: {e}")
        else:
            return
        self.running = False
        asyncio.create_task(self.event_queue.put(('disconnected', {})))
        asyncio.create_task(self._reconnect())

    async def authenticate(self, email: str, sms_code: str):
        self.email = email

        if not self._load_ed_keys(email):
            ed_keys = EdDSA.generate_keypair()
            self.ed_private_key = base64.urlsafe_b64decode(ed_keys["private_key"])
            self.ed_public_key_b64 = ed_keys["public_key"]
            self._save_ed_keys(email)

        from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
        priv = X25519PrivateKey.from_private_bytes(self.client_priv_key)
        pub = X25519PublicKey.from_public_bytes(self.server_public_key)
        shared = priv.exchange(pub)
        salt = email.encode('utf-8')
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
                    raise RuntimeError(f"Ошибка сервера: {plain.get('reason', 'Unknown')}")
                else:
                    resp = plain
            except Exception:
                raise RuntimeError(f"Ошибка аутентификации: {e}")

        if resp is None:
            raise RuntimeError("Не удалось получить ответ от сервера")

        if resp.get("status") != "ok":
            raise RuntimeError(f"Ошибка аутентификации: {resp.get('reason', 'Unknown')}")

        resp_data = resp.get("data", {})
        self.user_public_key = resp_data.get("public_key")
        self._requires_first_name = resp_data.get("requires_first_name", False)
        self.requires_first_name = self._requires_first_name
        logger.info(f"Аутентификация успешна для {email}")

        self.running = True
        self._reconnect_attempts = 0
        self.receive_task = asyncio.create_task(self.receive_loop())
        self.refresh_task = asyncio.create_task(self._refresh_loop())

    async def close(self):
        if self._reconnect_task:
            self._reconnect_task.cancel()
        await super().close()

# ---------- HTML-шаблон логина ----------
LOGIN_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Aerisyn</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: system-ui, -apple-system, sans-serif; }
        body { background: #eef2f7; display: flex; justify-content: center; align-items: center; min-height: 100vh; padding: 20px; }
        #app { width: 420px; max-width: 100%; background: white; border-radius: 32px; padding: 35px 25px; box-shadow: 0 12px 40px rgba(0,0,0,0.08); }
        h1 { color: #005f60; font-size: 30px; text-align: center; margin-bottom: 30px; font-weight: 600; letter-spacing: -0.5px; }
        .input-group { margin-bottom: 18px; }
        input { width: 100%; padding: 14px 18px; border: none; background: #f5f7fa; border-radius: 20px; font-size: 16px; transition: 0.2s; }
        input:focus { outline: none; background: #eef1f5; }
        .button-group { display: flex; gap: 12px; margin-bottom: 20px; }
        button { flex: 1; padding: 14px; border: none; border-radius: 20px; font-size: 16px; font-weight: 500; cursor: pointer; transition: 0.2s; }
        button.primary { background: #005f60; color: white; }
        button.primary:hover { background: #004a4b; transform: translateY(-1px); }
        button.secondary { background: #e8eaed; color: #333; }
        button.secondary:hover { background: #d5d8dd; }
        #status { text-align: center; font-size: 14px; padding: 12px; background: #fafafa; border-radius: 16px; margin-top: 10px; min-height: 48px; }
        .error { color: #d32f2f; }
        .success { color: #2e7d32; }
        .info { color: #0d47a1; }
    </style>
</head>
<body>
<div id="app">
    <h1>Aerisyn</h1>
    <div class="input-group"><input type="email" id="email" placeholder="Email" /></div>
    <div class="input-group"><input type="text" id="code" placeholder="Код из письма" /></div>
    <div class="button-group">
        <button class="secondary" id="request-btn">Запросить код</button>
        <button class="primary" id="login-btn">Войти</button>
    </div>
    <div id="status">Готов к работе</div>
</div>
<script>
    const emailInput = document.getElementById('email');
    const codeInput = document.getElementById('code');
    const statusDiv = document.getElementById('status');

    function setStatus(text, type='') {
        statusDiv.textContent = text;
        statusDiv.className = type;
    }

    async function requestCode() {
        const email = emailInput.value.trim();
        if (!email) { setStatus('Введите email', 'error'); return; }
        setStatus('Отправка запроса...', 'info');
        try {
            const resp = await fetch('/api/request_code', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ email })
            });
            const data = await resp.json();
            if (resp.ok && data.status === 'ok') {
                setStatus('✅ Код отправлен на почту', 'success');
            } else {
                setStatus('❌ ' + (data.reason || 'Ошибка'), 'error');
            }
        } catch(e) {
            setStatus('❌ ' + e.message, 'error');
        }
    }

    async function login() {
        const code = codeInput.value.trim();
        if (!code) { setStatus('Введите код', 'error'); return; }
        setStatus('Вход...', 'info');
        try {
            const resp = await fetch('/api/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ code })
            });
            const data = await resp.json();
            if (resp.ok && data.status === 'ok') {
                if (data.requires_first_name) {
                    window.location.href = '/?setup_name=true';
                } else {
                    window.location.href = '/';
                }
            } else {
                setStatus('❌ ' + (data.reason || 'Ошибка'), 'error');
            }
        } catch(e) {
            setStatus('❌ ' + e.message, 'error');
        }
    }

    document.getElementById('request-btn').addEventListener('click', requestCode);
    document.getElementById('login-btn').addEventListener('click', login);
</script>
</body>
</html>
'''

# ---------- HTML-шаблон основного интерфейса ----------
UI_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>Aerisyn</title>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; font-family: system-ui, -apple-system, sans-serif; }
        body { background: #eef2f7; height: 100vh; overflow: hidden; }
        #app { display: flex; flex-direction: column; height: 100vh; max-width: 600px; margin: 0 auto; background: white; position: relative; }
        #top-bar { display: flex; align-items: center; justify-content: space-between; padding: 14px 18px; background: white; border-bottom: 1px solid #e0e4e8; flex-shrink: 0; min-height: 60px; }
        #logo { font-size: 22px; font-weight: 700; color: #005f60; cursor: pointer; letter-spacing: -0.5px; transition: color 0.2s; }
        #logo.reconnecting { color: #e65100; }
        #search-btn { background: none; border: none; font-size: 24px; cursor: pointer; padding: 4px 8px; }
        #entity-list { flex: 1; overflow-y: auto; padding: 8px 12px; background: #fafcfe; }
        .entity-item { display: flex; align-items: center; padding: 12px 14px; border-radius: 16px; cursor: pointer; transition: 0.15s; background: #f5f7fa; margin-bottom: 8px; }
        .entity-item:hover { background: #eef3f8; }
        .entity-item .icon-badge { width: 40px; height: 40px; border-radius: 12px; background: rgba(0,95,96,0.1); color: #005f60; display: flex; align-items: center; justify-content: center; font-size: 16px; margin-right: 14px; flex-shrink: 0; }
        .entity-item .info { flex: 1; min-width: 0; }
        .entity-item .name { font-weight: 500; font-size: 16px; }
        .entity-item .sub { font-size: 13px; color: #999; }
        #fab { position: fixed; bottom: 24px; right: 24px; width: 60px; height: 60px; border-radius: 50%; background: #005f60; color: white; border: none; font-size: 32px; box-shadow: 0 4px 16px rgba(0,95,96,0.3); cursor: pointer; transition: 0.2s; z-index: 10; display: flex; align-items: center; justify-content: center; }
        #fab:hover { transform: scale(1.05); background: #004a4b; }
        .modal-overlay { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); z-index: 100; display: flex; align-items: center; justify-content: center; visibility: hidden; opacity: 0; transition: 0.2s; }
        .modal-overlay.active { visibility: visible; opacity: 1; }
        .modal-content { background: white; border-radius: 32px; padding: 24px; max-width: 400px; width: 90%; max-height: 80vh; overflow-y: auto; box-shadow: 0 12px 40px rgba(0,0,0,0.2); position: relative; }
        .modal-content .close { position: absolute; top: 12px; right: 16px; font-size: 28px; cursor: pointer; color: #aaa; }
        .modal-content .close:hover { color: #333; }
        .modal-content h3 { margin-bottom: 16px; color: #222; font-size: 18px; font-weight: 600; text-align: center; }
        .modal-content .modal-subtitle { font-size: 12px; color: #999; text-align: center; margin: -10px 0 16px; }
        .modal-content .profile-avatar { width: 64px; height: 64px; border-radius: 20px; background: rgba(0,95,96,0.1); color: #005f60; font-size: 26px; font-weight: 600; display: flex; align-items: center; justify-content: center; margin: 0 auto 10px; }
        .modal-content .profile-name { font-size: 20px; font-weight: 600; text-align: center; margin: 0 0 2px; }
        .modal-content .profile-sub { font-size: 12px; color: #999; text-align: center; margin-bottom: 18px; }
        .modal-content .profile-buttons { display: flex; gap: 4px; background: #f0f2f5; border-radius: 16px; padding: 4px; margin: 16px 0 4px; }
        .modal-content .profile-buttons button { flex: 1; padding: 8px; border: none; background: transparent; border-radius: 12px; color: #777; font-size: 13px; font-weight: 500; cursor: pointer; transition: 0.15s; }
        .modal-content .profile-buttons button:hover { background: white; color: #005f60; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }
        .modal-content .profile-buttons button.danger:hover { background: white; color: #c62828; }
        .modal-content input, .modal-content select { width: 100%; padding: 12px 16px; border: none; background: #f5f7fa; border-radius: 16px; font-size: 16px; margin-bottom: 12px; }
        .modal-content button { padding: 12px 20px; border: none; border-radius: 20px; background: #005f60; color: white; font-size: 16px; cursor: pointer; width: 100%; margin-top: 6px; }
        .modal-content button.secondary { background: #e8eaed; color: #333; }
        .modal-content .button-group { display: flex; gap: 12px; margin-top: 8px; }
        .modal-content .button-group button { flex: 1; }
        #chat-screen { position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: white; z-index: 50; display: flex; flex-direction: column; transform: translateX(100%); transition: 0.3s ease; }
        #chat-screen.open { transform: translateX(0); }
        #chat-header { display: flex; align-items: center; padding: 12px 16px; border-bottom: 1px solid #e0e4e8; background: white; flex-shrink: 0; cursor: pointer; }
        #chat-header .header-left { display: flex; align-items: center; flex: 1; }
        #chat-back { font-size: 28px; cursor: pointer; padding: 0 8px; }
        #chat-title { font-size: 18px; font-weight: 500; margin-left: 10px; cursor: pointer; }
        #chat-title:hover { text-decoration: underline; }
        #chat-menu-btn { background: none; border: none; font-size: 24px; cursor: pointer; padding: 4px 8px; color: #555; }
        #chat-menu-dropdown { position: absolute; top: 60px; right: 16px; background: white; border-radius: 16px; box-shadow: 0 8px 24px rgba(0,0,0,0.15); min-width: 160px; padding: 8px 0; z-index: 60; visibility: hidden; opacity: 0; transition: 0.2s; }
        #chat-menu-dropdown.show { visibility: visible; opacity: 1; }
        #chat-menu-dropdown .menu-item { padding: 10px 20px; cursor: pointer; transition: 0.15s; display: flex; align-items: center; gap: 10px; font-size: 14px; }
        #chat-menu-dropdown .menu-item:hover { background: #f0f2f5; }
        #chat-menu-dropdown .menu-item.danger { color: #d32f2f; }
        #chat-messages { flex: 1; overflow-y: auto; padding: 16px 20px; background: #fafcfe; display: flex; flex-direction: column; }
        .message { display: flex; flex-direction: column; margin-bottom: 12px; width: 100%; align-items: flex-start; }
        .message .bubble { max-width: 80%; padding: 10px 16px; border-radius: 20px; background: #f0f2f5; word-break: break-word; line-height: 1.5; position: relative; }
        .message .bubble .delete-msg { position: absolute; top: -6px; right: -6px; background: #ffebee; border: none; border-radius: 50%; width: 22px; height: 22px; font-size: 14px; cursor: pointer; color: #c62828; display: none; align-items: center; justify-content: center; }
        .message .bubble:hover .delete-msg { display: flex; }
        .message .meta { font-size: 12px; color: #888; margin-bottom: 3px; padding-left: 4px; cursor: pointer; }
        .message .meta .from-name { font-weight: 500; color: #005f60; cursor: pointer; }
        .message .meta .from-name:hover { text-decoration: underline; }
        .message .meta .time { margin-left: 8px; }
        .message.self { align-items: flex-end; }
        .message.self .bubble { background: #d4edda; }
        .message .bubble .media-container { margin-top: 4px; max-width: 100%; cursor: pointer; }
        .message .bubble .media-container img { max-width: 100%; max-height: 300px; border-radius: 12px; display: block; }
        .message .bubble .media-container video { max-width: 100%; max-height: 400px; border-radius: 12px; background: #000; }
        .message .bubble .media-container audio { width: 100%; margin-top: 4px; }
        .message .bubble .file-download { display: inline-flex; align-items: center; gap: 8px; padding: 4px 8px; background: rgba(0,0,0,0.04); border-radius: 12px; text-decoration: none; color: #005f60; }
        .message .bubble .file-download:hover { background: rgba(0,0,0,0.08); }
        .sticker-message { width: 100%; display: flex; flex-direction: column; align-items: flex-start; margin-bottom: 12px; }
        .sticker-message.self { align-items: flex-end; }
        .sticker-message .meta { font-size: 12px; color: #888; padding: 2px 0 4px 0; cursor: pointer; }
        .sticker-message .meta .from-name { font-weight: 500; color: #005f60; cursor: pointer; }
        .sticker-message .meta .from-name:hover { text-decoration: underline; }
        .sticker-message .sticker-container { position: relative; max-width: 200px; max-height: 200px; cursor: pointer; transition: 0.15s; border-radius: 16px; overflow: hidden; background: transparent; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .sticker-message .sticker-container img { width: 100%; height: auto; display: block; }
        .sticker-message .sticker-container .delete-msg { position: absolute; top: -6px; right: -6px; background: #ffebee; border: none; border-radius: 50%; width: 22px; height: 22px; font-size: 14px; cursor: pointer; color: #c62828; display: none; align-items: center; justify-content: center; z-index: 5; }
        .sticker-message .sticker-container:hover .delete-msg { display: flex; }
        .sticker-message .reactions { align-self: flex-start; margin-top: 2px; }
        .sticker-message.self .reactions { align-self: flex-end; }
        .reactions { display: flex; gap: 4px; margin-top: 4px; flex-wrap: wrap; }
        .reactions .reaction-badge { display: inline-flex; align-items: center; gap: 2px; padding: 2px 8px; background: #e8eaed; border-radius: 12px; font-size: 14px; cursor: pointer; transition: 0.15s; user-select: none; }
        .reactions .reaction-badge:hover { background: #d5d8dd; }
        .reactions .reaction-badge.active { background: #d4edda; border: 1px solid #2e7d32; }
        .reactions .reaction-badge .count { font-size: 12px; color: #666; margin-left: 2px; }
        #chat-input-area { display: flex; flex-direction: column; padding: 12px 16px 20px; border-top: 1px solid #f0f2f5; background: white; gap: 6px; flex-shrink: 0; }
        #chat-input-row { display: flex; gap: 6px; align-items: center; flex-wrap: nowrap; }
        #chat-input-area .sticker-btn {
            font-size: 24px;
            cursor: pointer;
            background: none;
            border: none;
            transition: 0.15s;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 8px 12px;
            min-width: 44px;
            min-height: 44px;
        }
        #chat-input-area .sticker-btn:hover { transform: scale(1.1); }
        #chat-input-area .sticker-btn.active { background: #e8eaed; border-radius: 12px; }
        #chat-input-area input[type="text"] { flex: 1; padding: 12px 18px; border: none; background: #f5f7fa; border-radius: 24px; font-size: 15px; min-width: 0; }
        #chat-input-area input[type="text"]:focus { outline: none; background: #eef1f5; }
        #chat-input-area .file-label { font-size: 22px; cursor: pointer; padding: 6px 8px; }
        #chat-input-area .file-input { display: none; }
        #chat-input-area #chat-send { padding: 12px 20px; border: none; border-radius: 24px; background: #005f60; color: white; font-size: 15px; cursor: pointer; white-space: nowrap; }
        #reply-preview { display: none; padding: 6px 12px; background: #f0f2f5; border-radius: 12px; margin-bottom: 6px; font-size: 0.9em; align-items: center; justify-content: space-between; }
        #reply-preview .reply-info { display: flex; flex-direction: column; overflow: hidden; }
        #reply-preview .reply-author { font-weight: 500; }
        #reply-preview .reply-content { color: #555; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        #reply-cancel { background: none; border: none; font-size: 18px; cursor: pointer; padding: 0 6px; color: #888; }
        #reply-cancel:hover { color: #333; }
        #sticker-panel { position: absolute; bottom: 70px; left: 0; right: 0; background: white; border-top: 1px solid #e0e4e8; padding: 12px 16px 20px; z-index: 55; display: none; flex-direction: column; max-height: 340px; box-shadow: 0 -4px 20px rgba(0,0,0,0.05); }
        #sticker-panel.open { display: flex; }
        #sticker-search { display: flex; gap: 8px; margin-bottom: 10px; align-items: center; }
        #sticker-search input { flex: 1; padding: 8px 14px; border: none; background: #f5f7fa; border-radius: 20px; font-size: 15px; }
        #sticker-search input:focus { outline: none; background: #eef1f5; }
        #sticker-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; overflow-y: auto; flex: 1; padding-bottom: 4px; }
        #sticker-grid .sticker-item { cursor: pointer; transition: 0.15s; border-radius: 12px; overflow: hidden; background: #f5f7fa; display: flex; align-items: center; justify-content: center; aspect-ratio: 1; padding: 4px; }
        #sticker-grid .sticker-item:hover { background: #e8ecf1; transform: scale(1.02); }
        #sticker-grid .sticker-item img { max-width: 100%; max-height: 100%; object-fit: contain; display: block; }
        #media-modal .modal-content { max-width: 90vw; max-height: 90vh; padding: 20px; background: #111; }
        #media-modal .modal-content img, #media-modal .modal-content video { max-width: 100%; max-height: 80vh; display: block; margin: 0 auto; }
        #media-modal .modal-content .close { color: #fff; top: 8px; right: 12px; font-size: 32px; }
        #media-modal .modal-content .media-info { color: #ccc; text-align: center; margin-top: 8px; font-size: 14px; }
        #reaction-panel { position: fixed; bottom: 100px; left: 50%; transform: translateX(-50%); background: rgba(0,0,0,0.85); border-radius: 16px; padding: 8px 12px; display: none; z-index: 150; backdrop-filter: blur(8px); -webkit-backdrop-filter: blur(8px); box-shadow: 0 8px 32px rgba(0,0,0,0.3); touch-action: none; user-select: none; -webkit-user-select: none; }
        #reaction-panel.active { display: flex; gap: 4px; animation: fadeUp 0.15s ease-out; }
        @keyframes fadeUp { from { opacity: 0; transform: translateX(-50%) translateY(10px); } to { opacity: 1; transform: translateX(-50%) translateY(0); } }
        #reaction-panel .reaction-emoji { font-size: 32px; padding: 4px 8px; border-radius: 12px; cursor: pointer; transition: 0.15s; text-align: center; min-width: 44px; line-height: 1.4; }
        #reaction-panel .reaction-emoji:hover { background: rgba(255,255,255,0.15); transform: scale(1.1); }
        #reaction-panel .reaction-emoji:active { transform: scale(0.9); }
        @media (max-width: 600px) { #app { max-width: 100%; } #reaction-panel .reaction-emoji { font-size: 28px; padding: 2px 6px; min-width: 36px; } .sticker-message .sticker-container { max-width: 160px; max-height: 160px; } }
        #toast { position: fixed; bottom: 80px; left: 50%; transform: translateX(-50%); background: #333; color: white; padding: 10px 20px; border-radius: 30px; font-size: 14px; z-index: 200; opacity: 0; visibility: hidden; transition: 0.3s; max-width: 90%; text-align: center; }
        #toast.show { opacity: 1; visibility: visible; }
        #toast.error { background: #d32f2f; }
        #toast.success { background: #2e7d32; }
        .create-menu { display: flex; flex-direction: column; gap: 12px; }
        .create-menu .item { padding: 16px; border-radius: 16px; background: #f5f7fa; cursor: pointer; transition: 0.15s; display: flex; align-items: center; gap: 14px; }
        .create-menu .item:hover { background: #e8ecf1; }
        .create-menu .item .icon { width: 40px; height: 40px; border-radius: 12px; background: rgba(0,95,96,0.1); color: #005f60; display: flex; align-items: center; justify-content: center; font-size: 18px; flex-shrink: 0; }
        .create-menu .item .label { font-weight: 500; }
        #search-results { margin-top: 4px; max-height: 260px; overflow-y: auto; }

        #name-modal .modal-content { max-width: 360px; }
        #name-modal .modal-content h3 { text-align: center; }
        #name-modal .modal-content .input-group { margin: 16px 0; }
        #name-modal .modal-content .input-group input { width: 100%; padding: 12px 16px; border: none; background: #f5f7fa; border-radius: 20px; font-size: 16px; }
        #name-modal .modal-content .button-group { display: flex; gap: 12px; }
        #name-modal .modal-content .button-group button { flex: 1; padding: 12px; border: none; border-radius: 20px; font-size: 16px; font-weight: 500; cursor: pointer; transition: 0.2s; }
        #name-modal .modal-content .button-group .primary { background: #005f60; color: white; }
        #name-modal .modal-content .button-group .primary:hover { background: #004a4b; }
        #name-modal .modal-content .button-group .secondary { background: #e8eaed; color: #333; }
        #name-modal .modal-content .button-group .secondary:hover { background: #d5d8dd; }
        #name-modal .modal-content .error-text { color: #d32f2f; font-size: 14px; margin-top: 8px; text-align: center; display: none; }
        #join-requests-modal .request-item { display: flex; justify-content: space-between; align-items: center; background: #f5f7fa; border-radius: 16px; padding: 12px 16px; margin-bottom: 8px; }
        #join-requests-modal .request-item .info { flex: 1; }
        #join-requests-modal .request-item .info .name { font-weight: 500; }
        #join-requests-modal .request-item .info .username { font-size: 13px; color: #888; }
        #join-requests-modal .request-item .actions { display: flex; gap: 8px; }
        #join-requests-modal .request-item .actions button { padding: 4px 12px; border: none; border-radius: 12px; font-size: 13px; cursor: pointer; }
        #join-requests-modal .request-item .actions .approve { background: #2e7d32; color: white; }
        #join-requests-modal .request-item .actions .reject { background: #c62828; color: white; }
        #join-requests-modal .private-toggle { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; }
        #join-requests-modal .private-toggle label { font-weight: 500; }
        #join-requests-modal .private-toggle input[type="checkbox"] { width: 20px; height: 20px; cursor: pointer; }
        .empty { text-align: center; color: #999; padding: 40px 0; font-size: 13px; }
        .list-item { display: flex; justify-content: space-between; align-items: center; background: #f5f7fa; border-radius: 16px; padding: 12px 16px; margin-bottom: 8px; }
        .list-item .value { font-weight: 500; }
        .list-item button { padding: 6px 16px; border: none; border-radius: 16px; background: #c62828; color: white; cursor: pointer; font-size: 13px; flex-shrink: 0; }
        .reply-preview { font-size:0.9em; background:#f5f7fa; border-radius:8px; padding:4px 10px; margin-bottom:4px; border-left:3px solid #005f60; overflow:hidden; white-space:nowrap; text-overflow:ellipsis; max-width:100%; }
        .reply-preview .reply-author { font-weight:500; }
        .reply-preview .reply-content { color:#555; }
        #settings-modal .settings-edit { display: none; margin-top: -2px; margin-bottom: 10px; }
        #settings-modal .settings-edit input { width: 100%; padding: 8px 12px; border: 1px solid #ddd; border-radius: 12px; font-size: 14px; margin-bottom: 6px; }
        #settings-modal .settings-edit .edit-actions { display: flex; gap: 8px; }
        #settings-modal .settings-edit .edit-actions button { padding: 6px 16px; border: none; border-radius: 16px; font-size: 13px; cursor: pointer; }
        #settings-modal .settings-edit .edit-actions .save { background: #005f60; color: white; }
        #settings-modal .settings-edit .edit-actions .cancel { background: #e8eaed; color: #333; }
        .audio-player {
            display: flex;
            flex-direction: column;
            width: 100%;
            max-width: 400px;
            padding: 10px 14px 10px 10px;
            border-radius: 16px;
            gap: 6px;
            background: var(--msg-bg, #f0f2f5);
        }
        .audio-player .row {
            display: flex;
            align-items: center;
            gap: 12px;
            width: 100%;
        }
        .audio-player .cover {
            width: 54px;
            height: 54px;
            border-radius: 50%;
            flex-shrink: 0;
            position: relative;
            overflow: hidden;
            background: var(--cover-color, #d4edda);
        }
        .audio-player .cover .play-btn {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            width: 36px;
            height: 36px;
            border-radius: 50%;
            background: rgba(0,0,0,0.5);
            border: 2px solid white;
            color: white;
            font-size: 18px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: 0.2s;
        }
        .audio-player .cover .play-btn:hover {
            background: rgba(0,0,0,0.7);
        }
        .audio-player .info {
            flex: 1;
            min-width: 0;
        }
        .audio-player .info .title {
            font-size: 15px;
            font-weight: 500;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            line-height: 1.3;
        }
        .audio-player .info .artist {
            font-size: 13px;
            color: #666;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            line-height: 1.3;
        }
        .audio-player .info .artist:empty {
            display: none;
        }
        .audio-player .progress {
            width: 100%;
            height: 4px;
            -webkit-appearance: none;
            appearance: none;
            background: rgba(0,0,0,0.12);
            border-radius: 2px;
            outline: none;
            cursor: pointer;
            margin: 2px 0 0;
        }
        .audio-player .progress::-webkit-slider-thumb {
            -webkit-appearance: none;
            appearance: none;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #005f60;
            cursor: pointer;
        }
        .audio-player .progress::-moz-range-thumb {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: #005f60;
            border: none;
            cursor: pointer;
        }
        #attachment-menu .menu-item { padding: 10px 20px; cursor: pointer; transition: 0.15s; display: flex; align-items: center; gap: 10px; font-size: 14px; }
        #attachment-menu .menu-item:hover { background: #f0f2f5; }
        .poll-form .form-group { margin-bottom: 16px; }
        .poll-form .form-group label { display: block; font-weight: 500; margin-bottom: 6px; }
        .poll-option-row { display: flex; gap: 8px; margin-bottom: 8px; align-items: center; }
        .poll-option-row input { flex: 1; padding: 8px 12px; border: none; background: #f5f7fa; border-radius: 12px; }
        .poll-remove-option { background: none; border: none; font-size: 18px; cursor: pointer; color: #c62828; }
        #poll-add-option { background: none; border: 1px dashed #aaa; border-radius: 12px; padding: 8px; width: 100%; cursor: pointer; margin-bottom: 12px; }
        #poll-create { width: 100%; padding: 12px; border: none; border-radius: 20px; background: #005f60; color: white; font-size: 16px; cursor: pointer; }
        .poll-result-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; }
        .poll-result-bar .bar { flex: 1; height: 8px; background: #e0e4e8; border-radius: 4px; overflow: hidden; }
        .poll-result-bar .bar .fill { height: 100%; background: #005f60; border-radius: 4px; transition: width 0.3s; }
        .poll-result-bar .pct { width: 40px; text-align: right; font-size: 13px; color: #555; }
        .poll-option-label { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; cursor: pointer; }
        .poll-option-label input[type="radio"] { margin: 0; }
        .poll-vote-btn { margin-top: 8px; padding: 6px 16px; border: none; border-radius: 16px; background: #005f60; color: white; cursor: pointer; }
        .poll-close-btn { margin-left: 8px; padding: 4px 12px; border: none; border-radius: 16px; background: #c62828; color: white; cursor: pointer; font-size: 12px; }
        /* ---- Аналитика ---- */
        .analytics-header { text-align: center; margin-bottom: 18px; }
        .analytics-header .entity-name { font-size: 18px; font-weight: 600; color: #222; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .analytics-header .entity-sub { font-size: 12px; color: #999; margin-top: 2px; }
        .analytics-tabs { display: flex; gap: 4px; background: #f0f2f5; border-radius: 16px; padding: 4px; margin-bottom: 18px; }
        .analytics-tabs button { flex: 1; border: none; background: transparent; padding: 8px; border-radius: 12px; font-size: 13px; font-weight: 500; color: #777; cursor: pointer; transition: 0.15s; }
        .analytics-tabs button.active { background: white; color: #005f60; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }
        .stats-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin-bottom: 18px; }
        .stat-card { background: #f5f7fa; border-radius: 18px; padding: 14px; display: flex; align-items: flex-start; gap: 10px; }
        .stat-card .stat-icon { width: 36px; height: 36px; border-radius: 11px; background: rgba(0,95,96,0.1); color: #005f60; display: flex; align-items: center; justify-content: center; font-size: 14px; flex-shrink: 0; }
        .stat-card .stat-body { min-width: 0; }
        .stat-card .stat-value { font-size: 19px; font-weight: 700; color: #222; line-height: 1.2; }
        .stat-card .stat-label { font-size: 11.5px; color: #888; margin-top: 2px; }
        .stat-card .stat-trend { font-size: 11px; font-weight: 600; margin-top: 5px; display: flex; align-items: center; gap: 4px; }
        .stat-trend.up { color: #2e7d32; }
        .stat-trend.down { color: #c62828; }
        .stat-trend.flat { color: #999; }
        .chart-card { background: #f5f7fa; border-radius: 20px; padding: 16px 16px 8px; }
        .chart-card .chart-title { font-size: 13px; font-weight: 600; color: #444; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
        .chart-card .chart-title .muted { font-weight: 400; color: #999; }
        .analytics-empty { text-align: center; color: #999; padding: 40px 0; font-size: 13px; }

        /* ---- Единый "современный" стиль (по образцу Аналитики) ---- */
        .section-header { text-align: center; margin-bottom: 18px; }
        .section-header .section-title { font-size: 18px; font-weight: 600; color: #222; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .section-header .section-sub { font-size: 12px; color: #999; margin-top: 2px; }

        .icon-row { background: #f5f7fa; border-radius: 18px; padding: 12px 14px; margin-bottom: 10px; display: flex; align-items: center; gap: 12px; cursor: pointer; transition: 0.15s; }
        .icon-row:hover { box-shadow: 0 2px 6px rgba(0,0,0,0.08); }
        .icon-row .row-icon { width: 36px; height: 36px; border-radius: 11px; background: rgba(0,95,96,0.1); color: #005f60; display: flex; align-items: center; justify-content: center; font-size: 14px; flex-shrink: 0; }
        .icon-row .row-icon.danger { background: rgba(198,40,40,0.1); color: #c62828; }
        .icon-row .row-body { min-width: 0; flex: 1; }
        .icon-row .row-title { font-weight: 500; font-size: 14px; color: #222; }
        .icon-row .row-sub { font-size: 12px; color: #888; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .icon-row .row-value { font-size: 12.5px; color: #888; flex-shrink: 0; }
        .icon-row .row-chevron { color: #ccc; font-size: 13px; flex-shrink: 0; }

        .pill-group { display: flex; gap: 4px; background: #f0f2f5; border-radius: 16px; padding: 4px; margin-bottom: 16px; }
        .pill-group button { flex: 1; border: none; background: transparent; padding: 8px; border-radius: 12px; font-size: 13px; font-weight: 500; color: #777; cursor: pointer; transition: 0.15s; }
        .pill-group button.active { background: white; color: #005f60; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }
        .pill-group button.danger.active { color: #c62828; }

        .group-tag { font-size: 11.5px; font-weight: 600; color: #999; text-transform: uppercase; letter-spacing: 0.3px; margin: 14px 0 8px; padding-left: 2px; }
        .group-tag:first-child { margin-top: 0; }
        .type-tag { font-size: 10.5px; color: #005f60; background: rgba(0,95,96,0.1); border-radius: 8px; padding: 2px 7px; flex-shrink: 0; }
    </style>
</head>
<body>

<div id="app">
    <div id="top-bar">
        <span id="logo">Aerisyn</span>
        <button id="search-btn"><i class="fas fa-search"></i></button>
    </div>
    <div id="entity-list"></div>
</div>

<button id="fab"><i class="fas fa-plus-circle"></i></button>

<!-- Модалки -->
<div class="modal-overlay" id="create-modal">
    <div class="modal-content">
        <span class="close" id="create-close">&times;</span>
        <h3>Создать</h3>
        <div class="create-menu">
            <div class="item" data-type="chat"><span class="icon"><i class="fas fa-comment-dots"></i></span><span class="label">Чат</span></div>
            <div class="item" data-type="group"><span class="icon"><i class="fas fa-user-shield"></i></span><span class="label">Группа</span></div>
            <div class="item" data-type="channel"><span class="icon"><i class="fas fa-bullhorn"></i></span><span class="label">Канал</span></div>
        </div>
        <div id="create-detail" style="display:none; margin-top:16px;">
            <input id="create-name" placeholder="Имя" />
            <input id="create-username" placeholder="Юзернейм (опционально)" />
            <input id="create-target" placeholder="Email или @username (для чата)" style="display:none;" />
            <button id="create-confirm">Создать</button>
            <button id="create-cancel" class="secondary">Отмена</button>
        </div>
    </div>
</div>

<!-- Модалка профиля (общая для своего и чужих) -->
<div class="modal-overlay" id="profile-modal">
    <div class="modal-content">
        <span class="close" id="profile-close">&times;</span>
        <div id="profile-content"></div>
    </div>
</div>

<div class="modal-overlay" id="search-modal">
    <div class="modal-content">
        <span class="close" id="search-close">&times;</span>
        <div class="section-header">
            <div class="section-title">Поиск</div>
            <div class="section-sub">Люди, чаты и каналы</div>
        </div>
        <input id="search-input" placeholder="Введите имя или @username" />
        <div id="search-results"></div>
        <div class="group-tag">Вступить по ID</div>
        <input id="join-id" placeholder="ID сущности" type="number" />
        <button id="join-btn">Вступить</button>
    </div>
</div>

<div class="modal-overlay" id="media-modal">
    <div class="modal-content">
        <span class="close" id="media-close">&times;</span>
        <div id="media-container"></div>
        <div class="media-info" id="media-info"></div>
    </div>
</div>

<div class="modal-overlay" id="name-modal">
    <div class="modal-content">
        <h3>Добро пожаловать!</h3>
        <p style="text-align:center; color:#666; margin-bottom:16px;">Пожалуйста, укажите ваше имя, чтобы продолжить</p>
        <div class="input-group">
            <input type="text" id="name-input" placeholder="Ваше имя" maxlength="64" />
        </div>
        <div class="error-text" id="name-error">Имя не может быть пустым</div>
        <div class="button-group">
            <button class="secondary" id="name-cancel">Отмена</button>
            <button class="primary" id="name-save">Сохранить</button>
        </div>
    </div>
</div>

<!-- Модалка заявок на вступление -->
<div class="modal-overlay" id="join-requests-modal">
    <div class="modal-content">
        <span class="close" id="join-requests-close">&times;</span>
        <h3>Управление заявками</h3>
        <div class="private-toggle">
            <label for="private-toggle-check">Частный режим</label>
            <input type="checkbox" id="private-toggle-check" />
        </div>
        <div id="join-requests-list">Загрузка...</div>
    </div>
</div>

<!-- Модалка настроек -->
<div class="modal-overlay" id="settings-modal">
    <div class="modal-content">
        <span class="close" id="settings-close">&times;</span>
        <div class="section-header">
            <div class="section-title">Настройки</div>
            <div class="section-sub">Аккаунт и приватность</div>
        </div>
        <div class="icon-row settings-item" id="settings-username">
            <div class="row-icon"><i class="fas fa-at"></i></div>
            <div class="row-body">
                <div class="row-title">Установить юзернейм</div>
            </div>
            <span class="row-value" id="settings-username-current">не установлен</span>
        </div>
        <div id="settings-username-edit" class="settings-edit">
            <input type="text" id="settings-username-input" placeholder="Новый юзернейм (без @)" />
            <div class="edit-actions">
                <button class="save" id="settings-username-save">Сохранить</button>
                <button class="cancel" id="settings-username-cancel">Отмена</button>
            </div>
        </div>
        <div class="icon-row settings-item" id="settings-email-visibility">
            <div class="row-icon"><i class="fas fa-envelope"></i></div>
            <div class="row-body">
                <div class="row-title">Показывать email</div>
            </div>
            <span class="row-value" id="settings-email-status">вкл</span>
        </div>
        <div class="icon-row settings-item" id="settings-sessions">
            <div class="row-icon"><i class="fas fa-desktop"></i></div>
            <div class="row-body">
                <div class="row-title">Управление сессиями</div>
            </div>
            <span class="row-chevron"><i class="fas fa-chevron-right"></i></span>
        </div>
        <div class="icon-row settings-item" id="settings-blacklist">
            <div class="row-icon danger"><i class="fas fa-ban"></i></div>
            <div class="row-body">
                <div class="row-title">Черный список</div>
            </div>
            <span class="row-chevron"><i class="fas fa-chevron-right"></i></span>
        </div>
    </div>
</div>

<!-- Панель реакций -->
<div id="reaction-panel">
    <span class="reaction-emoji" data-reaction="❤️">❤️</span>
    <span class="reaction-emoji" data-reaction="😂">😂</span>
    <span class="reaction-emoji" data-reaction="😮">😮</span>
    <span class="reaction-emoji" data-reaction="😢">😢</span>
    <span class="reaction-emoji" data-reaction="😡">😡</span>
    <span class="reaction-emoji" data-reaction="👍">👍</span>
    <span class="reaction-emoji" data-reaction="👎">👎</span>
    <span class="reaction-emoji" data-reaction="🎉">🎉</span>
</div>

<div class="modal-overlay" id="blacklist-modal">
    <div class="modal-content">
        <span class="close" id="blacklist-close">&times;</span>
        <h3>Черный список</h3>
        <div id="blacklist-list">Загрузка...</div>
    </div>
</div>


<!-- Меню скрепки -->
<div id="attachment-menu" style="display:none; position:fixed; bottom:80px; left:20px; background:white; border-radius:16px; box-shadow:0 8px 24px rgba(0,0,0,0.15); padding:8px 0; z-index:60; min-width:150px;">
    <div class="menu-item" id="attach-file"><i class="fas fa-file"></i> Файл</div>
    <div class="menu-item" id="attach-poll"><i class="fas fa-poll"></i> Опрос</div>
</div>

<!-- Модалка опроса -->
<div class="modal-overlay" id="poll-modal">
    <div class="modal-content">
        <span class="close" id="poll-close">&times;</span>
        <h3>Создать опрос</h3>
        <div class="poll-form">
            <div class="form-group">
                <label>Вопрос</label>
                <input type="text" id="poll-question" placeholder="Введите вопрос" />
            </div>
            <div class="form-group" id="poll-options-container">
                <label>Варианты ответа</label>
                <div class="poll-option-row">
                    <input type="text" class="poll-option" placeholder="Вариант 1" />
                    <button class="poll-remove-option" style="display:none;">✕</button>
                </div>
                <div class="poll-option-row">
                    <input type="text" class="poll-option" placeholder="Вариант 2" />
                    <button class="poll-remove-option" style="display:none;">✕</button>
                </div>
            </div>
            <button id="poll-add-option">+ Добавить вариант</button>
            <button id="poll-create" class="primary">Создать</button>
        </div>
    </div>
</div>

<!-- Модалка аналитики -->
<div class="modal-overlay" id="analytics-modal">
    <div class="modal-content" style="max-width: 640px;">
        <span class="close" id="analytics-close">&times;</span>
        <div class="analytics-header">
            <div class="entity-name" id="analytics-entity-name">Аналитика</div>
            <div class="entity-sub">Обзор активности</div>
        </div>
        <div class="analytics-tabs">
            <button type="button" class="active" data-period="7">7 дней</button>
            <button type="button" data-period="30">30 дней</button>
        </div>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-users"></i></div>
                <div class="stat-body">
                    <div class="stat-value" id="stat-members">0</div>
                    <div class="stat-label">Участников</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-comment"></i></div>
                <div class="stat-body">
                    <div class="stat-value" id="stat-messages">0</div>
                    <div class="stat-label" id="stat-messages-label">Сообщений за неделю</div>
                    <div class="stat-trend" id="stat-messages-trend"></div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-heart"></i></div>
                <div class="stat-body">
                    <div class="stat-value" id="stat-reactions">0</div>
                    <div class="stat-label" id="stat-reactions-label">Реакций за неделю</div>
                </div>
            </div>
            <div class="stat-card">
                <div class="stat-icon"><i class="fas fa-chart-line"></i></div>
                <div class="stat-body">
                    <div class="stat-value" id="stat-avg">0</div>
                    <div class="stat-label">Сообщений в день, в среднем</div>
                </div>
            </div>
        </div>
        <div class="chart-card">
            <div class="chart-title">
                <span>Динамика сообщений</span>
                <span class="muted" id="chart-range-label">последние 7 дней</span>
            </div>
            <div id="analytics-chart-wrap" style="width: 100%; height: 240px;">
                <canvas id="activityChart"></canvas>
            </div>
            <div class="analytics-empty" id="analytics-empty" style="display:none;">За этот период сообщений не было</div>
        </div>
    </div>
</div>

<!-- Модалка управления сессиями -->
<div class="modal-overlay" id="sessions-modal">
    <div class="modal-content">
        <span class="close" id="sessions-close">&times;</span>
        <h3>Управление сессиями</h3>
        <p style="font-size:13px; color:#888; margin-bottom:12px;">
            Здесь перечислены все ваши активные сессии. Вы можете завершить любую, кроме текущей.
            Для завершения других сессий ваша текущая сессия должна быть активна более 30 минут.
        </p>
        <div id="sessions-list">Загрузка...</div>
    </div>
</div>

<!-- Экран чата -->
<div id="chat-screen">
    <div id="chat-header">
        <div class="header-left">
            <span id="chat-back">‹</span>
            <span id="chat-title">Чат</span>
        </div>
        <button id="chat-menu-btn">⋯</button>
        <div id="chat-menu-dropdown">
            <div class="menu-item" id="menu-admins" style="display:none;"><i class="fas fa-user-shield"></i> Админы</div>
            <div class="menu-item" id="menu-invites" style="display:none;"><i class="fas fa-key"></i> Приглашения</div>
            <div class="menu-item" id="menu-analytics"><i class="fas fa-chart-line"></i> Аналитика</div>
            <div class="menu-item danger" id="menu-block" style="display:none;"><i class="fas fa-ban"></i> Заблокировать</div>
            <div class="menu-item danger" id="menu-leave"><i class="fas fa-sign-out-alt"></i> Выйти</div>
        </div>
    </div>
    <div id="chat-messages"></div>
    <!-- БЛОК "ВСТУПИТЬ" ДЛЯ НЕ-УЧАСТНИКОВ -->
    <div id="join-prompt" style="display: none; flex: 1; flex-direction: column; align-items: center; justify-content: center; padding: 20px; text-align: center;">
        <p style="font-size: 18px; color: #555; margin-bottom: 16px;">Вы не являетесь участником этой сущности.</p>
        <button id="join-entity-btn" style="padding: 12px 32px; border: none; border-radius: 20px; background: #005f60; color: white; font-size: 18px; cursor: pointer; transition: 0.15s;">Вступить</button>
    </div>
    <!-- КОНЕЦ БЛОКА -->
    <div id="chat-input-area">
        <div id="reply-preview">
            <div class="reply-info">
                <span class="reply-author" id="reply-author"></span>
                <span class="reply-content" id="reply-content"></span>
            </div>
            <button id="reply-cancel">✕</button>
        </div>
        <div id="chat-input-row">
            <button id="sticker-toggle" class="sticker-btn"><i class="fas fa-smile"></i></button>
            <input type="text" id="chat-input" placeholder="Сообщение..." />
            <label class="file-label" id="chat-file-label"><i class="fas fa-paperclip"></i></label>
            <input type="file" id="chat-file-input" class="file-input" multiple />
            <button id="chat-send"><i class="fas fa-paper-plane"></i></button>
        </div>
    </div>
    <!-- Панель стикеров -->
    <div id="sticker-panel">
        <div id="sticker-search">
            <input type="text" id="sticker-query" placeholder="Поиск стикеров..." />
            <button id="sticker-search-btn" style="padding:8px 12px; background:#005f60; color:white; border:none; border-radius:20px;"><i class="fas fa-search"></i></button>
        </div>
        <div id="sticker-grid"></div>
    </div>
</div>

<div id="toast"></div>

<script>
// ---------- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ----------
let currentEntity = null;
let userId = null;
let userEmail = '';
let myDisplayName = '';
let entityMap = {};
let syncInterval = null;
let emailVisible = true;
let reconnectTimer = null;
let reconnectDots = 0;
let isReconnecting = false;
let longPressTimer = null;
let longPressTarget = null;
let isLongPress = false;
let reactionPanel = document.getElementById('reaction-panel');
let selectedMessageId = null;
let selectedEntityId = null;
let currentMessageReactions = {};
let currentReply = null;
let currentUserEmail = '';
let chatTargets = {};

let stickersList = [];
let stickerCache = {};
let stickerPanelOpen = false;
let botsList = [];

let entitiesInitialized = false;

const eventSource = new EventSource('/api/events');
eventSource.onopen = function() { updateLogo('connected'); };

// ---------- БРАУЗЕРНЫЕ PUSH-УВЕДОМЛЕНИЯ ----------
function requestNotificationPermissionOnce() {
    if (!('Notification' in window)) return;
    if (localStorage.getItem('notif_permission_asked')) return;
    localStorage.setItem('notif_permission_asked', '1');
    if (Notification.permission === 'default') {
        Notification.requestPermission();
    }
}

function showBrowserNotification(title, body, tag) {
    if (!('Notification' in window)) return;
    if (Notification.permission !== 'granted') return;
    try {
        const n = new Notification(title, {
            body: body || '',
            tag: tag || undefined,
            renotify: false
        });
        n.onclick = function() {
            window.focus();
            n.close();
        };
    } catch (e) {
        console.error('Notification error', e);
    }
}

function notifyNewMessage(data) {
    // Не уведомляем о собственных сообщениях
    const fromName = data.from || data.sender || data.display_name || '';
    if (fromName && myDisplayName && fromName === myDisplayName) return;
    // Не уведомляем, если вкладка активна и открыт именно этот чат
    if (document.hasFocus() && currentEntity && data.entity_id === currentEntity) return;

    const entityInfo = entityMap[data.entity_id];
    const chatName = entityInfo ? entityInfo.name : '';
    const title = fromName ? (chatName && chatName !== fromName ? `${fromName} — ${chatName}` : fromName) : (chatName || 'Aerisyn');

    let body = data.content || data.text || data.message || '';
    if (data.type === 'sticker' || (!body && data.message_type === 'sticker')) body = 'Стикер';
    else if (!body) body = 'Новое сообщение';

    showBrowserNotification(title, body, 'aerisyn-msg-' + data.entity_id);
}

function notifyNewChat(name) {
    if (document.hasFocus()) return;
    showBrowserNotification('Новый чат', name || 'Вас добавили в новый чат');
}

const entityList = document.getElementById('entity-list');
const chatScreen = document.getElementById('chat-screen');
const chatMessages = document.getElementById('chat-messages');
const chatInput = document.getElementById('chat-input');
const chatSend = document.getElementById('chat-send');
const chatTitle = document.getElementById('chat-title');
const chatBack = document.getElementById('chat-back');
const chatMenuBtn = document.getElementById('chat-menu-btn');
const chatMenuDropdown = document.getElementById('chat-menu-dropdown');
const menuAdmins = document.getElementById('menu-admins');
const menuInvites = document.getElementById('menu-invites');
const menuLeave = document.getElementById('menu-leave');
const fab = document.getElementById('fab');

const createModal = document.getElementById('create-modal');
const profileModal = document.getElementById('profile-modal');
const profileContent = document.getElementById('profile-content');
const searchModal = document.getElementById('search-modal');
const mediaModal = document.getElementById('media-modal');
const nameModal = document.getElementById('name-modal');
const nameInput = document.getElementById('name-input');
const nameError = document.getElementById('name-error');
const nameSave = document.getElementById('name-save');
const nameCancel = document.getElementById('name-cancel');

const mediaContainer = document.getElementById('media-container');
const mediaInfo = document.getElementById('media-info');
const toast = document.getElementById('toast');
const logo = document.getElementById('logo');

const stickerToggle = document.getElementById('sticker-toggle');
const stickerPanel = document.getElementById('sticker-panel');
const stickerGrid = document.getElementById('sticker-grid');
const stickerQuery = document.getElementById('sticker-query');
const stickerSearchBtn = document.getElementById('sticker-search-btn');

const joinRequestsModal = document.getElementById('join-requests-modal');
const joinRequestsList = document.getElementById('join-requests-list');
const privateToggleCheck = document.getElementById('private-toggle-check');

const settingsModal = document.getElementById('settings-modal');
const settingsUsername = document.getElementById('settings-username');
const settingsUsernameCurrent = document.getElementById('settings-username-current');
const settingsUsernameEdit = document.getElementById('settings-username-edit');
const settingsUsernameInput = document.getElementById('settings-username-input');
const settingsUsernameSave = document.getElementById('settings-username-save');
const settingsUsernameCancel = document.getElementById('settings-username-cancel');
const settingsEmailStatus = document.getElementById('settings-email-status');

// ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------
function showToast(text, type='') {
    toast.textContent = text;
    toast.className = 'show' + (type ? ' ' + type : '');
    clearTimeout(toast._hide);
    toast._hide = setTimeout(() => { toast.className = ''; }, 3000);
}

function escHtml(str) { return String(str).replace(/[&<>"]/g, function(m) { if(m==='&')return'&amp;';if(m==='<')return'&lt;';if(m==='>')return'&gt;';if(m==='"')return'&quot;';return m; }); }
function formatTime(ts) { return new Date(ts * 1000).toLocaleTimeString('ru', {hour:'2-digit', minute:'2-digit'}); }

function updateLogo(state) {
    if (state === 'reconnecting') {
        if (!isReconnecting) {
            isReconnecting = true;
            reconnectDots = 0;
            logo.classList.add('reconnecting');
            clearInterval(reconnectTimer);
            reconnectTimer = setInterval(() => {
                reconnectDots = (reconnectDots % 3) + 1;
                logo.textContent = 'Переподключение' + '.'.repeat(reconnectDots);
            }, 500);
        }
    } else {
        if (isReconnecting) {
            isReconnecting = false;
            clearInterval(reconnectTimer);
            reconnectTimer = null;
            logo.classList.remove('reconnecting');
            logo.textContent = 'Aerisyn';
        }
    }
}

async function apiFetch(url, options = {}) {
    try {
        const resp = await fetch(url, options);
        if (resp.status === 401) {
            window.location.href = '/';
            return null;
        }
        if (!resp.ok) {
            let errorText = `Ошибка ${resp.status}`;
            try {
                const data = await resp.json();
                if (data.reason) errorText = data.reason;
                else if (data.message) errorText = data.message;
            } catch (_) {
                try { const text = await resp.text(); if (text) errorText = text; } catch (_) {}
            }
            updateLogo('reconnecting');
            throw new Error(errorText);
        }
        updateLogo('connected');
        return resp;
    } catch (e) {
        updateLogo('reconnecting');
        throw e;
    }
}

function getFileType(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    const imageExts = ['jpg','jpeg','png','gif','bmp','webp','svg','ico'];
    const videoExts = ['mp4','webm','ogg','mov','avi','mkv','flv','wmv','m4v'];
    const audioExts = ['mp3','wav','flac','aac','ogg','wma','m4a','opus','weba','aiff','alac'];
    if (imageExts.includes(ext)) return 'image';
    if (videoExts.includes(ext)) return 'video';
    if (audioExts.includes(ext)) return 'audio';
    return 'document';
}

async function loadAndDisplayMedia(entityId, msgId, filename, container, isSelf = false) {
    container.innerHTML = '⏳ Загрузка...';
    try {
        const resp = await apiFetch(`/api/download_file?entity_id=${entityId}&message_id=${msgId}`);
        if (!resp) { container.textContent = '⚠️ Ошибка сети'; return; }
        if (!resp.ok) { container.textContent = '⚠️ Ошибка загрузки'; return; }

        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const fileType = getFileType(filename);

        if (fileType === 'audio') {
            let titleText = filename.replace(/\.[^.]+$/, '');
            let artistText = '';
            if (titleText.includes(' - ')) {
                const parts = titleText.split(' - ');
                if (parts.length >= 2) {
                    artistText = parts[0].trim();
                    titleText = parts.slice(1).join(' - ').trim();
                }
            }
            if (!titleText) titleText = 'Аудио';
            const msgBg = isSelf ? '#d4edda' : '#f0f2f5';
            const coverColor = msgBg;
            const player = document.createElement('div');
            player.className = 'audio-player';
            player.style.setProperty('--msg-bg', msgBg);
            player.style.setProperty('--cover-color', coverColor);
            const row = document.createElement('div');
            row.className = 'row';
            const cover = document.createElement('div');
            cover.className = 'cover';
            cover.style.background = coverColor;
            const playBtn = document.createElement('button');
            playBtn.className = 'play-btn';
            playBtn.textContent = '▶';
            cover.appendChild(playBtn);
            const info = document.createElement('div');
            info.className = 'info';
            const title = document.createElement('div');
            title.className = 'title';
            title.textContent = titleText;
            const artist = document.createElement('div');
            artist.className = 'artist';
            if (artistText) {
                artist.textContent = artistText;
            }
            info.appendChild(title);
            info.appendChild(artist);
            row.appendChild(cover);
            row.appendChild(info);
            const progress = document.createElement('input');
            progress.type = 'range';
            progress.className = 'progress';
            progress.min = 0;
            progress.max = 100;
            progress.value = 0;
            player.appendChild(row);
            player.appendChild(progress);
            container.innerHTML = '';
            container.appendChild(player);
            const audio = new Audio(url);
            let playing = false;
            playBtn.addEventListener('click', () => {
                if (playing) {
                    audio.pause();
                    playBtn.textContent = '▶';
                    playing = false;
                } else {
                    audio.play().catch(() => {});
                    playBtn.textContent = '⏸';
                    playing = true;
                }
            });
            audio.addEventListener('timeupdate', () => {
                if (audio.duration) {
                    progress.value = (audio.currentTime / audio.duration) * 100;
                }
            });
            progress.addEventListener('input', () => {
                if (audio.duration) {
                    audio.currentTime = (progress.value / 100) * audio.duration;
                }
            });
            audio.addEventListener('ended', () => {
                playBtn.textContent = '▶';
                playing = false;
                progress.value = 0;
            });
            return;
        }

        let el;
        if (fileType === 'image') {
            el = document.createElement('img');
            el.src = url;
            el.alt = filename;
            el.style.maxWidth = '100%';
            el.style.maxHeight = '300px';
            el.style.borderRadius = '12px';
            el.style.display = 'block';
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                viewMedia(entityId, msgId, filename, url);
            });
        } else if (fileType === 'video') {
            el = document.createElement('video');
            el.src = url;
            el.controls = true;
            el.style.maxWidth = '100%';
            el.style.maxHeight = '400px';
            el.style.borderRadius = '12px';
            el.style.background = '#000';
            el.autoplay = false;
        } else {
            el = document.createElement('a');
            el.href = url;
            el.download = filename;
            el.innerHTML = '<i class="fas fa-paperclip"></i> ' + filename;
            el.className = 'file-download';
            el.style.display = 'inline-flex';
            el.style.alignItems = 'center';
            el.style.gap = '8px';
            el.style.padding = '4px 8px';
            el.style.background = 'rgba(0,0,0,0.04)';
            el.style.borderRadius = '12px';
            el.style.textDecoration = 'none';
            el.style.color = '#005f60';
        }
        container.innerHTML = '';
        container.appendChild(el);

    } catch (e) {
        container.textContent = '⚠️ Ошибка: ' + e.message;
    }
}

async function viewMedia(entityId, msgId, filename, existingUrl = null) {
    try {
        let url = existingUrl;
        if (!url) {
            const resp = await apiFetch(`/api/download_file?entity_id=${entityId}&message_id=${msgId}`);
            if (!resp) return;
            if (!resp.ok) throw new Error('Ошибка загрузки');
            const blob = await resp.blob();
            url = URL.createObjectURL(blob);
        }
        const fileType = getFileType(filename);
        let html = '';
        if (fileType === 'image') {
            html = `<img src="${url}" alt="${escHtml(filename)}" />`;
        } else if (fileType === 'video') {
            html = `<video controls autoplay src="${url}"></video>`;
        } else {
            html = `<a href="${url}" download="${escHtml(filename)}">Скачать ${escHtml(filename)}</a>`;
        }
        mediaContainer.innerHTML = html;
        mediaInfo.textContent = filename;
        mediaModal.classList.add('active');
        const cleanup = () => {
            if (!existingUrl) URL.revokeObjectURL(url);
            mediaModal.removeEventListener('click', cleanup);
            document.getElementById('media-close').removeEventListener('click', cleanup);
        };
        mediaModal.addEventListener('click', cleanup);
        document.getElementById('media-close').addEventListener('click', cleanup);
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

// ---------- СТИКЕРЫ ----------
async function loadStickerIntoImage(stickerId, imgElement) {
    try {
        let dataUrl = stickerCache[stickerId];
        if (!dataUrl) {
            const resp = await apiFetch(`/api/sticker/${stickerId}`);
            if (!resp) return;
            if (!resp.ok) {
                imgElement.alt = 'Ошибка загрузки';
                return;
            }
            const blob = await resp.blob();
            dataUrl = URL.createObjectURL(blob);
            stickerCache[stickerId] = dataUrl;
        }
        imgElement.src = dataUrl;
    } catch(e) {
        imgElement.alt = 'Ошибка: ' + e.message;
    }
}

async function viewSticker(stickerId) {
    try {
        let dataUrl = stickerCache[stickerId];
        if (!dataUrl) {
            const resp = await apiFetch(`/api/sticker/${stickerId}`);
            if (!resp) return;
            if (!resp.ok) throw new Error('Ошибка загрузки');
            const blob = await resp.blob();
            dataUrl = URL.createObjectURL(blob);
            stickerCache[stickerId] = dataUrl;
        }
        mediaContainer.innerHTML = `<img src="${dataUrl}" style="max-width: 100%; max-height: 80vh;" />`;
        mediaInfo.textContent = 'Стикер';
        mediaModal.classList.add('active');
        const cleanup = () => {
            mediaModal.removeEventListener('click', cleanup);
            document.getElementById('media-close').removeEventListener('click', cleanup);
        };
        mediaModal.addEventListener('click', cleanup);
        document.getElementById('media-close').addEventListener('click', cleanup);
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

// ---------- ОТОБРАЖЕНИЕ ПРЕВЬЮ ОТВЕТА ----------
function showReplyPreview(reply) {
    const preview = document.getElementById('reply-preview');
    document.getElementById('reply-author').textContent = reply.from;
    document.getElementById('reply-content').textContent = reply.content;
    preview.style.display = 'flex';
}

// ---------- ПРОФИЛЬ ----------
function renderProfile(data, isOwn) {
    let html = '';
    const initial = (data.first_name || '?').trim().charAt(0).toUpperCase();
    html += `<div class="profile-avatar">${data.is_bot ? '<i class="fas fa-robot"></i>' : escHtml(initial)}</div>`;
    html += `<div class="profile-name">${escHtml(data.first_name || '')}</div>`;
    html += `<div class="profile-sub">${data.is_bot ? 'Бот' : (isOwn ? 'Ваш профиль' : 'Профиль')}</div>`;

    const emailDisplay = (isOwn || (data.show_email !== false && data.email)) ? data.email : 'Скрыт';
    html += `<div class="icon-row">
                <div class="row-icon"><i class="fas fa-envelope"></i></div>
                <div class="row-body">
                    <div class="row-title">${escHtml(emailDisplay)}</div>
                    <div class="row-sub">почта</div>
                </div>
             </div>`;

    const bioText = data.bio || 'Не указано';
    html += `<div class="icon-row">
                <div class="row-icon"><i class="fas fa-align-left"></i></div>
                <div class="row-body">
                    <div class="row-title">${escHtml(bioText)}</div>
                    <div class="row-sub">о себе</div>
                </div>
             </div>`;

    if (data.username) {
        html += `<div class="icon-row">
                    <div class="row-icon"><i class="fas fa-at"></i></div>
                    <div class="row-body">
                        <div class="row-title">@${escHtml(data.username)}</div>
                        <div class="row-sub">юзернейм</div>
                    </div>
                 </div>`;
    }

    if (isOwn) {
        html += `<div class="profile-buttons">
                    <button id="profile-bots-btn">Боты</button>
                    <button id="profile-settings-btn">Настройки</button>
                    <button class="danger" id="profile-logout-btn">Выйти</button>
                </div>`;
    }

    profileContent.innerHTML = html;

    if (isOwn) {
        document.getElementById('profile-bots-btn')?.addEventListener('click', showBots);
        document.getElementById('profile-settings-btn')?.addEventListener('click', () => {
            profileModal.classList.remove('active');
            openSettings();
        });
        document.getElementById('profile-logout-btn')?.addEventListener('click', () => {
            if (confirm('Выйти?')) {
                fetch('/api/logout', { method: 'POST' }).then(() => { window.location.href = '/'; });
            }
        });
    }
}

async function loadMyProfile() {
    try {
        const resp = await apiFetch('/api/profile');
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const info = data.data;
        emailVisible = info.show_email !== undefined ? info.show_email : true;
        renderProfile(info, true);
        currentUserEmail = info.email;
        updateSettingsUI(info);
    } catch(e) {
        profileContent.innerHTML = 'Ошибка загрузки профиля: ' + e.message;
        showToast('Ошибка загрузки профиля', 'error');
    }
}

async function loadUserProfile(identifier) {
    try {
        const resp = await apiFetch(`/api/user_info/${encodeURIComponent(identifier)}`);
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const info = data.data;
        const isOwn = (info.email === currentUserEmail);
        renderProfile(info, isOwn);
    } catch(e) {
        profileContent.innerHTML = 'Ошибка загрузки профиля: ' + e.message;
        showToast('Ошибка загрузки профиля', 'error');
    }
}

// ---------- НАСТРОЙКИ ----------
function openSettings() {
    settingsModal.classList.add('active');
    updateSettingsUI(null);
    settingsUsernameEdit.style.display = 'none';
}

function updateSettingsUI(info) {
    if (!info) {
        apiFetch('/api/profile').then(resp => {
            if (resp && resp.ok) resp.json().then(data => {
                if (data.status === 'ok') {
                    const d = data.data;
                    settingsUsernameCurrent.textContent = d.username || 'не установлен';
                    settingsEmailStatus.textContent = d.show_email !== false ? 'вкл' : 'выкл';
                }
            });
        });
        return;
    }
    settingsUsernameCurrent.textContent = info.username || 'не установлен';
    settingsEmailStatus.textContent = info.show_email !== false ? 'вкл' : 'выкл';
}

settingsUsername.addEventListener('click', function() {
    settingsUsernameEdit.style.display = 'block';
    settingsUsernameInput.value = settingsUsernameCurrent.textContent === 'не установлен' ? '' : settingsUsernameCurrent.textContent;
    settingsUsernameInput.focus();
});

settingsUsernameCancel.addEventListener('click', function() {
    settingsUsernameEdit.style.display = 'none';
});

settingsUsernameSave.addEventListener('click', async function() {
    const val = settingsUsernameInput.value.trim();
    try {
        const resp = await apiFetch('/api/set_username', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: val })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Юзернейм обновлён', 'success');
            settingsUsernameCurrent.textContent = val || 'не установлен';
            settingsUsernameEdit.style.display = 'none';
            await loadMyProfile();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
});

settingsEmailStatus.parentElement?.addEventListener('click', async function() {
    const current = settingsEmailStatus.textContent === 'вкл';
    const newVal = !current;
    try {
        const resp = await apiFetch('/api/set_show_email', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ show: newVal })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Настройка обновлена', 'success');
            settingsEmailStatus.textContent = newVal ? 'вкл' : 'выкл';
            emailVisible = newVal;
            await loadMyProfile();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
});

// ---------- ЗАГРУЗКА СООБЩЕНИЙ ----------
async function loadMessages(entityId) {
    const resp = await apiFetch(`/api/messages/${entityId}?limit=50`);
    if (!resp) return;
    if (resp.status === 401) { window.location.href = '/'; return; }
    const data = await resp.json();
    if (data.status === 'error') throw new Error(data.reason);
    const msgs = data.messages || [];
    chatMessages.innerHTML = '';
    if (msgs.length === 0) {
        chatMessages.innerHTML = '<div style="color:#aaa; text-align:center; padding:40px 0;">Нет сообщений</div>';
        return;
    }
    msgs.forEach(msg => {
        // --- СТИКЕРЫ ---
        if (msg.type === 'sticker') {
            const div = document.createElement('div');
            div.className = 'sticker-message';
            div.dataset.messageId = msg.id;
            const isSelf = (msg.from === myDisplayName);
            if (isSelf) div.classList.add('self');

            const meta = document.createElement('div');
            meta.className = 'meta';
            const fromName = escHtml(msg.from);
            meta.innerHTML = `<span class="from-name" data-identifier="${escHtml(msg.from_email || '')}">${fromName}</span> <span class="time">${formatTime(msg.timestamp)}</span>`;
            div.appendChild(meta);

            const container = document.createElement('div');
            container.className = 'sticker-container';
            const img = document.createElement('img');
            img.alt = 'Стикер';
            img.loading = 'lazy';
            container.appendChild(img);
            div.appendChild(container);

            if (isSelf) {
                const del = document.createElement('button');
                del.className = 'delete-msg';
                del.textContent = '✕';
                del.title = 'Удалить у всех';
                del.addEventListener('click', (e) => {
                    e.stopPropagation();
                    deleteMessage(entityId, msg.id);
                });
                container.appendChild(del);
            }

            let stickerId = msg.sticker_id || 0;
            if (!stickerId && msg.filename && msg.filename.startsWith('sticker_')) {
                const parts = msg.filename.split('_');
                if (parts.length > 1) {
                    const idPart = parts[1].split('.')[0];
                    stickerId = parseInt(idPart) || 0;
                }
            }
            loadStickerIntoImage(stickerId, img);
            container.addEventListener('click', () => viewSticker(stickerId));

            if (msg.reply_markup) {
                const btnContainer = renderButtons(entityId, msg.id, msg.reply_markup);
                div.appendChild(btnContainer);
            }

            chatMessages.appendChild(div);

            meta.querySelector('.from-name')?.addEventListener('click', function(e) {
                e.stopPropagation();
                const ident = this.dataset.identifier;
                if (ident) openUserProfile(ident);
            });

            if (msg.reactions) {
                currentMessageReactions[msg.id] = msg.reactions;
                renderReactions(msg.id, msg.reactions);
            } else {
                if (!currentMessageReactions[msg.id]) {
                    currentMessageReactions[msg.id] = { counts: {}, my_reaction: null };
                }
            }
            setupLongPress(div);
            return;
        }

        // --- ОБЫЧНЫЕ СООБЩЕНИЯ (текст, файл, опрос) ---
        const div = document.createElement('div');
        div.className = 'message';
        div.dataset.messageId = msg.id;
        const isSelf = (msg.from === myDisplayName);
        if (isSelf) div.classList.add('self');

        const meta = document.createElement('div');
        meta.className = 'meta';
        const fromName = escHtml(msg.from);
        meta.innerHTML = `<span class="from-name" data-identifier="${escHtml(msg.from_email || '')}">${fromName}</span> <span class="time">${formatTime(msg.timestamp)}</span>`;
        div.appendChild(meta);

        const bubble = document.createElement('div');
        bubble.className = 'bubble';

        if (msg.reply_to) {
            const replyDiv = document.createElement('div');
            replyDiv.className = 'reply-preview';
            if (msg.reply_to.deleted) {
                replyDiv.innerHTML = `<span style="color:#999;">Сообщение удалено</span>`;
            } else {
                replyDiv.innerHTML = `
                    <span class="reply-author">${escHtml(msg.reply_to.from)}</span>
                    <span class="reply-content">${escHtml(msg.reply_to.content)}</span>
                `;
            }
            bubble.appendChild(replyDiv);
        }

        if (isSelf) {
            const del = document.createElement('button');
            del.className = 'delete-msg';
            del.textContent = '✕';
            del.title = 'Удалить у всех';
            del.addEventListener('click', (e) => {
                e.stopPropagation();
                deleteMessage(entityId, msg.id);
            });
            bubble.appendChild(del);
        }

        // --- ОБРАБОТКА ТИПОВ СООБЩЕНИЙ ---
        if (msg.type === 'file') {
            const mediaContainer = document.createElement('div');
            mediaContainer.className = 'media-container';
            mediaContainer.textContent = '⏳ Загрузка...';
            bubble.appendChild(mediaContainer);
            loadAndDisplayMedia(entityId, msg.id, msg.content, mediaContainer);
        } else if (msg.type === 'poll') {
    const pollData = msg.poll || {};
    const question = pollData.question || msg.content;
    const options = pollData.options || [];
    const results = pollData.results || { percentages: [] };
    const totalVotes = pollData.total_votes || 0;
    const isClosed = pollData.is_closed || false;
    const userVote = pollData.user_vote; // null или индекс

    // Заголовок опроса
    const qDiv = document.createElement('div');
    qDiv.style.fontWeight = 'bold';
    qDiv.textContent = question;
    bubble.appendChild(qDiv);

    // Варианты ответа
    if (options.length === 0) {
        const empty = document.createElement('div');
        empty.textContent = 'Нет вариантов';
        empty.style.color = '#999';
        bubble.appendChild(empty);
    } else {
        options.forEach((opt, idx) => {
            const row = document.createElement('div');
            row.style.marginTop = '6px';

            // Если опрос закрыт или пользователь уже голосовал – показываем проценты
            if (isClosed || userVote !== null) {
                const pct = (results && results.percentages && results.percentages.length > idx) ? results.percentages[idx] : 0;
                const bar = document.createElement('div');
                bar.className = 'poll-result-bar';
                const label = document.createElement('span');
                label.textContent = opt;
                label.style.width = '120px';
                label.style.flexShrink = '0';
                const barWrap = document.createElement('div');
                barWrap.className = 'bar';
                const fill = document.createElement('div');
                fill.className = 'fill';
                fill.style.width = pct + '%';
                barWrap.appendChild(fill);
                const pctSpan = document.createElement('span');
                pctSpan.className = 'pct';
                pctSpan.textContent = pct + '%';
                bar.appendChild(label);
                bar.appendChild(barWrap);
                bar.appendChild(pctSpan);
                row.appendChild(bar);
            } else {
                // Радиокнопка с явными стилями
                const label = document.createElement('label');
                label.className = 'poll-option-label';
                label.style.display = 'flex';
                label.style.alignItems = 'center';
                label.style.gap = '10px';
                label.style.cursor = 'pointer';
                label.style.padding = '4px 0';
                label.style.fontSize = '15px';

                const radio = document.createElement('input');
                radio.type = 'radio';
                radio.name = 'poll_' + pollData.id;
                radio.value = idx;
                radio.style.width = '20px';
                radio.style.height = '20px';
                radio.style.flexShrink = '0';
                radio.style.cursor = 'pointer';
                radio.style.accentColor = '#005f60'; // для современных браузеров

                const textSpan = document.createElement('span');
                textSpan.textContent = opt;

                label.appendChild(radio);
                label.appendChild(textSpan);
                row.appendChild(label);
            }
            bubble.appendChild(row);
        });
    }

    // Кнопка "Голосовать"
    if (!isClosed && userVote === null && options.length > 0) {
        const voteBtn = document.createElement('button');
        voteBtn.className = 'poll-vote-btn';
        voteBtn.textContent = 'Голосовать';
        voteBtn.style.marginTop = '8px';
        voteBtn.style.padding = '8px 20px';
        voteBtn.style.border = 'none';
        voteBtn.style.borderRadius = '20px';
        voteBtn.style.background = '#005f60';
        voteBtn.style.color = 'white';
        voteBtn.style.fontSize = '15px';
        voteBtn.style.cursor = 'pointer';
        voteBtn.addEventListener('click', function() {
            const selected = document.querySelector(`input[name="poll_${pollData.id}"]:checked`);
            if (!selected) {
                showToast('Выберите вариант', 'error');
                return;
            }
            const idx = parseInt(selected.value);
            votePoll(pollData.id, idx);
        });
        bubble.appendChild(voteBtn);
    }

    // Кнопка "Завершить" для создателя
    if (pollData.created_by === currentUserEmail && !isClosed) {
        const closeBtn = document.createElement('button');
        closeBtn.className = 'poll-close-btn';
        closeBtn.textContent = 'Завершить';
        closeBtn.style.marginTop = '8px';
        closeBtn.style.marginLeft = '8px';
        closeBtn.style.padding = '6px 16px';
        closeBtn.style.border = 'none';
        closeBtn.style.borderRadius = '16px';
        closeBtn.style.background = '#c62828';
        closeBtn.style.color = 'white';
        closeBtn.style.cursor = 'pointer';
        closeBtn.addEventListener('click', function() { closePoll(pollData.id); });
        bubble.appendChild(closeBtn);
    }

    if (isClosed) {
        const closedLabel = document.createElement('div');
        closedLabel.style.fontSize = '12px';
        closedLabel.style.color = '#888';
        closedLabel.style.marginTop = '6px';
        closedLabel.textContent = '🔒 Опрос завершён';
        bubble.appendChild(closedLabel);
    }
}
        
        else {
            // Текстовое сообщение
            const contentSpan = document.createElement('span');
            contentSpan.textContent = msg.content;
            bubble.appendChild(contentSpan);
        }

        div.appendChild(bubble);

        // --- ИНЛАЙН-КНОПКИ (для любых сообщений) ---
        if (msg.reply_markup) {
            const btnContainer = renderButtons(entityId, msg.id, msg.reply_markup);
            div.appendChild(btnContainer);
        }

        chatMessages.appendChild(div);

        meta.querySelector('.from-name')?.addEventListener('click', function(e) {
            e.stopPropagation();
            const ident = this.dataset.identifier;
            if (ident) openUserProfile(ident);
        });

        if (msg.reactions) {
            currentMessageReactions[msg.id] = msg.reactions;
            renderReactions(msg.id, msg.reactions);
        } else {
            if (!currentMessageReactions[msg.id]) {
                currentMessageReactions[msg.id] = { counts: {}, my_reaction: null };
            }
        }

        let touchStartX = 0;
        div.addEventListener('touchstart', (e) => {
            touchStartX = e.touches[0].clientX;
        }, { passive: true });
        div.addEventListener('touchmove', (e) => {
            const deltaX = e.touches[0].clientX - touchStartX;
            if (deltaX < -50) {
                e.preventDefault();
                const from = msg.from;
                const content = msg.type === 'file' ? 'Файл: ' + msg.content : msg.content;
                currentReply = {
                    entity_id: currentEntity,
                    message_id: msg.id,
                    from: from,
                    content: content
                };
                showReplyPreview(currentReply);
                if (navigator.vibrate) navigator.vibrate(20);
            }
        }, { passive: false });

        setupLongPress(div);
    });
    const lastMsg = chatMessages.lastElementChild;
    if (lastMsg) {
        lastMsg.scrollIntoView({ block: 'end' });
    }
}

// ---------- РЕАКЦИИ ----------
function renderReactions(messageId, reactionsData) {
    const msgElement = document.querySelector(`.message[data-message-id="${messageId}"], .sticker-message[data-message-id="${messageId}"]`);
    if (!msgElement) return;
    const existing = msgElement.querySelector('.reactions');
    if (existing) existing.remove();

    const counts = reactionsData.counts || {};
    const myReaction = reactionsData.my_reaction || null;
    const reactionTypes = Object.keys(counts);
    if (reactionTypes.length === 0) return;

    const container = document.createElement('div');
    container.className = 'reactions';

    reactionTypes.forEach(rt => {
        const count = counts[rt];
        const isActive = (myReaction === rt);
        const badge = document.createElement('span');
        badge.className = 'reaction-badge' + (isActive ? ' active' : '');
        badge.innerHTML = `${rt} <span class="count">${count}</span>`;
        badge.dataset.reaction = rt;
        badge.addEventListener('click', (e) => {
            e.stopPropagation();
            toggleReaction(messageId, rt);
        });
        container.appendChild(badge);
    });

    msgElement.appendChild(container);
}

async function toggleReaction(messageId, reactionType) {
    if (!currentEntity) return;
    if (!currentMessageReactions[messageId]) {
        currentMessageReactions[messageId] = { counts: {}, my_reaction: null };
    }

    try {
        const msgElement = document.querySelector(`.message[data-message-id="${messageId}"], .sticker-message[data-message-id="${messageId}"]`);
        let currentReaction = null;
        if (msgElement) {
            const activeBadge = msgElement.querySelector('.reaction-badge.active');
            if (activeBadge) {
                currentReaction = activeBadge.dataset.reaction;
            }
        }

        if (currentReaction === reactionType) {
            const resp = await apiFetch('/api/remove_reaction', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message_id: messageId,
                    entity_id: currentEntity
                })
            });
            if (!resp) return;
            const data = await resp.json();
            if (data.status === 'ok') {
                const counts = currentMessageReactions[messageId].counts || {};
                if (counts[reactionType]) {
                    counts[reactionType]--;
                    if (counts[reactionType] <= 0) delete counts[reactionType];
                }
                currentMessageReactions[messageId].my_reaction = null;
                renderReactions(messageId, currentMessageReactions[messageId]);
            } else {
                showToast('Ошибка: ' + data.reason, 'error');
            }
        } else {
            const resp = await apiFetch('/api/add_reaction', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    message_id: messageId,
                    reaction_type: reactionType,
                    entity_id: currentEntity
                })
            });
            if (!resp) return;
            const data = await resp.json();
            if (data.status === 'ok') {
                const counts = currentMessageReactions[messageId].counts || {};
                const oldReaction = currentMessageReactions[messageId].my_reaction;
                if (oldReaction && counts[oldReaction]) {
                    counts[oldReaction]--;
                    if (counts[oldReaction] <= 0) delete counts[oldReaction];
                }
                counts[reactionType] = (counts[reactionType] || 0) + 1;
                currentMessageReactions[messageId].my_reaction = reactionType;
                renderReactions(messageId, currentMessageReactions[messageId]);
            } else {
                showToast('Ошибка: ' + data.reason, 'error');
            }
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

// ---------- ДОЛГОЕ НАЖАТИЕ ----------
function setupLongPress(element) {
    element.addEventListener('touchstart', (e) => {
        longPressTarget = e.currentTarget;
        isLongPress = false;
        longPressTimer = setTimeout(() => {
            isLongPress = true;
            const msgId = parseInt(longPressTarget.dataset.messageId);
            if (msgId) {
                showReactionPanel(e, msgId);
            }
        }, 600);
    }, { passive: true });

    element.addEventListener('touchmove', (e) => {
        if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
    }, { passive: true });

    element.addEventListener('touchend', (e) => {
        if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
        if (isLongPress) { e.preventDefault(); isLongPress = false; }
    }, { passive: false });

    element.addEventListener('mouseup', (e) => {
        if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
        if (isLongPress) { e.preventDefault(); isLongPress = false; }
    });

    element.addEventListener('mouseleave', (e) => {
        if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null; }
        if (isLongPress) { isLongPress = false; hideReactionPanel(); }
    });
}

function showReactionPanel(event, messageId) {
    selectedMessageId = messageId;
    selectedEntityId = currentEntity;
    const panel = reactionPanel;
    panel.classList.add('active');

    const rect = event.currentTarget.getBoundingClientRect();
    let top = rect.top - 70;
    if (top < 20) top = rect.bottom + 10;
    panel.style.bottom = 'auto';
    panel.style.top = top + 'px';
    panel.style.left = '50%';
    panel.style.transform = 'translateX(-50%)';
}

function hideReactionPanel() {
    reactionPanel.classList.remove('active');
    selectedMessageId = null;
    selectedEntityId = null;
}

document.querySelectorAll('#reaction-panel .reaction-emoji').forEach(el => {
    el.addEventListener('click', (e) => {
        e.stopPropagation();
        const reaction = el.dataset.reaction;
        if (selectedMessageId && selectedEntityId) {
            toggleReaction(selectedMessageId, reaction);
            hideReactionPanel();
        }
    });
    el.addEventListener('touchstart', (e) => { e.stopPropagation(); }, { passive: true });
});

document.addEventListener('click', (e) => {
    if (!reactionPanel.contains(e.target) && !e.target.closest('.message') && !e.target.closest('.sticker-message')) {
        hideReactionPanel();
    }
});

document.addEventListener('touchstart', (e) => {
    if (!reactionPanel.contains(e.target) && !e.target.closest('.message') && !e.target.closest('.sticker-message')) {
        hideReactionPanel();
    }
}, { passive: true });

// ---------- ФУНКЦИИ ОПРОСОВ ----------
async function votePoll(pollId, optionIndex) {
    try {
        const resp = await apiFetch('/api/vote_poll', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ poll_id: pollId, option_index: optionIndex })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            await loadMessages(currentEntity);
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

async function closePoll(pollId) {
    if (!confirm('Завершить опрос?')) return;
    try {
        const resp = await apiFetch('/api/close_poll', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ poll_id: pollId })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            await loadMessages(currentEntity);
            showToast('Опрос завершён', 'success');
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

// ---------- ОТПРАВКА СООБЩЕНИЙ, ФАЙЛОВ, СТИКЕРОВ ----------
async function sendMessage() {
    const text = chatInput.value.trim();
    if (!text || !currentEntity) return;
    try {
        const payload = {
            entity_id: currentEntity,
            content: text
        };
        if (currentReply) {
            payload.reply_to = {
                entity_id: currentReply.entity_id,
                message_id: currentReply.message_id
            };
        }
        const resp = await apiFetch('/api/send', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            chatInput.value = '';
            currentReply = null;
            document.getElementById('reply-preview').style.display = 'none';
            await loadMessages(currentEntity);
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка отправки: ' + e.message, 'error'); }
}

async function deleteMessage(entityId, msgId) {
    if (!confirm('Удалить сообщение у всех?')) return;
    try {
        const resp = await apiFetch('/api/delete_message', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: entityId, message_id: msgId })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            await loadMessages(entityId);
            showToast('Сообщение удалено', 'success');
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

window.downloadFile = async function(entityId, msgId) {
    try {
        const resp = await apiFetch(`/api/download_file?entity_id=${entityId}&message_id=${msgId}`);
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        if (!resp.ok) { showToast('Ошибка загрузки файла', 'error'); return; }
        const blob = await resp.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = resp.headers.get('content-disposition')?.split('filename=')[1] || 'file';
        a.click();
        window.URL.revokeObjectURL(url);
        showToast('Файл скачан', 'success');
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
};

async function uploadFiles() {
    if (!currentEntity) return;
    const files = document.getElementById('chat-file-input').files;
    if (files.length === 0) return;
    for (let file of files) {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('entity_id', currentEntity);
        try {
            const resp = await apiFetch('/api/upload_file', {
                method: 'POST',
                body: formData
            });
            if (!resp) return;
            if (resp.status === 401) { window.location.href = '/'; return; }
            const data = await resp.json();
            if (data.status !== 'ok') {
                showToast('Ошибка загрузки: ' + data.reason, 'error');
            } else {
                showToast('Файл отправлен', 'success');
            }
        } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
    }
    document.getElementById('chat-file-input').value = '';
    await loadMessages(currentEntity);
}

// ---------- СТИКЕРЫ: ПАНЕЛЬ И ОТПРАВКА ----------
async function loadStickers(query = '') {
    try {
        const resp = await apiFetch(`/api/stickers?query=${encodeURIComponent(query)}`);
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            stickersList = data.stickers || [];
            renderStickers(stickersList);
        } else {
            showToast('Ошибка загрузки стикеров: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка загрузки стикеров: ' + e.message, 'error');
    }
}

function renderStickers(stickers) {
    stickerGrid.innerHTML = '';
    if (stickers.length === 0) {
        stickerGrid.innerHTML = '<div style="grid-column:1/-1; text-align:center; color:#999; padding:20px;">Стикеры не найдены</div>';
        return;
    }
    stickers.forEach(st => {
        const div = document.createElement('div');
        div.className = 'sticker-item';
        div.dataset.id = st.id;
        const img = document.createElement('img');
        img.alt = st.name;
        const loadImg = async () => {
            try {
                let dataUrl = stickerCache[st.id];
                if (!dataUrl) {
                    const resp = await apiFetch(`/api/sticker/${st.id}`);
                    if (resp && resp.ok) {
                        const blob = await resp.blob();
                        dataUrl = URL.createObjectURL(blob);
                        stickerCache[st.id] = dataUrl;
                    }
                }
                if (dataUrl) img.src = dataUrl;
            } catch(e) {
                img.alt = '?';
            }
        };
        loadImg();
        div.appendChild(img);
        div.addEventListener('click', () => {
            if (currentEntity) {
                sendSticker(st.id);
            } else {
                showToast('Сначала откройте чат', 'error');
            }
        });
        stickerGrid.appendChild(div);
    });
}

async function sendSticker(stickerId) {
    if (!currentEntity) return;
    try {
        const resp = await apiFetch('/api/send_sticker', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: currentEntity, sticker_id: stickerId })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            closeStickerPanel();
            await loadMessages(currentEntity);
        } else {
            showToast('Ошибка отправки стикера: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

function toggleStickerPanel() {
    if (!currentEntity) {
        showToast('Сначала откройте чат', 'error');
        return;
    }
    stickerPanelOpen = !stickerPanelOpen;
    if (stickerPanelOpen) {
        stickerPanel.classList.add('open');
        stickerToggle.classList.add('active');
        stickerToggle.innerHTML = '<i class="fas fa-keyboard"></i>';
        if (stickersList.length === 0) {
            loadStickers();
        }
        setTimeout(() => stickerQuery.focus(), 100);
    } else {
        closeStickerPanel();
    }
}

function closeStickerPanel() {
    stickerPanel.classList.remove('open');
    stickerToggle.classList.remove('active');
    stickerToggle.innerHTML = '<i class="fas fa-smile"></i>';
    stickerPanelOpen = false;
}

stickerSearchBtn.addEventListener('click', () => {
    const query = stickerQuery.value.trim();
    loadStickers(query);
});
stickerQuery.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        stickerSearchBtn.click();
    }
});
stickerToggle.addEventListener('click', toggleStickerPanel);

document.addEventListener('click', (e) => {
    if (stickerPanelOpen) {
        if (!stickerPanel.contains(e.target) && e.target !== stickerToggle) {
            closeStickerPanel();
        }
    }
});

// ---------- УПРАВЛЕНИЕ ЧАТОМ ----------
async function createEntity(type, name, username, target) {
    try {
        const payload = { type, name };
        if (username) payload.username = username;
        if (target) payload.target = target;
        const resp = await apiFetch('/api/create_entity', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            if (type === 'chat' && data.data && data.data.target) {
                chatTargets[data.data.entity_id] = data.data.target;
            }
            await loadEntities();
            if (data.data && data.data.entity_id) {
                openChat(data.data.entity_id);
            }
            showToast('Создано!', 'success');
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

async function joinEntityById(id) {
    try {
        const resp = await apiFetch('/api/join', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: id })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            if (data.data && data.data.status === 'request_submitted') {
                showToast('Заявка отправлена, ожидайте подтверждения', 'info');
            } else {
                await loadEntities();
                openChat(id);
                showToast('Вступление успешно', 'success');
            }
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

async function leaveEntity() {
    if (!currentEntity) return;
    if (!confirm('Покинуть сущность?')) return;
    try {
        const resp = await apiFetch('/api/leave', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: currentEntity })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            closeChat();
            await loadEntities();
            showToast('Вы покинули сущность', 'success');
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

function closeChat() {
    chatScreen.classList.remove('open');
    currentEntity = null;
    chatInput.disabled = true;
    chatSend.disabled = true;
    chatMenuDropdown.classList.remove('show');
    menuAdmins.style.display = 'none';
    menuInvites.style.display = 'none';
    hideReactionPanel();
    closeStickerPanel();
    currentReply = null;
    document.getElementById('reply-preview').style.display = 'none';
    // Сброс видимости элементов чата
    const messagesDiv = document.getElementById('chat-messages');
    const inputArea = document.getElementById('chat-input-area');
    const joinPrompt = document.getElementById('join-prompt');
    if (messagesDiv) messagesDiv.style.display = 'block';
    if (inputArea) inputArea.style.display = 'flex';
    if (joinPrompt) joinPrompt.style.display = 'none';
}

async function openChat(entityId) {
    try {
        const resp = await apiFetch(`/api/entity_info/${entityId}`);
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') {
            if (data.reason && (data.reason.includes('private') || data.reason.includes('Not a member'))) {
                showToast('Нет доступа к этой сущности', 'error');
                closeChat();
            } else {
                showToast('Ошибка: ' + data.reason, 'error');
                closeChat();
            }
            return;
        }

        const info = {
            name: data.name,
            type: data.type,
            username: data.username || '',
            owner: data.owner || '',
            is_private: data.is_private || false,
            is_member: data.is_member || false,
        };
        if (data.target) chatTargets[entityId] = data.target;
        entityMap[entityId] = info;
        currentEntity = entityId;
        chatTitle.textContent = info.name;
        chatTitle.onclick = function() {
            if (info.type === 'chat') {
                const target = chatTargets[entityId];
                if (target) {
                    openUserProfile(target);
                } else {
                    showToast('Не удалось определить собеседника', 'error');
                }
            } else {
                showToast('Информация о группе/канале пока не поддерживается', 'info');
            }
        };

        const messagesDiv = document.getElementById('chat-messages');
        const inputArea = document.getElementById('chat-input-area');
        const joinPrompt = document.getElementById('join-prompt');

        if (!messagesDiv || !inputArea || !joinPrompt) {
            console.error('Missing chat elements');
            return;
        }

        if (!info.is_member) {
            // Не участник — показываем кнопку "Вступить"
            messagesDiv.style.display = 'none';
            inputArea.style.display = 'none';
            joinPrompt.style.display = 'flex';
            chatMessages.innerHTML = '';
            closeStickerPanel();
            const joinBtn = document.getElementById('join-entity-btn');
            if (joinBtn) {
                joinBtn.onclick = function() { joinEntityById(entityId); };
            }
        } else {
            // Участник — показываем интерфейс
            messagesDiv.style.display = 'block';
            inputArea.style.display = 'flex';
            joinPrompt.style.display = 'none';
            chatInput.disabled = false;
            chatSend.disabled = false;
            await loadMessages(entityId);
        }

        updateChatMenu();
        updateAnalyticsMenu();
        chatScreen.classList.add('open');
        chatMenuDropdown.classList.remove('show');

    } catch (e) {
        showToast('Ошибка загрузки: ' + e.message, 'error');
        closeChat();
    }
}

function updateChatMenu() {
    const info = entityMap[currentEntity];
    if (!info) {
        menuAdmins.style.display = 'none';
        menuInvites.style.display = 'none';
        document.getElementById('menu-block').style.display = 'none'; // скрываем кнопку блокировки
        return;
    }
    const isGroupOrChannel = info.type === 'group' || info.type === 'channel';
    const isOwner = info.owner === userEmail;
    const isChat = info.type === 'chat';

    // Админы и приглашения – только для групп/каналов и только владельцу
    menuAdmins.style.display = (isGroupOrChannel && isOwner) ? 'flex' : 'none';
    menuInvites.style.display = (isGroupOrChannel && isOwner) ? 'flex' : 'none';

    // Кнопка "Заблокировать" – только для личных чатов
    document.getElementById('menu-block').style.display = isChat ? 'flex' : 'none';

    // Аналитика – для групп/каналов и владельцу (если нужно)
    const analyticsItem = document.getElementById('menu-analytics');
    if (analyticsItem) {
        analyticsItem.style.display = (isGroupOrChannel && isOwner) ? 'flex' : 'none';
    }
}

function updateAnalyticsMenu() {
    const analyticsItem = document.getElementById('menu-analytics');
    if (!analyticsItem) return;
    const info = entityMap[currentEntity];
    if (!info) {
        analyticsItem.style.display = 'none';
        return;
    }
    const isGroupOrChannel = info.type === 'group' || info.type === 'channel';
    const isOwner = info.owner === userEmail;
    analyticsItem.style.display = (isGroupOrChannel && isOwner) ? 'flex' : 'none';
}

// ---------- ЗАЯВКИ НА ВСТУПЛЕНИЕ ----------
async function openJoinRequestsModal() {
    if (!currentEntity) return;
    joinRequestsModal.classList.add('active');
    await refreshJoinRequests();
}

async function refreshJoinRequests() {
    if (!currentEntity) return;
    if (!entityMap[currentEntity] || entityMap[currentEntity].is_private === undefined) {
        try {
            const resp = await apiFetch(`/api/entity_info/${currentEntity}`);
            if (resp && resp.ok) {
                const data = await resp.json();
                if (data.status === 'ok') {
                    entityMap[currentEntity] = {
                        name: data.name || `Сущность ${currentEntity}`,
                        type: data.type || 'chat',
                        username: data.username || '',
                        owner: data.owner || '',
                        is_private: data.is_private || false,
                    };
                } else {
                    if (!entityMap[currentEntity]) {
                        entityMap[currentEntity] = {
                            name: `Сущность ${currentEntity}`,
                            type: 'chat',
                            username: '',
                            owner: '',
                            is_private: false,
                        };
                    } else {
                        entityMap[currentEntity].is_private = false;
                    }
                }
            } else {
                if (!entityMap[currentEntity]) {
                    entityMap[currentEntity] = {
                        name: `Сущность ${currentEntity}`,
                        type: 'chat',
                        username: '',
                        owner: '',
                        is_private: false,
                    };
                } else {
                    entityMap[currentEntity].is_private = false;
                }
            }
        } catch (e) {
            console.error('Ошибка загрузки информации о сущности для заявок:', e);
            if (!entityMap[currentEntity]) {
                entityMap[currentEntity] = {
                    name: `Сущность ${currentEntity}`,
                    type: 'chat',
                    username: '',
                    owner: '',
                    is_private: false,
                };
            } else {
                entityMap[currentEntity].is_private = false;
            }
        }
    }

    const info = entityMap[currentEntity];
    privateToggleCheck.checked = info && info.is_private ? true : false;

    try {
        const resp = await apiFetch(`/api/join_requests/${currentEntity}`);
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const requests = data.requests || [];

        let html = '';
        if (requests.length === 0) {
            html = '<div class="empty">Нет активных заявок</div>';
        } else {
            html = '<div>';
            requests.forEach(req => {
                const displayName = req.first_name || req.username || req.email;
                const username = req.username ? '@' + req.username : '';
                html += `
                    <div class="request-item">
                        <div class="info">
                            <div class="name">${escHtml(displayName)}</div>
                            <div class="username">${escHtml(username)}</div>
                        </div>
                        <div class="actions">
                            <button class="approve" data-email="${escHtml(req.email)}">✅</button>
                            <button class="reject" data-email="${escHtml(req.email)}">❌</button>
                        </div>
                    </div>
                `;
            });
            html += '</div>';
        }
        joinRequestsList.innerHTML = html;

        joinRequestsList.querySelectorAll('.approve').forEach(btn => {
            btn.addEventListener('click', () => {
                const email = btn.dataset.email;
                approveJoinRequest(currentEntity, email);
            });
        });
        joinRequestsList.querySelectorAll('.reject').forEach(btn => {
            btn.addEventListener('click', () => {
                const email = btn.dataset.email;
                rejectJoinRequest(currentEntity, email);
            });
        });
    } catch (e) {
        showToast('Ошибка загрузки заявок: ' + e.message, 'error');
        joinRequestsList.innerHTML = '<div class="empty">Ошибка загрузки</div>';
    }
}
privateToggleCheck.addEventListener('change', async function() {
    if (!currentEntity) return;
    const isPrivate = this.checked;
    try {
        const resp = await apiFetch('/api/set_entity_private', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: currentEntity, is_private: isPrivate })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Режим обновлён', 'success');
            if (entityMap[currentEntity]) entityMap[currentEntity].is_private = isPrivate;
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
            this.checked = !isPrivate;
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
        this.checked = !isPrivate;
    }
});

async function approveJoinRequest(entityId, email) {
    try {
        const resp = await apiFetch('/api/approve_join_request', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: entityId, email })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Заявка подтверждена', 'success');
            refreshJoinRequests();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

async function rejectJoinRequest(entityId, email) {
    try {
        const resp = await apiFetch('/api/reject_join_request', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: entityId, email })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Заявка отклонена', 'success');
            refreshJoinRequests();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

// ---------- ПРОФИЛЬ, БОТЫ, АДМИНЫ ----------
async function loadProfile() {
    await loadMyProfile();
    profileModal.classList.add('active');
}
async function showBotToken(botId) {
    try {
        const resp = await apiFetch('/api/bot_token', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({bot_id: botId})
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            const token = data.data.token;
            const bot = botsList.find(b => b.id === botId);
            const botName = bot ? bot.name : 'бота';
            let html = `
                <div class="section-header">
                    <div class="section-title">Токен бота</div>
                    <div class="section-sub">«${escHtml(botName)}»</div>
                </div>
                <div class="icon-row" style="cursor:default;">
                    <div class="row-icon"><i class="fas fa-key"></i></div>
                    <div class="row-body">
                        <div class="row-title" style="word-break:break-all; font-family:monospace; font-size:12.5px;">${escHtml(token)}</div>
                    </div>
                </div>
                <div class="profile-buttons">
                    <button onclick="navigator.clipboard.writeText('${token}').then(() => showToast('Токен скопирован', 'success'))">Скопировать</button>
                    <button onclick="openBotSettings(${botId})">Назад</button>
                </div>
            `;
            profileContent.innerHTML = html;
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

function openBotSettings(botId) {
    const bot = botsList.find(b => b.id === botId);
    if (!bot) {
        showToast('Бот не найден', 'error');
        return;
    }
    let html = `
        <div class="section-header">
            <div class="section-title">${escHtml(bot.name)}</div>
            <div class="section-sub">Настройки бота</div>
        </div>
        <div class="icon-row" style="cursor:default;">
            <div class="row-icon"><i class="fas fa-at"></i></div>
            <div class="row-body"><div class="row-title">@${escHtml(bot.username)}</div><div class="row-sub">юзернейм</div></div>
        </div>
        <div class="icon-row" style="cursor:default;">
            <div class="row-icon"><i class="fas fa-hashtag"></i></div>
            <div class="row-body"><div class="row-title">${bot.id}</div><div class="row-sub">ID</div></div>
        </div>
        <div class="icon-row" style="cursor:default;">
            <div class="row-icon"><i class="fas fa-circle" style="font-size:9px;"></i></div>
            <div class="row-body"><div class="row-title">${bot.is_active ? 'Активен' : 'Неактивен'}</div><div class="row-sub">статус</div></div>
        </div>
        <div style="display:flex; flex-direction:column; gap:10px; margin-top:16px;">
            <button onclick="showBotToken(${bot.id})" style="padding:10px; border:none; border-radius:16px; background:#005f60; color:white; cursor:pointer;">Показать токен</button>
            <button onclick="deleteBot(${bot.id})" style="padding:10px; border:none; border-radius:16px; background:#ffebee; color:#c62828; cursor:pointer;">Удалить бота</button>
            <button onclick="showBots()" style="padding:10px; border:none; border-radius:16px; background:#e8eaed; color:#333; cursor:pointer;">Назад к списку</button>
        </div>
    `;
    profileContent.innerHTML = html;
    if (!profileModal.classList.contains('active')) {
        profileModal.classList.add('active');
    }
}

async function showBots() {
    try {
        const resp = await apiFetch('/api/bots');
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        botsList = data.bots || [];
        let html = `<div class="section-header">
                        <div class="section-title">Ваши боты</div>
                        <div class="section-sub">${botsList.length ? botsList.length + ' шт.' : 'Пока нет ни одного'}</div>
                    </div>`;
        botsList.forEach(b => {
            html += `<div class="icon-row" onclick="openBotSettings(${b.id})">
                        <div class="row-icon"><i class="fas fa-robot"></i></div>
                        <div class="row-body">
                            <div class="row-title">${escHtml(b.name)}</div>
                            <div class="row-sub">@${escHtml(b.username)} · ID ${b.id}</div>
                        </div>
                        <span class="row-chevron"><i class="fas fa-chevron-right"></i></span>
                    </div>`;
        });
        html += '<button onclick="createBot()" style="margin-top:6px; padding:10px 24px; border:none; border-radius:20px; background:#005f60; color:white; cursor:pointer; font-size:15px; width:100%;">Создать бота</button>';
        profileContent.innerHTML = html;
        profileModal.classList.add('active');
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

window.deleteBot = async function(botId) {
    if (!confirm('Удалить бота?')) return;
    try {
        const resp = await apiFetch('/api/delete_bot', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({bot_id: botId}) });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') { showBots(); showToast('Бот удалён', 'success'); } else { showToast('Ошибка: ' + data.reason, 'error'); }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
};

window.getBotToken = async function(botId) {
    try {
        const resp = await apiFetch('/api/bot_token', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({bot_id: botId}) });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Токен: ' + data.data.token, 'success');
        } else { showToast('Ошибка: ' + data.reason, 'error'); }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
};

window.createBot = async function() {
    const name = prompt('Введите имя бота:');
    if (!name) return;
    const username = prompt('Введите юзернейм (начинается с @, опционально):') || '';
    const cleanUsername = username.startsWith('@') ? username.substring(1) : username;
    try {
        const resp = await apiFetch('/api/create_bot', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({name, username: cleanUsername})
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Бот создан! Токен: ' + data.data.token, 'success');
            showBots();
        } else { showToast('Ошибка: ' + data.reason, 'error'); }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
};

async function showAdminsMenu() {
    if (!currentEntity) return;
    try {
        const resp = await apiFetch(`/api/entity_admins/${currentEntity}`);
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const admins = data.admins || [];
        let html = `<div class="section-header">
                        <div class="section-title">Админы сущности</div>
                        <div class="section-sub">${admins.length ? admins.length + ' чел.' : 'Пока нет админов'}</div>
                    </div>`;
        admins.forEach(email => {
            html += `<div class="icon-row" style="cursor:default;">
                        <div class="row-icon"><i class="fas fa-user-shield"></i></div>
                        <div class="row-body"><div class="row-title">${escHtml(email)}</div></div>
                    </div>`;
        });
        html += `<div class="profile-buttons">
                    <button onclick="promoteAdmin()">Назначить</button>
                    <button onclick="demoteAdmin()">Снять</button>
                  </div>`;
        profileContent.innerHTML = html;
        profileModal.classList.add('active');
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

window.promoteAdmin = async function() {
    const email = prompt('Введите email пользователя для назначения админом:');
    if (!email) return;
    try {
        const resp = await apiFetch('/api/promote_admin', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ entity_id: currentEntity, email })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Админ назначен', 'success');
            showAdminsMenu();
        } else { showToast('Ошибка: ' + data.reason, 'error'); }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
};

window.demoteAdmin = async function() {
    const email = prompt('Введите email пользователя для снятия админа:');
    if (!email) return;
    try {
        const resp = await apiFetch('/api/demote_admin', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ entity_id: currentEntity, email })
        });
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Админ снят', 'success');
            showAdminsMenu();
        } else { showToast('Ошибка: ' + data.reason, 'error'); }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
};

// ---------- ОТКРЫТИЕ ПРОФИЛЯ ПО IDENTIFIER ----------
function openUserProfile(identifier) {
    profileModal.classList.add('active');
    loadUserProfile(identifier);
}

// ---------- ИНЛАЙН-КНОПКИ ----------
function renderButtons(entityId, msgId, replyMarkup) {
    const container = document.createElement('div');
    container.className = 'inline-buttons';
    container.style.display = 'flex';
    container.style.flexWrap = 'wrap';
    container.style.gap = '8px';
    container.style.marginTop = '8px';

    const keyboard = replyMarkup.inline_keyboard || [];
    for (let row of keyboard) {
        const rowDiv = document.createElement('div');
        rowDiv.style.display = 'flex';
        rowDiv.style.gap = '8px';
        rowDiv.style.width = '100%';
        for (let btn of row) {
            const button = document.createElement('button');
            button.textContent = btn.text;
            button.style.padding = '6px 12px';
            button.style.border = 'none';
            button.style.borderRadius = '12px';
            button.style.background = '#e8eaed';
            button.style.cursor = 'pointer';
            button.style.fontSize = '14px';
            button.addEventListener('click', () => {
                handleCallback(entityId, msgId, btn.callback_data);
            });
            rowDiv.appendChild(button);
        }
        container.appendChild(rowDiv);
    }
    return container;
}

async function handleCallback(entityId, msgId, callbackData) {
    try {
        const resp = await apiFetch('/api/callback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ entity_id: entityId, message_id: msgId, callback_data: callbackData })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Действие выполнено', 'success');
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

// ---------- ОБРАБОТЧИКИ UI ----------
document.getElementById('logo').addEventListener('click', loadProfile);
document.getElementById('search-btn').addEventListener('click', () => { searchModal.classList.add('active'); document.getElementById('search-input').value = ''; document.getElementById('search-results').innerHTML = ''; });
document.querySelectorAll('.modal-overlay .close').forEach(el => {
    el.addEventListener('click', () => { el.closest('.modal-overlay').classList.remove('active'); });
});
document.querySelectorAll('.modal-overlay').forEach(el => {
    el.addEventListener('click', (e) => { if (e.target === el) el.classList.remove('active'); });
});

document.getElementById('settings-close').addEventListener('click', () => { settingsModal.classList.remove('active'); });
settingsModal.addEventListener('click', (e) => { if (e.target === settingsModal) settingsModal.classList.remove('active'); });

fab.addEventListener('click', () => {
    createModal.classList.add('active');
    document.getElementById('create-detail').style.display = 'none';
    document.querySelector('.create-menu').style.display = 'flex';
});

document.querySelectorAll('.create-menu .item').forEach(item => {
    item.addEventListener('click', () => {
        const type = item.dataset.type;
        document.querySelector('.create-menu').style.display = 'none';
        const detail = document.getElementById('create-detail');
        detail.style.display = 'block';
        const nameInput = document.getElementById('create-name');
        const usernameInput = document.getElementById('create-username');
        const targetInput = document.getElementById('create-target');

        if (type === 'chat') {
            nameInput.style.display = 'none';
            targetInput.style.display = 'block';
            targetInput.placeholder = 'Email или @username собеседника';
            usernameInput.style.display = 'none';
        } else {
            nameInput.style.display = 'block';
            nameInput.placeholder = 'Имя';
            targetInput.style.display = 'none';
            usernameInput.style.display = 'block';
            usernameInput.placeholder = 'Юзернейм (опционально)';
        }
        detail.dataset.type = type;
    });
});

document.getElementById('create-cancel').addEventListener('click', () => { createModal.classList.remove('active'); });
document.getElementById('create-confirm').addEventListener('click', () => {
    const type = document.getElementById('create-detail').dataset.type;
    let name = document.getElementById('create-name').value.trim();
    if (type === 'chat') {
        name = '';
    } else if (!name) {
        showToast('Введите имя', 'error');
        return;
    }
    let username = document.getElementById('create-username').value.trim() || undefined;
    let target = '';
    if (type === 'chat') {
        target = document.getElementById('create-target').value.trim();
        if (!target) { showToast('Введите email или @username', 'error'); return; }
        username = undefined;
    }
    createModal.classList.remove('active');
    createEntity(type, name, username, target);
});

chatBack.addEventListener('click', closeChat);
chatSend.addEventListener('click', sendMessage);
chatInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') sendMessage(); });

const attachBtn = document.getElementById('chat-file-label');
const attachMenu = document.getElementById('attachment-menu');
let attachMenuOpen = false;
attachBtn.addEventListener('click', function(e) {
    e.preventDefault();
    attachMenuOpen = !attachMenuOpen;
    attachMenu.style.display = attachMenuOpen ? 'block' : 'none';
});
document.addEventListener('click', function(e) {
    if (!attachMenu.contains(e.target) && e.target !== attachBtn && !attachBtn.contains(e.target)) {
        attachMenu.style.display = 'none';
        attachMenuOpen = false;
    }
});
document.getElementById('attach-file').addEventListener('click', function() {
    attachMenu.style.display = 'none';
    attachMenuOpen = false;
    document.getElementById('chat-file-input').click();
});
document.getElementById('attach-poll').addEventListener('click', function() {
    attachMenu.style.display = 'none';
    attachMenuOpen = false;
    openPollModal();
});

function openPollModal() {
    document.getElementById('poll-modal').classList.add('active');
    document.getElementById('poll-question').value = '';
    const container = document.getElementById('poll-options-container');
    const rows = container.querySelectorAll('.poll-option-row');
    rows.forEach((row, idx) => {
        if (idx < 2) {
            row.style.display = 'flex';
            row.querySelector('input').value = '';
            row.querySelector('.poll-remove-option').style.display = 'none';
        } else {
            row.remove();
        }
    });
}
document.getElementById('poll-close').addEventListener('click', function() {
    document.getElementById('poll-modal').classList.remove('active');
});
document.getElementById('poll-modal').addEventListener('click', function(e) {
    if (e.target === this) this.classList.remove('active');
});

document.getElementById('poll-add-option').addEventListener('click', function() {
    const container = document.getElementById('poll-options-container');
    const rows = container.querySelectorAll('.poll-option-row');
    if (rows.length >= 8) { showToast('Максимум 8 вариантов', 'error'); return; }
    const newRow = document.createElement('div');
    newRow.className = 'poll-option-row';
    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'poll-option';
    input.placeholder = 'Вариант ' + (rows.length + 1);
    const btn = document.createElement('button');
    btn.className = 'poll-remove-option';
    btn.textContent = '✕';
    btn.style.display = 'inline';
    btn.addEventListener('click', function() {
        if (container.querySelectorAll('.poll-option-row').length <= 2) {
            showToast('Минимум 2 варианта', 'error');
            return;
        }
        newRow.remove();
    });
    newRow.appendChild(input);
    newRow.appendChild(btn);
    container.appendChild(newRow);
});

document.getElementById('poll-create').addEventListener('click', createPoll);
async function createPoll() {
    const entityId = currentEntity;
    if (!entityId) { showToast('Сначала откройте чат', 'error'); return; }
    const question = document.getElementById('poll-question').value.trim();
    const optionInputs = document.querySelectorAll('.poll-option');
    const options = [];
    optionInputs.forEach(inp => {
        const val = inp.value.trim();
        if (val) options.push(val);
    });
    if (!question) { showToast('Введите вопрос', 'error'); return; }
    if (options.length < 2) { showToast('Минимум 2 варианта', 'error'); return; }
    try {
        const resp = await apiFetch('/api/create_poll', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ entity_id: entityId, question, options })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            document.getElementById('poll-modal').classList.remove('active');
            await loadMessages(entityId);
            showToast('Опрос создан', 'success');
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
}

document.getElementById('chat-file-label').addEventListener('click', function() {
});
document.getElementById('chat-file-input').addEventListener('change', uploadFiles);

let searchTimeout = null;
document.getElementById('search-input').addEventListener('input', function() {
    const query = this.value.trim();
    const resultsContainer = document.getElementById('search-results');
    if (!query) { resultsContainer.innerHTML = ''; return; }
    clearTimeout(searchTimeout);
    searchTimeout = setTimeout(async () => {
        const ownMatches = Object.values(entityMap).filter(e =>
            e.name.toLowerCase().includes(query.toLowerCase()) ||
            (e.username && e.username.toLowerCase().includes(query.toLowerCase()))
        );
        let globalResults = [];
        try {
            const resp = await fetch(`/api/search?q=${encodeURIComponent(query)}`);
            if (resp.ok) {
                const data = await resp.json();
                if (data.status === 'ok') globalResults = data.results || [];
            }
        } catch(e) { console.warn(e); }
        const typeIcon = { user: 'fa-user', bot: 'fa-robot', entity: 'fa-hashtag', chat: 'fa-comment-dots', group: 'fa-user-shield', channel: 'fa-bullhorn' };
        const typeLabel = { user: 'человек', bot: 'бот', entity: 'чат', chat: 'чат', group: 'группа', channel: 'канал' };
        let html = '';
        if (ownMatches.length) {
            html += `<div class="group-tag">Свои чаты</div>`;
            ownMatches.forEach(e => {
                const id = Object.keys(entityMap).find(k => entityMap[k] === e);
                const icon = typeIcon[e.type] || 'fa-comment-dots';
                html += `<div class="icon-row search-item" data-id="${id}">
                    <div class="row-icon"><i class="fas ${icon}"></i></div>
                    <div class="row-body">
                        <div class="row-title">${escHtml(e.name)}</div>
                    </div>
                    <span class="type-tag">${escHtml(typeLabel[e.type] || e.type)}</span>
                </div>`;
            });
        }
        if (globalResults.length) {
            html += `<div class="group-tag">Найдено в системе</div>`;
            globalResults.forEach(item => {
                let displayName = item.name || item.first_name || item.email || '';
                let sub = '';
                if (item.type === 'user') {
                    displayName = item.first_name || item.username || item.email;
                    sub = item.email;
                } else if (item.type === 'bot') {
                    displayName = item.name;
                    sub = item.username ? `@${item.username}` : '';
                } else if (item.type === 'entity') {
                    displayName = item.name;
                    sub = item.username ? `@${item.username}` : '';
                }
                const icon = typeIcon[item.type] || 'fa-user';
                html += `<div class="icon-row search-item" data-type="${item.type}" data-id="${item.id || ''}" data-email="${item.email || ''}" data-username="${item.username || ''}">
                    <div class="row-icon"><i class="fas ${icon}"></i></div>
                    <div class="row-body">
                        <div class="row-title">${escHtml(displayName)}</div>
                        ${sub ? `<div class="row-sub">${escHtml(sub)}</div>` : ''}
                    </div>
                    <span class="type-tag">${escHtml(typeLabel[item.type] || item.type)}</span>
                </div>`;
            });
        }
        if (!ownMatches.length && !globalResults.length) {
            html = '<div class="analytics-empty">Ничего не найдено</div>';
        }
        resultsContainer.innerHTML = html;
        resultsContainer.querySelectorAll('.search-item').forEach(el => {
            el.addEventListener('click', async function() {
                const type = this.dataset.type;
                const id = this.dataset.id ? parseInt(this.dataset.id) : null;
                const email = this.dataset.email;
                const username = this.dataset.username;
                searchModal.classList.remove('active');
                if (type === 'entity') {
                    openChat(id);
                } else if (type === 'user' || type === 'bot') {
                    const target = email || (username ? '@' + username : '');
                    if (!target) { showToast('Неизвестный пользователь', 'error'); return; }
                    try {
                        const resp = await fetch('/api/create_entity', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ type: 'chat', name: '', target })
                        });
                        const data = await resp.json();
                        if (data.status === 'ok') {
                            const entityId = data.data.entity_id;
                            if (entityId) { await loadEntities(); openChat(entityId); }
                        } else { showToast('Ошибка: ' + data.reason, 'error'); }
                    } catch(e) { showToast('Ошибка: ' + e.message, 'error'); }
                } else {
                    if (this.dataset.id) openChat(parseInt(this.dataset.id));
                }
            });
        });
    }, 300);
});

document.getElementById('join-btn').addEventListener('click', () => {
    const id = parseInt(document.getElementById('join-id').value);
    if (!id) { showToast('Введите ID', 'error'); return; }
    searchModal.classList.remove('active');
    joinEntityById(id);
});

mediaModal.addEventListener('click', (e) => {
    if (e.target === mediaModal) {
        mediaModal.classList.remove('active');
        mediaContainer.innerHTML = '';
        mediaInfo.textContent = '';
    }
});
document.getElementById('media-close').addEventListener('click', () => {
    mediaModal.classList.remove('active');
    mediaContainer.innerHTML = '';
    mediaInfo.textContent = '';
});

chatMenuBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    chatMenuDropdown.classList.toggle('show');
});
document.addEventListener('click', () => { chatMenuDropdown.classList.remove('show'); });

menuAdmins.addEventListener('click', () => {
    chatMenuDropdown.classList.remove('show');
    showAdminsMenu();
});
menuInvites.addEventListener('click', () => {
    chatMenuDropdown.classList.remove('show');
    openJoinRequestsModal();
});
menuLeave.addEventListener('click', () => {
    chatMenuDropdown.classList.remove('show');
    leaveEntity();
});

function showNameModal() {
    updateLogo('connected');
    nameModal.classList.add('active');
    nameInput.value = '';
    nameError.style.display = 'none';
    nameInput.focus();
}

function hideNameModal() {
    nameModal.classList.remove('active');
}

async function saveName() {
    const name = nameInput.value.trim();
    if (!name) {
        nameError.style.display = 'block';
        return;
    }
    nameError.style.display = 'none';
    try {
        const resp = await apiFetch('/api/set_first_name', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Имя установлено!', 'success');
            hideNameModal();
            window.location.href = '/';
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

nameSave.addEventListener('click', saveName);
nameCancel.addEventListener('click', hideNameModal);
nameInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') saveName();
});

document.getElementById('reply-cancel').addEventListener('click', () => {
    currentReply = null;
    document.getElementById('reply-preview').style.display = 'none';
});

eventSource.onmessage = function(event) {
    const data = JSON.parse(event.data);
    
    if (data.type === 'new_message') {
        if (currentEntity && data.entity_id === currentEntity) {
            loadMessages(currentEntity);
        }
        notifyNewMessage(data);
        loadEntities();
    } else if (data.type === 'edit_message') {
        if (currentEntity && data.entity_id === currentEntity) {
            loadMessages(currentEntity);
        }
    } else if (data.type === 'file_stream') {
        loadEntities();
    } else if (data.type === 'disconnected') {
        updateLogo('reconnecting');
    } else if (data.type === 'reconnected') {
        updateLogo('connected');
    } else if (data.type === 'reaction_update') {
        const msgId = data.message_id;
        const reactions = data.reactions;
        if (currentEntity && msgId) {
            if (currentMessageReactions[msgId]) {
                currentMessageReactions[msgId].counts = reactions;
                const myReaction = currentMessageReactions[msgId].my_reaction;
                if (myReaction && !reactions[myReaction]) {
                    currentMessageReactions[msgId].my_reaction = null;
                }
                renderReactions(msgId, currentMessageReactions[msgId]);
            } else {
                currentMessageReactions[msgId] = { counts: reactions, my_reaction: null };
                renderReactions(msgId, currentMessageReactions[msgId]);
            }
        }
    } else if (data.type === 'new_join_request') {
        showToast(`Новая заявка от ${data.display_name || data.email}`, 'info');
        if (currentEntity && data.entity_id === currentEntity) {
            if (joinRequestsModal.classList.contains('active')) {
                refreshJoinRequests();
            }
        }
    } else if (data.type === 'join_request_approved') {
        showToast('Ваша заявка на вступление подтверждена!', 'success');
        if (currentEntity && data.entity_id === currentEntity) {
            loadMessages(currentEntity);
        }
        loadEntities();
    } else if (data.type === 'join_request_rejected') {
        showToast('Ваша заявка на вступление отклонена', 'error');
        if (currentEntity && data.entity_id === currentEntity) {
            closeChat();
        }
        loadEntities();
    } else if (data.type === 'poll_update') {
        if (currentEntity && data.entity_id === currentEntity) {
            loadMessages(currentEntity);
    } else if (data.type === 'refresh_entities') {
          loadEntities();
    }
    
    }
};
eventSource.onerror = function(e) {
    console.error('SSE error', e);
    updateLogo('reconnecting');
};

async function loadEntities() {
    try {
        const resp = await apiFetch('/api/entities');
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const entities = data.entities || [];
        const newMap = {};
        const newlyAdded = [];
        entities.forEach(e => {
            if (entitiesInitialized && !(e.id in entityMap)) {
                newlyAdded.push(e.name);
            }
            newMap[e.id] = { name: e.name, type: e.type, username: e.username || '', owner: e.owner || '', is_private: e.is_private || false };
        });
        entityMap = newMap;
        if (!entitiesInitialized) {
            entitiesInitialized = true;
        } else {
            newlyAdded.forEach(name => notifyNewChat(name));
        }
        renderEntityList(entities);
    } catch(e) {
        console.error('Ошибка загрузки сущностей:', e);
        showToast('Ошибка загрузки сущностей', 'error');
    }
}

function renderEntityList(entities) {
    entityList.innerHTML = '';
    if (entities.length === 0) {
        entityList.innerHTML = '<div style="padding:20px; color:#999; text-align:center;">Нет сущностей</div>';
        return;
    }
    entities.forEach(ent => {
        const div = document.createElement('div');
        div.className = 'entity-item';
        const icon = ent.type === 'chat' ? '<i class="fas fa-comment-dots"></i>' : 
             (ent.type === 'group' ? '<i class="fas fa-users"></i>' : '<i class="fas fa-bullhorn"></i>');
        const privateIcon = ent.is_private ? '🔒' : '🌐';
        div.innerHTML = `
            <span class="icon-badge">${icon}</span>
            <div class="info">
                <div class="name">${escHtml(ent.name)} <span style="font-size:12px; color:#999;">${privateIcon}</span></div>
                <div class="sub">${ent.type} ${ent.username ? '@'+escHtml(ent.username) : ''}</div>
            </div>
        `;
        div.dataset.id = ent.id;
        div.addEventListener('click', () => openChat(ent.id));
        entityList.appendChild(div);
    });
}

async function init() {
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.get('setup_name') === 'true') {
        window.history.replaceState({}, document.title, '/');
        showNameModal();
        try {
            const resp = await apiFetch('/api/whoami');
            if (resp && resp.status === 200) {
                const data = await resp.json();
                if (data.user_id) {
                    userId = data.user_id;
                    userEmail = data.email || '';
                    currentUserEmail = userEmail;
                }
            }
        } catch(e) {}
        try { await loadEntities(); } catch(e) {}
        syncInterval = setInterval(loadEntities, 60000);
        requestNotificationPermissionOnce();
        return;
    }

    const resp = await apiFetch('/api/whoami');
    if (!resp) return;
    if (resp.status === 401) { window.location.href = '/'; return; }
    const data = await resp.json();
    if (data.user_id) {
        userId = data.user_id;
        userEmail = data.email || '';
        currentUserEmail = userEmail;
    }
    try {
        const profResp = await apiFetch('/api/profile');
        if (profResp && profResp.ok) {
            const profData = await profResp.json();
            if (profData.status === 'ok') {
                myDisplayName = profData.data.first_name || userEmail;
                currentUserEmail = profData.data.email;
            } else {
                myDisplayName = userEmail;
            }
        } else {
            myDisplayName = userEmail;
        }
    } catch(e) {
        myDisplayName = userEmail;
    }
    await loadEntities();
    syncInterval = setInterval(loadEntities, 60000);
    requestNotificationPermissionOnce();
}

window.addEventListener('beforeunload', () => {
    if (reconnectTimer) clearInterval(reconnectTimer);
});

// ----- АНАЛИТИКА -----
document.getElementById('menu-analytics').addEventListener('click', function() {
    chatMenuDropdown.classList.remove('show');
    openAnalytics();
});

let analyticsPeriod = 7;
let analyticsStats = null;

document.querySelectorAll('.analytics-tabs button').forEach(function(btn) {
    btn.addEventListener('click', function() {
        document.querySelectorAll('.analytics-tabs button').forEach(b => b.classList.remove('active'));
        this.classList.add('active');
        analyticsPeriod = parseInt(this.dataset.period, 10);
        renderAnalytics();
    });
});

async function openAnalytics() {
    if (!currentEntity) {
        showToast('Сначала откройте чат', 'error');
        return;
    }
    const modal = document.getElementById('analytics-modal');
    modal.classList.add('active');

    const titleEl = document.getElementById('chat-title');
    document.getElementById('analytics-entity-name').textContent = (titleEl && titleEl.textContent) || 'Аналитика';

    analyticsPeriod = 7;
    document.querySelectorAll('.analytics-tabs button').forEach(b => b.classList.toggle('active', b.dataset.period === '7'));

    try {
        const resp = await apiFetch(`/api/analytics/${currentEntity}`);
        if (!resp) return;
        if (resp.status === 401) { window.location.href = '/'; return; }
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        analyticsStats = data.data;
        renderAnalytics();
    } catch (e) {
        showToast('Ошибка загрузки аналитики: ' + e.message, 'error');
    }
}

function formatNum(n) {
    return (n || 0).toLocaleString('ru-RU');
}

function renderTrend(elId, current, baseline) {
    const el = document.getElementById(elId);
    if (!el) return;
    if (!baseline) { el.textContent = ''; return; }
    const diff = Math.round(((current - baseline) / baseline) * 100);
    if (Math.abs(diff) < 1) {
        el.className = 'stat-trend flat';
        el.innerHTML = '<i class="fas fa-minus"></i> как обычно';
    } else if (diff > 0) {
        el.className = 'stat-trend up';
        el.innerHTML = `<i class="fas fa-arrow-up"></i> ${diff}% к среднему за месяц`;
    } else {
        el.className = 'stat-trend down';
        el.innerHTML = `<i class="fas fa-arrow-down"></i> ${Math.abs(diff)}% к среднему за месяц`;
    }
}

function renderAnalytics() {
    if (!analyticsStats) return;
    const stats = analyticsStats;
    const activity = stats.activity || [];
    const isWeek = analyticsPeriod === 7;

    document.getElementById('stat-members').textContent = formatNum(stats.member_count);

    const messages = isWeek ? (stats.messages_7d || 0) : (stats.messages_30d || 0);
    const reactions = isWeek ? (stats.reactions_7d || 0) : (stats.reactions_30d || 0);
    document.getElementById('stat-messages').textContent = formatNum(messages);
    document.getElementById('stat-messages-label').textContent = isWeek ? 'Сообщений за неделю' : 'Сообщений за месяц';
    document.getElementById('stat-reactions').textContent = formatNum(reactions);
    document.getElementById('stat-reactions-label').textContent = isWeek ? 'Реакций за неделю' : 'Реакций за месяц';

    const avgWeek = (stats.messages_7d || 0) / 7;
    const avgMonth = (stats.messages_30d || 0) / 30;
    document.getElementById('stat-avg').textContent = (isWeek ? avgWeek : avgMonth).toFixed(1).replace('.', ',');

    if (isWeek) {
        renderTrend('stat-messages-trend', avgWeek, avgMonth);
    } else {
        document.getElementById('stat-messages-trend').textContent = '';
    }

    document.getElementById('chart-range-label').textContent = isWeek ? 'последние 7 дней' : 'последние 30 дней';

    const chartData = isWeek ? activity.slice(-7) : activity;
    const counts = chartData.map(item => item.count || 0);
    const dateFmt = new Intl.DateTimeFormat('ru-RU', { day: 'numeric', month: 'short' });
    const labels = chartData.map(item => {
        const d = new Date(item.day);
        return isNaN(d.getTime()) ? item.day : dateFmt.format(d);
    });

    const chartWrap = document.getElementById('analytics-chart-wrap');
    const emptyEl = document.getElementById('analytics-empty');

    if (chartData.length === 0 || counts.every(c => c === 0)) {
        chartWrap.style.display = 'none';
        emptyEl.style.display = 'block';
        if (window.activityChartInstance) {
            window.activityChartInstance.destroy();
            window.activityChartInstance = null;
        }
        return;
    }
    chartWrap.style.display = '';
    emptyEl.style.display = 'none';

    const ctx = document.getElementById('activityChart').getContext('2d');
    if (window.activityChartInstance) {
        window.activityChartInstance.destroy();
    }
    const gradient = ctx.createLinearGradient(0, 0, 0, 240);
    gradient.addColorStop(0, 'rgba(0,95,96,0.25)');
    gradient.addColorStop(1, 'rgba(0,95,96,0)');

    window.activityChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Сообщений',
                data: counts,
                borderColor: '#005f60',
                backgroundColor: gradient,
                borderWidth: 2,
                pointRadius: counts.length > 20 ? 0 : 3,
                pointBackgroundColor: '#005f60',
                pointHoverRadius: 5,
                tension: 0.35,
                fill: true,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: '#ffffff',
                    titleColor: '#222',
                    bodyColor: '#005f60',
                    borderColor: '#e0e4e8',
                    borderWidth: 1,
                    padding: 10,
                    cornerRadius: 10,
                    displayColors: false,
                    callbacks: {
                        label: (item) => `${item.parsed.y} сообщ.`
                    }
                }
            },
            scales: {
                x: { grid: { display: false }, ticks: { color: '#999', font: { size: 11 } } },
                y: {
                    beginAtZero: true,
                    ticks: {
                        stepSize: Math.max(1, Math.ceil(Math.max(...counts, 1) / 5)),
                        color: '#999',
                        font: { size: 11 }
                    },
                    grid: { color: '#eef1f5' }
                }
            }
        }
    });
}

document.getElementById('analytics-close').addEventListener('click', function() {
    document.getElementById('analytics-modal').classList.remove('active');
});
document.getElementById('analytics-modal').addEventListener('click', function(e) {
    if (e.target === this) this.classList.remove('active');
});

document.getElementById('settings-sessions').addEventListener('click', function() {
    settingsModal.classList.remove('active');
    openSessionsModal();
});

async function openSessionsModal() {
    const modal = document.getElementById('sessions-modal');
    modal.classList.add('active');
    await loadSessions();
}

async function loadSessions() {
    const list = document.getElementById('sessions-list');
    list.innerHTML = 'Загрузка...';
    try {
        const resp = await apiFetch('/api/sessions');
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const sessions = data.data.sessions || [];
        if (sessions.length === 0) {
            list.innerHTML = '<div class="empty">Нет активных сессий</div>';
            return;
        }
        let html = '<div>';
        sessions.forEach(s => {
            const isCurrent = s.is_current;
            const created = new Date(s.created_at * 1000).toLocaleString();
            const lastActive = new Date(s.last_active * 1000).toLocaleString();
            let userAgent = s.user_agent || '';
            let agentName = 'Неизвестный';
            
            if (userAgent) {
                let browser = 'Неизвестный браузер';
                let os = '';
                
                if (userAgent.includes('Chrome') && !userAgent.includes('Edg')) browser = 'Chrome';
                else if (userAgent.includes('Firefox')) browser = 'Firefox';
                else if (userAgent.includes('Safari') && !userAgent.includes('Chrome')) browser = 'Safari';
                else if (userAgent.includes('Edg')) browser = 'Edge';
                else if (userAgent.includes('Opera') || userAgent.includes('OPR')) browser = 'Opera';
                else if (userAgent.includes('YaBrowser')) browser = 'Яндекс.Браузер';
                
                if (userAgent.includes('Android')) os = 'Android';
                else if (userAgent.includes('iPhone') || userAgent.includes('iPad')) os = 'iOS';
                else if (userAgent.includes('Windows')) os = 'Windows';
                else if (userAgent.includes('Mac OS')) os = 'macOS';
                else if (userAgent.includes('Linux')) os = 'Linux';
                
                if (browser !== 'Неизвестный браузер' || os) {
                    agentName = browser + (os ? ' (' + os + ')' : '');
                } else {
                    agentName = userAgent.substring(0, 20) + (userAgent.length > 20 ? '…' : '');
                }
            } else {
                agentName = 'Неизвестный (нет User‑Agent)';
            }

            html += `
                <div class="list-item" style="flex-direction:column; align-items:stretch;">
                    <div style="font-weight:500; display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                        ${agentName}
                        ${isCurrent ? '<span style="font-size:12px; background:#2e7d32; color:white; border-radius:12px; padding:0 8px;">Текущая</span>' : ''}
                    </div>
                    <div style="font-size:13px; color:#666; margin-top:2px;">IP: ${s.ip || 'неизвестен'}</div>
                    <div style="font-size:13px; color:#666;">Создана: ${created}</div>
                    <div style="font-size:13px; color:#666;">Последняя активность: ${lastActive}</div>
                    ${!isCurrent ? `<div style="margin-top:8px;"><button class="terminate-session-btn" data-session-id="${s.session_id}">Завершить</button></div>` : ''}
                </div>
            `;
        });
        html += '</div>';
        list.innerHTML = html;

        list.querySelectorAll('.terminate-session-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                const sessionId = this.dataset.sessionId;
                terminateSession(sessionId);
            });
        });
    } catch(e) {
        list.innerHTML = 'Ошибка загрузки: ' + e.message;
        showToast('Ошибка загрузки сессий', 'error');
    }
}

async function terminateSession(sessionId) {
    if (!confirm('Завершить эту сессию?')) return;
    try {
        const resp = await apiFetch('/api/terminate_session', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_id: sessionId })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Сессия завершена', 'success');
            await loadSessions();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

document.getElementById('sessions-close').addEventListener('click', function() {
    document.getElementById('sessions-modal').classList.remove('active');
});
document.getElementById('sessions-modal').addEventListener('click', function(e) {
    if (e.target === this) this.classList.remove('active');
});

// ========== ЧЕРНЫЙ СПИСОК ==========

// Обработчик для открытия черного списка из настроек
document.getElementById('settings-blacklist').addEventListener('click', function() {
    settingsModal.classList.remove('active');
    openBlacklistModal();
});

async function openBlacklistModal() {
    const modal = document.getElementById('blacklist-modal');
    modal.classList.add('active');
    await loadBlacklist();
}

async function loadBlacklist() {
    const list = document.getElementById('blacklist-list');
    list.innerHTML = 'Загрузка...';
    try {
        const resp = await apiFetch('/api/blacklist');
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.reason);
        const items = data.data.blacklist || [];
        if (items.length === 0) {
            list.innerHTML = '<div class="empty">Никто не заблокирован</div>';
            return;
        }
        let html = '';
        items.forEach(item => {
            const identifier = item.blocked_identifier;
            const type = item.blocked_type;
            const display = item.display_name || identifier;
            html += `
                <div class="list-item">
                    <span class="value">${escHtml(display)}</span>
                    <button class="unblock-btn" data-target="${escHtml(identifier)}">Разблокировать</button>
                </div>
            `;
        });
        list.innerHTML = html;
        list.querySelectorAll('.unblock-btn').forEach(btn => {
            btn.addEventListener('click', async function() {
                const target = this.dataset.target;
                await unblockUser(target);
                await loadBlacklist();
            });
        });
    } catch (e) {
        list.innerHTML = 'Ошибка загрузки: ' + e.message;
        showToast('Ошибка загрузки черного списка', 'error');
    }
}

async function unblockUser(target) {
    try {
        const resp = await apiFetch('/api/unblock', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ target })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Пользователь разблокирован', 'success');
            await loadEntities();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

document.getElementById('blacklist-close').addEventListener('click', function() {
    document.getElementById('blacklist-modal').classList.remove('active');
});
document.getElementById('blacklist-modal').addEventListener('click', function(e) {
    if (e.target === this) this.classList.remove('active');
});

// Блокировка пользователя
async function blockUser(target) {
    try {
        const resp = await apiFetch('/api/block', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ target })
        });
        if (!resp) return;
        const data = await resp.json();
        if (data.status === 'ok') {
            showToast('Пользователь заблокирован', 'success');
            closeChat();
            await loadEntities();
        } else {
            showToast('Ошибка: ' + data.reason, 'error');
        }
    } catch(e) {
        showToast('Ошибка: ' + e.message, 'error');
    }
}

// Обработчик кнопки "Заблокировать" в меню чата
document.getElementById('menu-block').addEventListener('click', function() {
    chatMenuDropdown.classList.remove('show');
    const target = chatTargets[currentEntity];
    if (!target) {
        showToast('Не удалось определить собеседника', 'error');
        return;
    }
    if (confirm(`Заблокировать пользователя ${target}? Чат будет удалён.`)) {
        blockUser(target);
    }
});

init();
</script>
</body>
</html>
'''

# ---------- Quart маршруты ----------
@app.route('/')
async def index():
    user_id = get_user_id()
    if user_id in clients:
        return UI_HTML
    else:
        return LOGIN_HTML

@app.route('/api/whoami')
async def whoami():
    user_id = get_user_id()
    if user_id not in clients:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    email = clients[user_id].email
    return jsonify({'user_id': user_id, 'email': email})

@app.route('/api/logout', methods=['POST'])
async def logout():
    user_id = get_user_id()
    if user_id in clients:
        client = clients.pop(user_id, None)
        if client and client.websocket:
            await client.websocket.close()
    session.clear()
    return jsonify({'status': 'ok'})

@app.route('/api/request_code', methods=['POST'])
async def request_code():
    data = await request.get_json()
    email = data.get('email')
    if not email:
        return jsonify({'status': 'error', 'reason': 'Email required'}), 400

    user_id = get_user_id()
    if user_id in pending_auth:
        try:
            await pending_auth[user_id]['client'].websocket.close()
        except:
            pass
        del pending_auth[user_id]

    client = WebChatClient()
    try:
        await client.connect()
        await client.request_code(email)
        pending_auth[user_id] = {'email': email, 'client': client}
        return jsonify({'status': 'ok'})
    except Exception as e:
        traceback.print_exc()
        error_msg = str(e)
        if 'Connection refused' in error_msg:
            error_msg = 'Не удалось подключиться к серверу. Убедитесь, что он запущен.'
        return jsonify({'status': 'error', 'reason': error_msg}), 500

@app.route('/api/login', methods=['POST'])
async def login():
    data = await request.get_json()
    code = data.get('code')
    if not code:
        return jsonify({'status': 'error', 'reason': 'Code required'}), 400

    user_id = get_user_id()
    pending = pending_auth.get(user_id)
    if not pending:
        return jsonify({'status': 'error', 'reason': 'Сначала запросите код'}), 400

    email, client = pending['email'], pending['client']
    try:
        await client.authenticate(email, code)
        client._user_id = user_id
        clients[user_id] = client
        user_id_to_email[user_id] = email
        del pending_auth[user_id]
        return jsonify({'status': 'ok', 'requires_first_name': client.requires_first_name})
    except Exception as e:
        traceback.print_exc()
        error_msg = str(e)
        if 'Authentication failed' in error_msg or 'invalid' in error_msg.lower():
            error_msg = 'Неверный код или email'
        return jsonify({'status': 'error', 'reason': error_msg}), 500

@app.route('/api/set_first_name', methods=['POST'])
async def set_first_name():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    name = data.get('name', '').strip()
    if not name:
        return jsonify({'status': 'error', 'reason': 'Name cannot be empty'}), 400

    try:
        await client.send_command('set_first_name', {'name': name})
        my_display_name_cache[user_id] = name
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/events')
async def events():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return Response('Not authenticated', status=401)

    async def event_generator():
        while True:
            try:
                event_type, data = await client.get_event()
                yield f"data: {json.dumps({'type': event_type, **data})}\n\n"
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"SSE error: {e}")
                break

    return Response(event_generator(), mimetype="text/event-stream")

@app.route('/api/entities')
async def get_entities():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    try:
        info = await client.send_command('get_user_info', {'target': client.email})
        entities_ids = info.get('entities', [])
        result = []
        for eid in entities_ids:
            try:
                entity_info = await client.get_entity_info(eid)
                name = entity_info.get('name', f"Сущность {eid}")
                etype = entity_info.get('type', 'chat')
                username = entity_info.get('username', '')
                owner = entity_info.get('owner', '')
                is_private = entity_info.get('is_private', False)
                if user_id not in entity_cache:
                    entity_cache[user_id] = {}
                entity_cache[user_id][eid] = {
                    'name': name,
                    'type': etype,
                    'username': username,
                    'owner': owner,
                    'is_private': is_private
                }
                result.append({
                    'id': eid,
                    'name': name,
                    'type': etype,
                    'username': username,
                    'owner': owner,
                    'is_private': is_private
                })
            except Exception as e:
                logger.warning(f"Не удалось получить info о сущности {eid}: {e}")
                continue
        return jsonify({'entities': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/entity_info/<int:entity_id>')
async def entity_info(entity_id):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        info = await client.get_entity_info(entity_id)
        return jsonify({'status': 'ok', **info})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/messages/<int:entity_id>')
async def get_messages(entity_id):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    limit = request.args.get('limit', 50, type=int)
    try:
        result = await client.send_command('get_messages', {'entity_id': entity_id, 'limit': limit})
        return jsonify(result)
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/send', methods=['POST'])
async def send():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    content = data.get('content')
    reply_to = data.get('reply_to')
    if not entity_id or not content:
        return jsonify({'status': 'error', 'reason': 'Missing entity_id or content'}), 400

    try:
        params = {'entity_id': entity_id, 'content': content}
        if reply_to:
            params['reply_to'] = reply_to
        result = await client.send_command('send_message', params)
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/upload_file', methods=['POST'])
async def upload_file():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    files = await request.files
    if 'file' not in files:
        return jsonify({'status': 'error', 'reason': 'No file'}), 400
    file = files['file']
    form = await request.form
    entity_id = form.get('entity_id', type=int)
    if not entity_id:
        return jsonify({'status': 'error', 'reason': 'Missing entity_id'}), 400

    content = file.read()
    filename = file.filename
    try:
        await client.send_file_from_bytes(entity_id, filename, content)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/download_file')
async def download_file():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    entity_id = request.args.get('entity_id', type=int)
    message_id = request.args.get('message_id', type=int)
    if not entity_id or not message_id:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        data = await client.download_file_to_bytes(entity_id, message_id)
        filename = f"file_{message_id}"
        response = await app.make_response(data)
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        response.headers['Content-Type'] = 'application/octet-stream'
        return response
    except Exception as e:
        logger.error(f"Download error: {e}", exc_info=True)
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/create_entity', methods=['POST'])
async def create_entity():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_type = data.get('type')
    name = data.get('name')
    target = data.get('target')
    username = data.get('username')
    if not entity_type:
        return jsonify({'status': 'error', 'reason': 'Missing type'}), 400
    if entity_type != 'chat' and not name:
        return jsonify({'status': 'error', 'reason': 'Missing name'}), 400
    try:
        params = {'type': entity_type, 'name': name}
        if target:
            params['target'] = target
        if username:
            params['username'] = username
        result = await client.send_command('create_entity', params)
        entity_id = result.get('entity_id')
        if entity_id:
            if user_id not in entity_cache:
                entity_cache[user_id] = {}
            entity_cache[user_id][entity_id] = {'name': name, 'type': entity_type, 'username': username or '', 'owner': user_id_to_email.get(user_id, ''), 'is_private': False}
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/join', methods=['POST'])
async def join_entity():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    if not entity_id:
        return jsonify({'status': 'error', 'reason': 'Missing entity_id'}), 400

    try:
        result = await client.send_command('join_entity', {'entity_id': entity_id})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/leave', methods=['POST'])
async def leave_entity():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    if not entity_id:
        return jsonify({'status': 'error', 'reason': 'Missing entity_id'}), 400

    try:
        await client.send_command('leave_entity', {'entity_id': entity_id})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/delete_message', methods=['POST'])
async def delete_message():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    message_id = data.get('message_id')
    if not entity_id or not message_id:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        await client.send_command('delete_message', {'entity_id': entity_id, 'message_id': message_id})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/profile')
async def profile():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    try:
        info = await client.send_command('get_user_info', {'target': client.email})
        return jsonify({'status': 'ok', 'data': info})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/user_info/<target>')
async def user_info(target):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        info = await client.send_command('get_user_info', {'target': target})
        return jsonify({'status': 'ok', 'data': info})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/set_username', methods=['POST'])
async def set_username():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    username = data.get('username', '')
    try:
        await client.send_command('set_username', {'username': username})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/set_bio', methods=['POST'])
async def set_bio():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    bio = data.get('bio', '')
    try:
        await client.send_command('set_bio', {'bio': bio})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/set_show_email', methods=['POST'])
async def set_show_email():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    show = data.get('show')
    if not isinstance(show, bool):
        return jsonify({'status': 'error', 'reason': 'show must be boolean'}), 400
    try:
        await client.send_command('set_show_email', {'show': show})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/entity_admins/<int:entity_id>')
async def entity_admins(entity_id):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    try:
        result = await client.send_command('get_entity_admins', {'entity_id': entity_id})
        return jsonify({'status': 'ok', 'admins': result.get('admins', [])})
    except Exception as e:
        logger.warning(f"get_entity_admins не поддерживается: {e}")
        return jsonify({'status': 'ok', 'admins': []})

@app.route('/api/promote_admin', methods=['POST'])
async def promote_admin():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    email = data.get('email')
    if not entity_id or not email:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        await client.send_command('promote_admin', {'entity_id': entity_id, 'email': email})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/demote_admin', methods=['POST'])
async def demote_admin():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    email = data.get('email')
    if not entity_id or not email:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        await client.send_command('demote_admin', {'entity_id': entity_id, 'email': email})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/bots')
async def list_bots():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        result = await client.send_command('list_bots', {})
        return jsonify({'status': 'ok', 'bots': result.get('bots', [])})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/create_bot', methods=['POST'])
async def create_bot():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    name = data.get('name')
    username = data.get('username')
    if not name:
        return jsonify({'status': 'error', 'reason': 'Name required'}), 400
    try:
        result = await client.send_command('create_bot', {'name': name, 'username': username})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/delete_bot', methods=['POST'])
async def delete_bot():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    bot_id = data.get('bot_id')
    if not bot_id:
        return jsonify({'status': 'error', 'reason': 'bot_id required'}), 400
    try:
        await client.send_command('delete_bot', {'bot_id': bot_id})
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/bot_token', methods=['POST'])
async def get_bot_token():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    bot_id = data.get('bot_id')
    if not bot_id:
        return jsonify({'status': 'error', 'reason': 'bot_id required'}), 400
    try:
        result = await client.send_command('get_bot_token', {'bot_id': bot_id})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/add_reaction', methods=['POST'])
async def add_reaction():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    message_id = data.get('message_id')
    reaction_type = data.get('reaction_type')
    entity_id = data.get('entity_id')
    if message_id is None or not reaction_type or entity_id is None:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        result = await client.send_command('add_reaction', {
            'message_id': message_id,
            'reaction_type': reaction_type,
            'entity_id': entity_id
        })
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 200

@app.route('/api/remove_reaction', methods=['POST'])
async def remove_reaction():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    message_id = data.get('message_id')
    entity_id = data.get('entity_id')
    if message_id is None or entity_id is None:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        result = await client.send_command('remove_reaction', {
            'message_id': message_id,
            'entity_id': entity_id
        })
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 200

@app.route('/api/stickers')
async def get_stickers():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    query = request.args.get('query', '')
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    try:
        result = await client.send_command('get_stickers', {'query': query, 'limit': limit, 'offset': offset})
        return jsonify({'status': 'ok', 'stickers': result.get('stickers', [])})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/send_sticker', methods=['POST'])
async def send_sticker():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    sticker_id = data.get('sticker_id')
    if not entity_id or not sticker_id:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        result = await client.send_command('send_sticker', {'entity_id': entity_id, 'sticker_id': sticker_id})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/sticker/<int:sticker_id>')
async def get_sticker_file(sticker_id):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        data = await client.download_sticker_file(sticker_id)
        return Response(data, mimetype='image/webp')
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/set_entity_private', methods=['POST'])
async def set_entity_private():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    entity_id = data.get('entity_id')
    is_private = data.get('is_private')
    if entity_id is None or is_private is None:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400
    try:
        await client.set_entity_private(entity_id, is_private)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/join_requests/<int:entity_id>')
async def get_join_requests(entity_id):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        requests = await client.get_join_requests(entity_id)
        return jsonify({'status': 'ok', 'requests': requests})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/approve_join_request', methods=['POST'])
async def approve_join_request():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    entity_id = data.get('entity_id')
    email = data.get('email')
    if entity_id is None or not email:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400
    try:
        await client.approve_join_request(entity_id, email)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/reject_join_request', methods=['POST'])
async def reject_join_request():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    entity_id = data.get('entity_id')
    email = data.get('email')
    if entity_id is None or not email:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400
    try:
        await client.reject_join_request(entity_id, email)
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/callback', methods=['POST'])
async def callback():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    data = await request.get_json()
    entity_id = data.get('entity_id')
    message_id = data.get('message_id')
    callback_data = data.get('callback_data')
    if entity_id is None or message_id is None or not callback_data:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400

    try:
        result = await client.send_command(CMD_CALLBACK_QUERY, {
            'entity_id': entity_id,
            'message_id': message_id,
            'callback_data': callback_data
        })
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/analytics/<int:entity_id>')
async def get_analytics(entity_id):
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        result = await client.send_command(CMD_GET_ANALYTICS, {'entity_id': entity_id})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/create_poll', methods=['POST'])
async def create_poll():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    entity_id = data.get('entity_id')
    question = data.get('question')
    options = data.get('options')
    if not entity_id or not question or not options:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400
    try:
        result = await client.send_command(CMD_CREATE_POLL, {
            'entity_id': entity_id,
            'question': question,
            'options': options,
        })
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/vote_poll', methods=['POST'])
async def vote_poll():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    poll_id = data.get('poll_id')
    option_index = data.get('option_index')
    if poll_id is None or option_index is None:
        return jsonify({'status': 'error', 'reason': 'Missing params'}), 400
    try:
        result = await client.send_command(CMD_VOTE_POLL, {
            'poll_id': poll_id,
            'option_index': option_index,
        })
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/close_poll', methods=['POST'])
async def close_poll():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    poll_id = data.get('poll_id')
    if poll_id is None:
        return jsonify({'status': 'error', 'reason': 'Missing poll_id'}), 400
    try:
        result = await client.send_command(CMD_CLOSE_POLL, {'poll_id': poll_id})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/sessions')
async def get_sessions():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        result = await client.send_command(CMD_GET_SESSIONS, {})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/terminate_session', methods=['POST'])
async def terminate_session():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    session_id = data.get('session_id')
    if not session_id:
        return jsonify({'status': 'error', 'reason': 'Missing session_id'}), 400
    try:
        result = await client.send_command(CMD_TERMINATE_SESSION, {'session_id': session_id})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/search')
async def search():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401

    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'status': 'ok', 'results': []})

    try:
        result = await client.send_command(CMD_SEARCH, {'query': q})
        return jsonify({'status': 'ok', 'results': result.get('results', [])})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/blacklist', methods=['GET'])
async def get_blacklist():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    try:
        result = await client.send_command(CMD_GET_BLACKLIST, {})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/block', methods=['POST'])
async def block_user():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    target = data.get('target')
    if not target:
        return jsonify({'status': 'error', 'reason': 'Missing target'}), 400
    try:
        result = await client.send_command(CMD_BLOCK_USER, {'target': target})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500

@app.route('/api/unblock', methods=['POST'])
async def unblock_user():
    user_id = get_user_id()
    client = clients.get(user_id)
    if not client:
        return jsonify({'status': 'error', 'reason': 'Not authenticated'}), 401
    data = await request.get_json()
    target = data.get('target')
    if not target:
        return jsonify({'status': 'error', 'reason': 'Missing target'}), 400
    try:
        result = await client.send_command(CMD_UNBLOCK_USER, {'target': target})
        return jsonify({'status': 'ok', 'data': result})
    except Exception as e:
        return jsonify({'status': 'error', 'reason': str(e)}), 500
        

if __name__ == '__main__':
    print('Запуск Aerisyn на http://localhost:5000')
    app.run(host='0.0.0.0', port=5000, debug=True)
