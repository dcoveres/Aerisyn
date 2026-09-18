import pytest
import base64
from axiso import DH
from services.auth_service import AuthService


@pytest.mark.asyncio
async def test_request_code(auth_service, send_code_service_mock):
    email = "test@example.com"
    success, msg = await auth_service.request_code(email)
    assert success is True
    stored = await auth_service.redis_store.get_auth_code(email)
    assert stored is not None
    assert "code" in stored
    # Повторный запрос слишком быстро
    success2, msg2 = await auth_service.request_code(email)
    assert success2 is False
    assert "Подождите" in msg2


@pytest.mark.asyncio
async def test_authenticate(auth_service, test_user, send_code_service_mock):
    email, pub_key = test_user

    # --- Успешная аутентификация ---
    await auth_service.request_code(email)
    stored = await auth_service.redis_store.get_auth_code(email)
    code = stored["code"]

    client_keys = DH.generate_keypair()
    client_public_raw = base64.urlsafe_b64decode(client_keys["public_key"])
    ed_public = pub_key

    success, session_key, user, error = await auth_service.authenticate(
        email, code, ed_public, client_public_raw
    )
    assert success is True
    assert session_key is not None
    assert user.email == email

    # После успешной аутентификации код удалён
    stored_after = await auth_service.redis_store.get_auth_code(email)
    assert stored_after is None

    # Попытка с неверным кодом без нового запроса должна вернуть "Код не запрошен"
    success2, _, _, error2 = await auth_service.authenticate(
        email, "wrong", ed_public, client_public_raw
    )
    assert success2 is False
    assert "Код не запрошен" in error2

@pytest.mark.asyncio
async def test_auth_block_bruteforce(auth_service, test_user):
    email, pub_key = test_user
    await auth_service.request_code(email)
    stored = await auth_service.redis_store.get_auth_code(email)
    code = stored["code"]  # правильный, но будем вводить неправильный

    client_keys = DH.generate_keypair()
    client_public_raw = base64.urlsafe_b64decode(client_keys["public_key"])

    # 5 неудачных попыток
    for _ in range(5):
        success, _, _, _ = await auth_service.authenticate(
            email, "wrong", pub_key, client_public_raw
        )
        assert success is False

    # Шестая должна заблокировать
    success, _, _, error = await auth_service.authenticate(
        email, "wrong", pub_key, client_public_raw
    )
    assert success is False
    assert "blocked" in error.lower()