import pytest
import base64
from axiso import EdDSA
from protocol import serialize_sorted


@pytest.mark.asyncio
async def test_verify_signature(security_service, test_user):
    email, pub_key = test_user
    keys = EdDSA.generate_keypair()
    user = await security_service.db_store.get_user(email)
    user.public_key = keys["public_key"]
    await security_service.db_store.update_user(user)

    obj = {"command": "test", "data": 123}
    payload = serialize_sorted(obj)
    signature = EdDSA.sign(payload, keys["private_key"])
    obj["signature"] = base64.urlsafe_b64encode(signature).decode()

    assert await security_service.verify_signature(obj, email) is True

    # Подмена данных
    obj2 = obj.copy()
    obj2["data"] = 456
    assert await security_service.verify_signature(obj2, email) is False


@pytest.mark.asyncio
async def test_is_request_replayed(security_service, test_user):
    email, _ = test_user
    req_id = 42
    # Первый раз не replay
    assert await security_service.is_request_replayed(email, req_id) is False
    # Второй раз replay
    assert await security_service.is_request_replayed(email, req_id) is True