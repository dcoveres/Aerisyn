import pytest
import os
import shutil
from unittest.mock import AsyncMock
from protocol import ENTITY_TYPE_GROUP, STORAGE_DIR, serialize, compute_file_checksum
from services.file_service import FileService
from protocol import MAX_CONCURRENT_UPLOADS

@pytest.fixture(autouse=True)
def clean_storage():
    """Очищает папку storage/files перед каждым тестом и после."""
    if os.path.exists(STORAGE_DIR):
        shutil.rmtree(STORAGE_DIR)
    os.makedirs(STORAGE_DIR, exist_ok=True)
    yield
    if os.path.exists(STORAGE_DIR):
        shutil.rmtree(STORAGE_DIR)


@pytest.mark.asyncio
async def test_allocate_stream_id(file_service, test_user):
    email, _ = test_user
    sid = await file_service.allocate_stream_id(email)
    assert sid is not None
    assert file_service.get_stream_owner(sid) == email


@pytest.mark.asyncio
async def test_start_upload(file_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    sid = await file_service.allocate_stream_id(email)
    content = b"hello"
    checksum = compute_file_checksum(content)
    header = {
        "type": "file_header",
        "filename": "test.txt",
        "size": len(content),
        "entity_id": entity.id,
        "checksum": checksum
    }
    header_data = serialize(header)
    assert await file_service.start_upload(sid, header_data, email) is True
    assert sid in file_service.file_streams
    # Повторный старт того же потока должен завершиться неудачей
    assert await file_service.start_upload(sid, header_data, email) is False


@pytest.mark.asyncio
async def test_upload_file_flow(file_service, test_user, entity_service, message_service):
    # Создаём мок для gateway с атрибутом message_service
    gateway_mock = AsyncMock()
    gateway_mock.message_service = message_service
    # Подменяем gateway у file_service
    file_service.gateway = gateway_mock

    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    sid = await file_service.allocate_stream_id(email)
    content = b"hello"
    checksum = compute_file_checksum(content)
    header = {
        "type": "file_header",
        "filename": "test.txt",
        "size": len(content),
        "entity_id": entity.id,
        "checksum": checksum
    }
    header_data = serialize(header)
    await file_service.start_upload(sid, header_data, email)
    await file_service.append_data(sid, content, email)
    await file_service.finish_upload(sid, True, email)

    entity_dir = os.path.join(STORAGE_DIR, str(entity.id))
    files = os.listdir(entity_dir)
    assert len(files) == 1

    msgs = await message_service.get_messages(email, entity.id, 10)
    assert len(msgs) == 1
    assert msgs[0].type == "file"
    assert msgs[0].filename == "test.txt"

@pytest.mark.asyncio
async def test_upload_abort(file_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    sid = await file_service.allocate_stream_id(email)
    content = b"hello"
    header = {
        "type": "file_header",
        "filename": "test.txt",
        "size": len(content),
        "entity_id": entity.id,
        "checksum": compute_file_checksum(content)
    }
    header_data = serialize(header)
    await file_service.start_upload(sid, header_data, email)
    await file_service.finish_upload(sid, False, email)  # abort
    entity_dir = os.path.join(STORAGE_DIR, str(entity.id))
    assert not os.path.exists(entity_dir) or len(os.listdir(entity_dir)) == 0

@pytest.mark.asyncio
async def test_upload_size_mismatch(file_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    sid = await file_service.allocate_stream_id(email)
    content = b"hello"
    header = {
        "type": "file_header",
        "filename": "test.txt",
        "size": 100,  # ожидаем 100, отправляем 5
        "entity_id": entity.id,
        "checksum": compute_file_checksum(content)
    }
    header_data = serialize(header)
    await file_service.start_upload(sid, header_data, email)
    await file_service.append_data(sid, content, email)
    await file_service.finish_upload(sid, True, email)
    entity_dir = os.path.join(STORAGE_DIR, str(entity.id))
    assert not os.path.exists(entity_dir) or len(os.listdir(entity_dir)) == 0

@pytest.mark.asyncio
async def test_upload_concurrent_limit(file_service, test_user, entity_service):
    email, _ = test_user
    entity = await entity_service.create_entity(ENTITY_TYPE_GROUP, "Group", email)
    # Занимаем все слоты
    sids = []
    for _ in range(MAX_CONCURRENT_UPLOADS):
        sid = await file_service.allocate_stream_id(email)
        sids.append(sid)
        header = {"type": "file_header", "filename": "f.txt", "size": 10, "entity_id": entity.id, "checksum": "fake"}
        await file_service.start_upload(sid, serialize(header), email)
    # Попытка ещё одного должна провалиться
    extra_sid = await file_service.allocate_stream_id(email)
    header = {"type": "file_header", "filename": "extra.txt", "size": 10, "entity_id": entity.id, "checksum": "fake"}
    result = await file_service.start_upload(extra_sid, serialize(header), email)
    assert result is False
    # Очищаем
    for sid in sids:
        await file_service.finish_upload(sid, False, email)