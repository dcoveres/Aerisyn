import pytest
import os
import tempfile
import shutil
from services.models import Sticker

@pytest.fixture(autouse=True)
def create_sticker_dir():
    """Создаёт временную папку для стикеров и удаляет её после теста."""
    tmp_dir = tempfile.mkdtemp(prefix="stickers_test_")
    # Подменяем путь в sticker_service
    original_dir = None
    if hasattr(pytest, 'sticker_service'):
        original_dir = pytest.sticker_service.sticker_dir
        pytest.sticker_service.sticker_dir = tmp_dir
    yield tmp_dir
    shutil.rmtree(tmp_dir, ignore_errors=True)
    if original_dir:
        pytest.sticker_service.sticker_dir = original_dir

@pytest.mark.asyncio
async def test_get_stickers_empty(sticker_service, create_sticker_dir):
    result = await sticker_service.get_stickers()
    assert result == []

@pytest.mark.asyncio
async def test_create_and_get_sticker(sticker_service, create_sticker_dir):
    sticker = Sticker(id=1, name="funny", file_path=os.path.join(create_sticker_dir, "1.webp"), tags=["fun"])
    await sticker_service.db_store.create_sticker(sticker)
    result = await sticker_service.get_stickers(query="fun")
    assert len(result) == 1
    assert result[0]["name"] == "funny"
    assert result[0]["id"] == 1

@pytest.mark.asyncio
async def test_get_sticker_by_id(sticker_service, create_sticker_dir):
    sticker = Sticker(id=2, name="cool", file_path=os.path.join(create_sticker_dir, "2.webp"), tags=[])
    await sticker_service.db_store.create_sticker(sticker)
    result = await sticker_service.get_sticker_by_id(2)
    assert result["name"] == "cool"

@pytest.mark.asyncio
async def test_get_sticker_file_path(sticker_service, create_sticker_dir):
    sticker = Sticker(id=3, name="test", file_path=os.path.join(create_sticker_dir, "3.webp"), tags=[])
    await sticker_service.db_store.create_sticker(sticker)
    path = await sticker_service.get_sticker_file_path(3)
    assert path == os.path.join(create_sticker_dir, "3.webp")

@pytest.mark.asyncio
async def test_send_sticker(sticker_service, create_sticker_dir):
    sticker = Sticker(id=4, name="meme", file_path=os.path.join(create_sticker_dir, "4.webp"), tags=[])
    await sticker_service.db_store.create_sticker(sticker)
    result = await sticker_service.send_sticker("test@example.com", 1, 4)
    assert result["sticker_name"] == "meme"
    assert result["file_path"] == os.path.join(create_sticker_dir, "4.webp")