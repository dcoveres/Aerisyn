import asyncio
import os
import time
import uuid
import sys
from services.postgres_store import PostgresStore
from services.redis_store import RedisStore
from services.models import Sticker
from protocol import STORAGE_DIR

async def add_sticker(name: str, file_path: str, tags: list = None):
    """Добавляет стикер в базу данных и копирует файл в хранилище."""
    if not os.path.isfile(file_path):
        print(f"Файл {file_path} не найден")
        return

    # Проверяем расширение (должен быть .webp)
    if not file_path.lower().endswith('.webp'):
        print("Стикер должен быть в формате WebP")
        return

    # Подключаемся к БД и Redis
    db_store = PostgresStore("sqlite+aiosqlite:///./messenger.db")  # или ваша строка подключения
    redis_store = RedisStore()
    await redis_store.connect()

    # Генерируем ID
    sticker_id = await redis_store.get_next_sticker_id()  # нужно добавить метод в RedisStore
    # Если метода нет, можно использовать отдельный счётчик в Redis: await redis_store.redis.incr("counter:sticker")

    # Копируем файл в папку стикеров
    sticker_dir = os.path.join(STORAGE_DIR, "stickers")
    os.makedirs(sticker_dir, exist_ok=True)
    new_filename = f"{uuid.uuid4().hex}.webp"
    new_path = os.path.join(sticker_dir, new_filename)
    import shutil
    shutil.copy2(file_path, new_path)

    # Создаём объект стикера
    sticker = Sticker(
        id=sticker_id,
        name=name,
        file_path=new_path,
        tags=tags or [],
        created_at=time.time()
    )

    # Сохраняем в БД 
    # Пока добавим напрямую через SQLAlchemy
    from db.models import StickerModel
    from sqlalchemy import select
    async with db_store.async_session() as session:
        # Проверяем, нет ли уже такого имени (опционально)
        result = await session.execute(select(StickerModel).where(StickerModel.name == name))
        if result.scalar_one_or_none():
            print(f"Стикер с именем '{name}' уже существует")
            return

        model = StickerModel(
            id=sticker.id,
            name=sticker.name,
            file_path=sticker.file_path,
            tags=sticker.tags,
            created_at=sticker.created_at
        )
        session.add(model)
        await session.commit()
        print(f"Стикер '{name}' добавлен с ID {sticker.id}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Использование: python add_sticker.py <название> <путь_к_файлу.webp> [теги через запятую]")
        sys.exit(1)

    name = sys.argv[1]
    file_path = sys.argv[2]
    tags = sys.argv[3].split(',') if len(sys.argv) > 3 else []

    asyncio.run(add_sticker(name, file_path, tags))
