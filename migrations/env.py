import asyncio
import os
import sys
sys.path.insert(0, os.getcwd())

from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine
from alembic import context
from db.models import Base

print("Текущая директория:", os.getcwd())
print("Файлы в ней:", os.listdir('.'))
print("Таблицы в Base.metadata:", list(Base.metadata.tables.keys()))

config = context.config
target_metadata = Base.metadata

# ----- ФУНКЦИЯ ДЛЯ ИГНОРИРОВАНИЯ ТАБЛИЦ -----
def include_object(object, name, type_, reflected, compare_to):
    # Игнорируем таблицу android_metadata
    if type_ == "table" and name == "android_metadata":
        return False
    # Можно добавить игнорирование других таблиц
    return True
# ---------------------------------------------

def run_migrations_offline():
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        include_object=include_object   
    )
    with context.begin_transaction():
        context.run_migrations()

def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=include_object  
    )
    with context.begin_transaction():
        context.run_migrations()

async def run_migrations_online():
    url = config.get_main_option("sqlalchemy.url")
    engine = create_async_engine(url, poolclass=pool.NullPool)
    async with engine.connect() as conn:
        await conn.run_sync(do_run_migrations)
    await engine.dispose()

if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())