"""add_reply_to

Revision ID: 66483e8f0695
Revises: 658fe93482c9
Create Date: 2026-07-31 14:14:45.387693

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = '66483e8f0695'
down_revision = '658fe93482c9'
branch_labels = None
depends_on = None

def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    # 1. Проверяем и добавляем столбцы, если их нет
    columns = [col['name'] for col in inspector.get_columns('messages')]
    if 'reply_to_entity_id' not in columns:
        op.add_column('messages', sa.Column('reply_to_entity_id', sa.Integer(), nullable=True))
    if 'reply_to_message_id' not in columns:
        op.add_column('messages', sa.Column('reply_to_message_id', sa.Integer(), nullable=True))

    # 2. Проверяем, существует ли уже внешний ключ
    fks = inspector.get_foreign_keys('messages')
    fk_exists = any(
        fk['constrained_columns'] == ['reply_to_entity_id'] and fk['referred_table'] == 'entities'
        for fk in fks
    )
    if not fk_exists:
        with op.batch_alter_table('messages') as batch_op:
            batch_op.create_foreign_key(
                'fk_messages_reply_to_entity_id_entities',
                'entities',
                ['reply_to_entity_id'],
                ['id']
            )

def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    # 1. Удаляем внешний ключ, если он существует
    fks = inspector.get_foreign_keys('messages')
    fk_exists = any(
        fk['constrained_columns'] == ['reply_to_entity_id'] and fk['referred_table'] == 'entities'
        for fk in fks
    )
    if fk_exists:
        with op.batch_alter_table('messages') as batch_op:
            batch_op.drop_constraint('fk_messages_reply_to_entity_id_entities', type_='foreignkey')

    # 2. Удаляем столбцы, если они есть
    columns = [col['name'] for col in inspector.get_columns('messages')]
    if 'reply_to_message_id' in columns:
        op.drop_column('messages', 'reply_to_message_id')
    if 'reply_to_entity_id' in columns:
        op.drop_column('messages', 'reply_to_entity_id')