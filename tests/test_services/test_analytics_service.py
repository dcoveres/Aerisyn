import pytest
import time
from services.models import Entity, Message, Reaction

@pytest.mark.asyncio
async def test_get_analytics_empty(analytics_service, postgres_store):
    entity = Entity(id=1, type="group", name="Group", owner="test@example.com")
    await postgres_store.create_entity(entity)
    await postgres_store.add_member(1, "test@example.com")
    result = await analytics_service.get_analytics(1)
    assert result["member_count"] == 1
    assert result["messages_7d"] == 0
    assert result["messages_30d"] == 0
    assert result["reactions_7d"] == 0
    assert result["reactions_30d"] == 0
    assert result["activity"] == []

@pytest.mark.asyncio
async def test_get_analytics_with_data(analytics_service, postgres_store):
    entity = Entity(id=2, type="group", name="Group", owner="test@example.com")
    await postgres_store.create_entity(entity)
    await postgres_store.add_member(2, "test@example.com")
    now = time.time()
    for i in range(5):
        msg = Message(id=i+1, from_email="test@example.com", content=f"msg{i}", timestamp=now - i*1000, entity_id=2)
        await postgres_store.add_message(msg)

    # Добавляем реакции с разными email, чтобы избежать уникальности
    emails = ["user1@example.com", "user2@example.com", "user3@example.com"]
    for i, email in enumerate(emails):
        # создаём пользователей
        await postgres_store.create_user(email, "key")
        reaction = Reaction(message_id=1, email=email, reaction_type="❤️", entity_id=2, timestamp=now - i*1000)
        await postgres_store.add_reaction(reaction)

    result = await analytics_service.get_analytics(2)
    assert result["member_count"] == 1  # только владелец
    assert result["messages_7d"] == 5
    assert result["messages_30d"] == 5
    assert result["reactions_7d"] == 3
    assert result["reactions_30d"] == 3
    assert len(result["activity"]) > 0