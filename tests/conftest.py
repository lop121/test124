import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://tests:tests@localhost/payments_test")
os.environ.setdefault("RABBITMQ_URL", "amqp://guest:guest@localhost/")
os.environ.setdefault("API_KEY", "test-api-key-at-least-sixteen")
os.environ.setdefault("WEBHOOK_SECRET", "test-webhook-secret-sixteen")
os.environ.setdefault("WEBHOOK_ALLOW_HTTP", "true")

from src.database import Base  # noqa: E402
from src.models import Outbox, Payment  # noqa: E402, F401


@pytest.fixture
async def sessions() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Создаёт изолированную схему; разрешает очистку только тестовой базы."""
    database_url = os.environ["DATABASE_URL"]
    if not database_url.endswith("/payments_test"):
        raise RuntimeError("Tests require the dedicated payments_test database")
    engine = create_async_engine(database_url)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.execute(text("TRUNCATE payments, outbox"))
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()
