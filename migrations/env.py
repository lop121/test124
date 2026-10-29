import asyncio

from alembic import context

from src.database import Base, engine
from src.models import Outbox, Payment  # noqa: F401


def run_sync_migrations(connection) -> None:
    """Применяет миграции в переданном синхронном соединении."""
    context.configure(connection=connection, target_metadata=Base.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations() -> None:
    """Выполняет миграции через асинхронный драйвер PostgreSQL."""
    async with engine.connect() as connection:
        await connection.run_sync(run_sync_migrations)
    await engine.dispose()


asyncio.run(run_migrations())
