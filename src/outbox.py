import asyncio
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import get_settings
from src.database import engine, session_factory
from src.logging import configure_logging
from src.messaging import create_broker, dead_exchange, declare_topology, exchange
from src.repositories.outbox import OutboxRepository


async def publish_next_event(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker
) -> bool:
    """Публикует событие с confirm перед отметкой в БД; допускает безопасные дубли."""
    async with sessions.begin() as session:
        event = await OutboxRepository(session).lock_next_ready()
        if event is None:
            return False
        try:
            confirmation = await broker.publish(
                event.payload,
                exchange=dead_exchange if event.routing_key == "payments.dlq" else exchange,
                routing_key=event.routing_key,
                message_id=str(event.id),
                persist=True,
                mandatory=True,
                timeout=5,
            )
            if confirmation is None or confirmation.name != "Basic.Ack":
                raise RuntimeError("Publisher confirmation missing")
        except Exception as error:
            event.publish_attempts += 1
            delay = min(60, 2 ** min(event.publish_attempts, 6))
            event.available_at = datetime.now(UTC) + timedelta(seconds=delay)
            logger.bind(
                event="outbox_publication_failed",
                outbox_id=str(event.id),
                error_type=type(error).__name__,
            ).error("Event remains pending for publication")
        else:
            event.published_at = datetime.now(UTC)
    return True


async def main() -> None:
    """Запускает отдельный издатель outbox с восстановлением после сбоев."""
    configure_logging()
    settings = get_settings()
    broker = create_broker()
    try:
        await broker.connect()
        await declare_topology(broker)
        while True:
            try:
                published = await publish_next_event(session_factory, broker)
            except Exception as error:
                logger.bind(event="outbox_poll_failed", error_type=type(error).__name__).error(
                    "Outbox unavailable; polling will resume"
                )
                published = False
            if not published:
                await asyncio.sleep(settings.outbox_poll_seconds)
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
