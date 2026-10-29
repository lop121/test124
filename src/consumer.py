import asyncio
from typing import Any

import httpx
from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import Channel
from faststream.rabbit.annotations import RabbitMessage
from loguru import logger
from pydantic import ValidationError

from src.config import get_settings
from src.database import engine, session_factory
from src.logging import configure_logging
from src.messaging import create_broker, declare_topology, exchange, work_queue
from src.schemas.payments import PaymentEventDto
from src.services.gateway import PaymentGateway
from src.services.processing import PaymentEventNotFoundError, PaymentProcessor
from src.services.webhooks import WebhookSender

settings = get_settings()
broker = create_broker()
app = FastStream(broker, logger=None)
http_client = httpx.AsyncClient(timeout=settings.webhook_timeout, trust_env=False)
processor = PaymentProcessor(
    session_factory, settings, PaymentGateway(), WebhookSender(http_client, settings)
)


async def decode_payment_body(message: Any) -> bytes:
    """Передаёт сырое тело в обработчик, чтобы повреждённый JSON тоже ушёл в DLQ."""
    return message.body


@broker.subscriber(
    work_queue,
    exchange,
    channel=Channel(prefetch_count=1),
    ack_policy=AckPolicy.MANUAL,
    decoder=decode_payment_body,
)
async def consume_payment(body: Any, message: RabbitMessage) -> None:
    """Выполняет весь сценарий и подтверждает сообщение после фиксации прогресса."""
    try:
        event = (
            PaymentEventDto.model_validate_json(body)
            if isinstance(body, (bytes, str))
            else PaymentEventDto.model_validate(body)
        )
        await processor.process_payment(event)
    except (ValidationError, PaymentEventNotFoundError) as error:
        logger.bind(event="payment_message_rejected", error_type=type(error).__name__).error(
            "Invalid payment event sent to DLQ"
        )
        await message.reject(requeue=False)
    except Exception as error:
        # При недоступной БД невозможно безопасно сохранить счётчик попыток.
        logger.bind(event="payment_consumer_failed", error_type=type(error).__name__).error(
            "Infrastructure unavailable; message will be redelivered"
        )
        await asyncio.sleep(settings.retry_base_seconds)
        await message.nack(requeue=True)
    else:
        await message.ack()


@app.on_startup
async def startup() -> None:
    """Настраивает журналирование и топологию до подключения обработчика."""
    configure_logging()
    await broker.connect()
    await declare_topology(broker)


@app.after_shutdown
async def shutdown() -> None:
    """Освобождает HTTP-соединения и пул базы данных."""
    await http_client.aclose()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(app.run())
