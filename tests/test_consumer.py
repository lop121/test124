from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

import src.consumer as consumer
from src.services.processing import PaymentEventNotFoundError


async def test_consumer_ack_after_saved_progress(monkeypatch) -> None:
    """Подтверждение происходит только после успешного завершения сценария."""
    processor = AsyncMock()
    message = AsyncMock()
    monkeypatch.setattr(consumer, "processor", processor)
    payment_id = str(uuid4())
    await consumer.consume_payment({"version": 1, "payment_id": payment_id, "attempt": 1}, message)
    processor.process_payment.assert_awaited_once()
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


@pytest.mark.parametrize("unknown_payment", [False, True])
async def test_consumer_rejects_invalid_event(monkeypatch, unknown_payment) -> None:
    """Невалидное событие и неизвестный платёж отклоняются без бесконечного requeue."""
    processor = AsyncMock()
    message = AsyncMock()
    monkeypatch.setattr(consumer, "processor", processor)
    body = {"version": 1, "payment_id": str(uuid4()), "attempt": 1}
    if unknown_payment:
        processor.process_payment.side_effect = PaymentEventNotFoundError()
    else:
        body = {"invalid": True}
    await consumer.consume_payment(body, message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()


async def test_consumer_requeues_on_infrastructure_failure(monkeypatch) -> None:
    """Недоступность БД не подтверждает сообщение и не расходует бизнес-попытку."""
    processor = AsyncMock()
    processor.process_payment.side_effect = ConnectionError("database unavailable")
    message = AsyncMock()
    monkeypatch.setattr(consumer, "processor", processor)
    monkeypatch.setattr(consumer.asyncio, "sleep", AsyncMock())
    await consumer.consume_payment(
        {"version": 1, "payment_id": str(uuid4()), "attempt": 1}, message
    )
    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_consumer_rejects_broken_json(monkeypatch) -> None:
    """Повреждённый JSON доходит до обработчика и не блокирует prefetch=1."""
    from types import SimpleNamespace

    processor = AsyncMock()
    message = AsyncMock()
    broken_body = b"{broken-json"
    monkeypatch.setattr(consumer, "processor", processor)
    decoded = await consumer.decode_payment_body(SimpleNamespace(body=broken_body))
    assert decoded == broken_body
    await consumer.consume_payment(decoded, message)
    processor.process_payment.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
