import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy import select

from src.config import get_settings
from src.models import Outbox, Payment
from src.schemas.payments import PaymentEventDto
from src.services.payments import PaymentsService
from src.services.processing import PaymentProcessor
from src.services.webhooks import WebhookSender
from tests.test_payments import payment_request


async def create_processor(sessions, *, status="succeeded", failures=0):
    """Готовит платёж и управляемые адаптеры без ожидания эмулятора шлюза."""
    settings = get_settings()
    service = PaymentsService(sessions, settings)
    response = await service.create_payment(payment_request(), "processor-test")
    gateway = AsyncMock()
    gateway.charge_payment.return_value = status
    webhooks = AsyncMock()
    webhooks.send_payment_result.side_effect = [RuntimeError("offline")] * failures + [None]
    processor = PaymentProcessor(sessions, settings, gateway, webhooks)
    event = PaymentEventDto(payment_id=response.payment_id)
    return processor, event, gateway, webhooks


@pytest.mark.parametrize("status", ["succeeded", "failed"])
async def test_result_and_concurrent_duplicate(sessions, status) -> None:
    """Успех и отказ шлюза отправляются один раз при конкурентном дубле события."""
    processor, event, gateway, webhooks = await create_processor(sessions, status=status)
    await asyncio.gather(processor.process_payment(event), processor.process_payment(event))
    gateway.charge_payment.assert_awaited_once()
    webhooks.send_payment_result.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == status
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None
        assert payment.attempts_completed == 1


async def test_durable_retries_and_dead_letter(sessions) -> None:
    """Три неудачи создают отложенные события и DLQ без повторной оплаты."""
    processor, event, gateway, webhooks = await create_processor(sessions, failures=3)
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        started = datetime.now(UTC)
        current = event.model_copy(update={"attempt": attempt})
        await processor.process_payment(current)
        # Повтор уже подтверждённой попытки не должен тратить бюджет повторов.
        await processor.process_payment(current)
        async with sessions() as session:
            events = list((await session.scalars(select(Outbox).order_by(Outbox.created_at))).all())
            newest = events[-1]
            if attempt < max_attempts:
                expected_delay = get_settings().retry_base_seconds * 2 ** (attempt - 1)
                assert newest.available_at >= started + timedelta(seconds=expected_delay)
                assert newest.payload["attempt"] == attempt + 1
                assert newest.routing_key == "payments.new"
            else:
                assert newest.routing_key == "payments.dlq"
                assert newest.payload["error_code"] == "webhook_error"
    gateway.charge_payment.assert_awaited_once()
    assert webhooks.send_payment_result.await_count == max_attempts
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "succeeded"
        assert payment.attempts_completed == max_attempts
        assert payment.webhook_delivered_at is None


async def test_flaky_webhook_recovers_on_third_attempt(sessions) -> None:
    """Перезапуск обработчика сохраняет прогресс и успешный третий повтор."""
    processor, event, gateway, webhooks = await create_processor(sessions, failures=2)
    attempts = 3
    for attempt in range(1, attempts + 1):
        # Новый объект не хранит состояние прошлой попытки в памяти.
        restarted = PaymentProcessor(sessions, get_settings(), gateway, webhooks)
        await restarted.process_payment(event.model_copy(update={"attempt": attempt}))
    gateway.charge_payment.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.webhook_delivered_at is not None
        assert payment.attempts_completed == attempts
        assert payment.last_error is None


async def test_gateway_errors_are_bounded(sessions) -> None:
    """Ошибка адаптера шлюза тоже ограничена тремя попытками с переходом в DLQ."""
    processor, event, gateway, webhooks = await create_processor(sessions)
    gateway.charge_payment.side_effect = RuntimeError("gateway unavailable")
    attempts = 3
    for attempt in range(1, attempts + 1):
        await processor.process_payment(event.model_copy(update={"attempt": attempt}))
    assert gateway.charge_payment.await_count == attempts
    webhooks.send_payment_result.assert_not_awaited()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.status == "failed"
        assert payment.processed_at is not None
        assert payment.attempts_completed == attempts
        failed_events = list(
            (
                await session.scalars(select(Outbox).where(Outbox.routing_key == "payments.dlq"))
            ).all()
        )
        assert len(failed_events) == 1


async def test_crash_after_gateway_commit_does_not_charge_again(sessions) -> None:
    """Падение до webhook оставляет финальный статус и безопасно повторяется."""
    processor, event, gateway, webhooks = await create_processor(sessions)
    webhooks.send_payment_result.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await processor.process_payment(event)
    webhooks.send_payment_result.side_effect = None
    await processor.process_payment(event)
    gateway.charge_payment.assert_awaited_once()
    async with sessions() as session:
        payment = await session.get(Payment, event.payment_id)
        assert payment.webhook_delivered_at is not None


async def test_signed_webhook_is_stable_and_redirect_is_not_followed(sessions) -> None:
    """Подпись и тело стабильны, а redirect считается неудачной доставкой."""
    import hashlib
    import hmac

    processor, event, gateway, _ = await create_processor(sessions)
    captured = []

    def receiver(request: httpx.Request) -> httpx.Response:
        """Запоминает исходящий запрос и отвечает перенаправлением."""
        captured.append(request)
        return httpx.Response(302, headers={"Location": "http://localhost/secret"})

    settings = get_settings()
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        processor.webhooks = WebhookSender(client, settings)
        await processor.process_payment(event)
        await processor.process_payment(event.model_copy(update={"attempt": 2}))
    expected_count = 2
    assert len(captured) == expected_count
    assert captured[0].content == captured[1].content
    signature = hmac.new(
        settings.webhook_secret.get_secret_value().encode(), captured[0].content, hashlib.sha256
    ).hexdigest()
    assert captured[0].headers["X-Webhook-Signature"] == f"sha256={signature}"
    assert captured[0].headers["Idempotency-Key"] == str(event.payment_id)
    gateway.charge_payment.assert_awaited_once()
