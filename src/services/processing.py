from datetime import UTC, datetime, timedelta

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import Settings
from src.models.payment import Payment
from src.repositories.outbox import OutboxRepository
from src.repositories.payments import PaymentsRepository
from src.schemas.payments import OutboxCreateDto, PaymentEventDto
from src.services.gateway import PaymentGateway
from src.services.webhooks import WebhookSender

MAX_ATTEMPTS = 3


class PaymentEventNotFoundError(Exception):
    """Сообщение ссылается на несуществующий платёж."""


class PaymentProcessor:
    """Обработка шлюза и webhook одним обработчиком с сохранением прогресса."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        settings: Settings,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
    ) -> None:
        """Принимает транзакции и внешние адаптеры для одной обработки."""
        self.sessions = sessions
        self.settings = settings
        self.gateway = gateway
        self.webhooks = webhooks

    @staticmethod
    def is_completed(payment: Payment, event: PaymentEventDto) -> bool:
        """Распознаёт дубли, устаревшие попытки и завершённые уведомления."""
        return (
            payment.webhook_delivered_at is not None
            or event.attempt != payment.attempts_completed + 1
        )

    def schedule_retry_or_dead_letter(
        self,
        payment: Payment,
        event: PaymentEventDto,
        outbox: OutboxRepository,
        error_code: str,
    ) -> None:
        """Атомарно сохраняет прогресс и следующее событие либо запись для DLQ."""
        payment.attempts_completed = event.attempt
        payment.last_error = error_code
        now = datetime.now(UTC)
        exhausted = event.attempt == MAX_ATTEMPTS
        if exhausted:
            if payment.status == "pending":
                payment.status = "failed"
                payment.processed_at = now
            payload = event.model_dump(mode="json") | {"error_code": error_code}
            routing_key = "payments.dlq"
            available_at = now
        else:
            payload = event.model_copy(update={"attempt": event.attempt + 1}).model_dump(
                mode="json"
            )
            routing_key = "payments.new"
            delay = self.settings.retry_base_seconds * 2 ** (event.attempt - 1)
            available_at = now + timedelta(seconds=delay)
        outbox.create(
            OutboxCreateDto(routing_key=routing_key, payload=payload, available_at=available_at)
        )

    async def process_payment(self, event: PaymentEventDto) -> None:
        """Сохраняет исход шлюза до отправки и подтверждает только надёжный прогресс."""
        gateway_error = False
        processed = False
        async with self.sessions.begin() as session:
            payment = await PaymentsRepository(session).lock_by_id(event.payment_id)
            if payment is None:
                raise PaymentEventNotFoundError
            if self.is_completed(payment, event):
                return
            if payment.status == "pending":
                try:
                    payment.status = await self.gateway.charge_payment(payment.id)
                    payment.processed_at = datetime.now(UTC)
                    processed = True
                except Exception:
                    gateway_error = True
                    self.schedule_retry_or_dead_letter(
                        payment, event, OutboxRepository(session), "gateway_error"
                    )
        bound_logger = logger.bind(payment_id=str(event.payment_id), attempt=event.attempt)
        if processed:
            bound_logger.bind(event="payment_processed", status=payment.status).info(
                "Payment result saved"
            )
        if gateway_error:
            bound_logger.bind(event="payment_processing_failed").error(
                "Gateway attempt failed; next action saved"
            )
            return

        async with self.sessions.begin() as session:
            payment = await PaymentsRepository(session).lock_by_id(event.payment_id)
            if payment is None:
                raise PaymentEventNotFoundError
            if self.is_completed(payment, event):
                return
            try:
                await self.webhooks.send_payment_result(payment)
            except Exception:
                self.schedule_retry_or_dead_letter(
                    payment, event, OutboxRepository(session), "webhook_error"
                )
                delivered = False
            else:
                payment.webhook_delivered_at = datetime.now(UTC)
                payment.attempts_completed = event.attempt
                payment.last_error = None
                delivered = True
        if delivered:
            bound_logger.bind(event="payment_webhook_delivered").info("Payment webhook delivered")
        else:
            bound_logger.bind(
                event="payment_webhook_failed", exhausted=event.attempt == MAX_ATTEMPTS
            ).error("Webhook attempt failed; next action saved")
