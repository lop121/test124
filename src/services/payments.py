from datetime import UTC, datetime
from uuid import UUID

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import Settings
from src.repositories.outbox import OutboxRepository
from src.repositories.payments import PaymentsRepository
from src.schemas.payments import (
    OutboxCreateDto,
    PaymentCreateDto,
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentEventDto,
    PaymentGetResponse,
)


class IdempotencyConflictError(Exception):
    """Ключ уже использован для другого содержимого платежа."""


class PaymentNotFoundError(Exception):
    """Платёж с указанным идентификатором отсутствует."""


class WebhookNotAllowedError(Exception):
    """Получатель не входит в разрешённый список адресов."""


class PaymentsService:
    """Атомарное создание и получение платежей."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], settings: Settings) -> None:
        """Принимает фабрику независимых транзакций и правила получателей."""
        self.sessions = sessions
        self.settings = settings

    async def create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> PaymentCreateResponse:
        """Сохраняет платёж и outbox или возвращает уже созданный платёж."""
        if (
            request.webhook_url.host not in self.settings.webhook_allowed_hosts
            or request.webhook_url.port not in (80, 443, 8080)
            or (request.webhook_url.scheme != "https" and not self.settings.webhook_allow_http)
        ):
            raise WebhookNotAllowedError
        payment_dto = PaymentCreateDto.from_request(request, idempotency_key)
        async with self.sessions.begin() as session:
            payments = PaymentsRepository(session)
            payment = await payments.create(payment_dto)
            created = payment is not None
            if payment is None:
                payment = await payments.get_by_idempotency_key(idempotency_key)
                if payment.request_hash != payment_dto.request_hash:
                    raise IdempotencyConflictError
            else:
                event = PaymentEventDto(payment_id=payment.id)
                OutboxRepository(session).create(
                    OutboxCreateDto(
                        routing_key="payments.new",
                        payload=event.model_dump(mode="json"),
                        available_at=datetime.now(UTC),
                    )
                )
            response = PaymentCreateResponse.from_row(payment)
        if created:
            logger.bind(event="payment_created", payment_id=str(payment.id)).info(
                "Payment accepted"
            )
        return response

    async def get_payment(self, payment_id: UUID) -> PaymentGetResponse:
        """Возвращает подробности существующего платежа."""
        async with self.sessions() as session:
            payment = await PaymentsRepository(session).get_by_id(payment_id)
            if payment is None:
                raise PaymentNotFoundError
            return PaymentGetResponse.from_row(payment)
