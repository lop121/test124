from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException

from src.config import get_settings
from src.database import session_factory
from src.schemas.field_types import IdempotencyKey
from src.schemas.payments import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentGetResponse,
)
from src.services.payments import (
    IdempotencyConflictError,
    PaymentNotFoundError,
    PaymentsService,
    WebhookNotAllowedError,
)

router = APIRouter(prefix="/api/v1/payments", tags=["Payments"])


def get_payments_service() -> PaymentsService:
    """Создаёт сервис с фабрикой сессий текущего процесса."""
    return PaymentsService(session_factory, get_settings())


@router.post("", status_code=202, response_model=PaymentCreateResponse)
async def create_payment(
    request: PaymentCreateRequest,
    idempotency_key: Annotated[IdempotencyKey, Header()],
    service: Annotated[PaymentsService, Depends(get_payments_service)],
) -> PaymentCreateResponse:
    """Принимает платёж в обработку или возвращает платёж с тем же ключом."""
    try:
        return await service.create_payment(request, idempotency_key)
    except IdempotencyConflictError as error:
        raise HTTPException(409, "Idempotency key already used for another request") from error
    except WebhookNotAllowedError as error:
        raise HTTPException(422, "Webhook destination is not allowed") from error


@router.get("/{payment_id}", response_model=PaymentGetResponse)
async def get_payment(
    payment_id: UUID,
    service: Annotated[PaymentsService, Depends(get_payments_service)],
) -> PaymentGetResponse:
    """Возвращает сохранённое состояние платежа и доставки уведомления."""
    try:
        return await service.get_payment(payment_id)
    except PaymentNotFoundError as error:
        raise HTTPException(404, "Payment not found") from error
