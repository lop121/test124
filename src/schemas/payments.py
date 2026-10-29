import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from src.models.payment import Payment
from src.schemas.field_types import Description, MoneyAmount

Currency = Literal["RUB", "USD", "EUR"]
PaymentStatus = Literal["pending", "succeeded", "failed"]


class PaymentCreateRequest(BaseModel):
    """Параметры создания платежа без технического ключа идемпотентности."""

    model_config = ConfigDict(extra="forbid")

    amount: MoneyAmount
    currency: Currency
    description: Description = ""
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("metadata")
    @classmethod
    def validate_metadata_size(cls, value: dict) -> dict:
        """Ограничивает размер метаданных для хранения и отправки уведомлений."""
        if len(json.dumps(value, allow_nan=False).encode()) > 16_384:
            raise ValueError("Metadata must not exceed 16 KiB")
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: HttpUrl) -> HttpUrl:
        """Запрещает секреты и фрагменты в адресе получателя."""
        if value.username or value.password or value.fragment:
            raise ValueError("Webhook URL must not contain credentials or a fragment")
        return value

    def get_request_hash(self) -> str:
        """Считает отпечаток нормализованного запроса без различий масштаба суммы."""
        payload = self.model_dump(mode="json")
        payload["amount"] = format(self.amount.quantize(Decimal("0.01")), "f")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode()).hexdigest()


class PaymentCreateDto(BaseModel):
    """Данные для атомарной вставки платежа репозиторием."""

    amount: Decimal
    currency: Currency
    description: str
    payment_metadata: dict
    webhook_url: str
    idempotency_key: str
    request_hash: str

    @classmethod
    def from_request(cls, request: PaymentCreateRequest, idempotency_key: str) -> Self:
        """Дополняет запрос техническими полями сохранения платежа."""
        return cls(
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            payment_metadata=request.metadata,
            webhook_url=str(request.webhook_url),
            idempotency_key=idempotency_key,
            request_hash=request.get_request_hash(),
        )


class PaymentSummaryDto(BaseModel):
    """Общие поля подтверждения создания и подробностей платежа."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_row(cls, payment: Payment) -> Self:
        """Формирует краткий ответ из сохранённого платежа."""
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentCreateResponse(PaymentSummaryDto):
    """Подтверждение принятого платежа, включая повторный запрос."""


class PaymentGetResponse(PaymentSummaryDto):
    """Подробности платежа и состояние доставки результата."""

    amount: Decimal
    currency: Currency
    description: str
    metadata: dict
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    attempts_completed: int

    @classmethod
    def from_row(cls, payment: Payment) -> Self:
        """Формирует публичные поля без ключа и служебных ошибок."""
        return cls(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.payment_metadata,
            webhook_url=payment.webhook_url,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
            attempts_completed=payment.attempts_completed,
        )


class PaymentEventDto(BaseModel):
    """Версионированное сообщение одной попытки обработки платежа."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    payment_id: UUID
    attempt: int = Field(default=1, ge=1, le=3)


class OutboxCreateDto(BaseModel):
    """Данные события с временем следующей публикации."""

    routing_key: str
    payload: dict
    available_at: datetime
