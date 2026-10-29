import hashlib
import hmac
import json

import httpx

from src.config import Settings
from src.models.payment import Payment
from src.schemas.payments import PaymentGetResponse


class WebhookSender:
    """Отправка подписанного результата без редиректов и переменных прокси."""

    def __init__(self, client: httpx.AsyncClient, settings: Settings) -> None:
        """Принимает общий HTTP-клиент и секрет подписи."""
        self.client = client
        self.settings = settings

    async def send_payment_result(self, payment: Payment) -> None:
        """Отправляет стабильное тело и ключ дедупликации получателю платежа."""
        payload = PaymentGetResponse.from_row(payment).model_dump(mode="json")
        # Прогресс доставки не входит в подписанное бизнес-событие.
        for field in ("attempts_completed", "webhook_delivered_at", "webhook_url"):
            payload.pop(field)
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        signature = hmac.new(
            self.settings.webhook_secret.get_secret_value().encode(), body, hashlib.sha256
        ).hexdigest()
        response = await self.client.post(
            payment.webhook_url,
            content=body,
            headers={
                "Content-Type": "application/json",
                "Idempotency-Key": str(payment.id),
                "X-Webhook-Signature": f"sha256={signature}",
            },
            follow_redirects=False,
        )
        response.raise_for_status()
