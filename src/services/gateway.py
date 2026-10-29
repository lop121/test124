import asyncio
import hashlib
import random
from uuid import UUID

from src.schemas.payments import PaymentStatus


class PaymentGateway:
    """Эмулятор идемпотентного шлюза без настоящего списания денег."""

    async def charge_payment(self, payment_id: UUID) -> PaymentStatus:
        """Ждёт 2–5 секунд и возвращает стабильный для ключа исход 90/10."""
        await asyncio.sleep(random.uniform(2, 5))
        digest = hashlib.sha256(payment_id.bytes).digest()
        bucket = int.from_bytes(digest[:8]) % 10
        return "failed" if bucket == 0 else "succeeded"
