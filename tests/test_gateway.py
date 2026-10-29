from unittest.mock import AsyncMock
from uuid import UUID

from src.services.gateway import PaymentGateway


async def test_gateway_delay_and_idempotent_distribution(monkeypatch) -> None:
    """Эмулятор ждёт нужный интервал и стабильно выдаёт оба исхода примерно 90/10."""
    sleep = AsyncMock()
    delay = 2.5
    random_delay = lambda minimum, maximum: delay  # noqa: E731
    monkeypatch.setattr("src.services.gateway.asyncio.sleep", sleep)
    monkeypatch.setattr("src.services.gateway.random.uniform", random_delay)
    gateway = PaymentGateway()
    payment_id = UUID(int=1)
    first = await gateway.charge_payment(payment_id)
    repeated = await gateway.charge_payment(payment_id)
    assert first == repeated
    sleep.assert_awaited_with(delay)
    statuses = [await gateway.charge_payment(UUID(int=index)) for index in range(1000)]
    failures = statuses.count("failed")
    assert 70 < failures < 130
