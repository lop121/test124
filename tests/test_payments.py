import asyncio
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from src.config import get_settings
from src.main import app
from src.models import Outbox, Payment
from src.routers.payments import get_payments_service
from src.schemas.payments import PaymentCreateRequest
from src.services.payments import IdempotencyConflictError, PaymentsService


def payment_request(**changes) -> PaymentCreateRequest:
    """Готовит валидный запрос платежа для текущего тестового сценария."""
    return PaymentCreateRequest.model_validate(
        {
            "amount": "100.50",
            "currency": "RUB",
            "metadata": {"order": 7},
            "webhook_url": "http://demo-webhook:8080/success",
            **changes,
        }
    )


async def test_concurrent_idempotency_and_atomic_outbox(sessions) -> None:
    """Конкурентные запросы создают ровно один платёж и одно событие."""
    request = payment_request()
    key = "concurrent-key"
    expected_count = 1
    service = PaymentsService(sessions, get_settings())
    responses = await asyncio.gather(*[service.create_payment(request, key) for _ in range(12)])
    assert len({response.payment_id for response in responses}) == expected_count
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == expected_count
        assert await session.scalar(select(func.count()).select_from(Outbox)) == expected_count


async def test_conflicting_key_and_equivalent_amount(sessions) -> None:
    """Одинаковые суммы переиспользуют платёж, а изменённый запрос конфликтует."""
    key = "same-key"
    amount = "100.50"
    service = PaymentsService(sessions, get_settings())
    first = await service.create_payment(payment_request(amount=amount), key)
    repeated = await service.create_payment(payment_request(amount="100.500"), key)
    assert first.payment_id == repeated.payment_id
    with pytest.raises(IdempotencyConflictError):
        await service.create_payment(payment_request(amount="101.50"), key)


async def test_payment_and_outbox_roll_back_together(sessions, monkeypatch) -> None:
    """Ошибка сохранения outbox откатывает вставку платежа."""
    from src.repositories.outbox import OutboxRepository

    def fail_outbox(*args, **kwargs) -> None:
        """Эмулирует сбой сохранения события до фиксации транзакции."""
        raise RuntimeError("outbox failure")

    monkeypatch.setattr(OutboxRepository, "create", fail_outbox)
    service = PaymentsService(sessions, get_settings())
    with pytest.raises(RuntimeError):
        await service.create_payment(payment_request(), "rollback-key")
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


async def test_http_contracts_and_authentication(sessions) -> None:
    """Проверяет реальные маршруты, авторизацию, ошибки и сериализацию Decimal."""
    service = PaymentsService(sessions, get_settings())
    app.dependency_overrides[get_payments_service] = lambda: service
    key = get_settings().api_key.get_secret_value()
    headers = {"X-API-Key": key, "Idempotency-Key": "http-test"}
    route = "/api/v1/payments"
    accepted = 202
    unauthorized = 401
    invalid = 422
    payload = payment_request().model_dump(mode="json")
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            for path in (route, "/health", "/openapi.json"):
                assert (await client.get(path)).status_code == unauthorized
                assert (
                    await client.get(path, headers={"X-API-Key": "wrong"})
                ).status_code == unauthorized
            response = await client.post(route, json=payload, headers=headers)
            assert response.status_code == accepted
            payment_id = response.json()["payment_id"]
            repeated = await client.post(route, json=payload, headers=headers)
            assert repeated.status_code == accepted
            assert repeated.json()["payment_id"] == payment_id
            detail = await client.get(f"{route}/{payment_id}", headers=headers)
            assert detail.status_code == 200
            assert Decimal(detail.json()["amount"]) == Decimal(payload["amount"])
            assert detail.json()["metadata"] == payload["metadata"]
            assert "idempotency_key" not in detail.json()
            assert (await client.get(f"{route}/{uuid4()}", headers=headers)).status_code == 404
            assert (await client.get(f"{route}/invalid", headers=headers)).status_code == invalid
            assert (
                await client.post(route, json=payload, headers={"X-API-Key": key})
            ).status_code == invalid
            conflict = payload | {"description": "different"}
            assert (await client.post(route, json=conflict, headers=headers)).status_code == 409
            for changes in (
                {"amount": "0"},
                {"amount": "-1"},
                {"amount": "1.001"},
                {"amount": "NaN"},
                {"currency": "GBP"},
                {"metadata": {"x": "x" * 17_000}},
                {"webhook_url": "http://localhost:8080"},
                {"webhook_url": "https://user:secret@webhook.site/test"},
                {"webhook_url": "ftp://webhook.site/test"},
                {"webhook_url": "https://webhook.site:1234/test"},
            ):
                bad = await client.post(route, json=payload | changes, headers=headers)
                assert bad.status_code == invalid
    finally:
        app.dependency_overrides.clear()
