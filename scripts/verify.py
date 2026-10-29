"""E2E-проверки через HTTP и настоящий брокер; запускаются scripts/check.sh."""

import asyncio
import hashlib
import json
import os
import sys
import time
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, select

from src.config import get_settings
from src.database import engine, session_factory
from src.models import Outbox, Payment
from src.repositories.outbox import OutboxRepository
from src.schemas.payments import (
    OutboxCreateDto,
    PaymentCreateDto,
    PaymentCreateRequest,
    PaymentEventDto,
)

settings = get_settings()
run_id = os.environ.get("VERIFY_RUN_ID", "manual-" + uuid4().hex)
base_url = os.environ.get("API_BASE_URL", "http://api:8000")
management_url = os.environ.get("RABBITMQ_MANAGEMENT_URL", "http://rabbitmq:15672")
route = "/api/v1/payments"
results: list[str] = []


def check(condition: bool, description: str) -> None:
    """Останавливает сценарий при нарушении контракта и печатает только итог."""
    if not condition:
        raise AssertionError(description)
    results.append(description)
    print(f"PASS: {description}", flush=True)


def payload(path: str = "success", currency: str = "RUB") -> dict:
    """Готовит публичный запрос с локальным получателем webhook."""
    return {
        "amount": "123.45",
        "currency": currency,
        "description": "E2E payment",
        "metadata": {"run": run_id, "nested": {"items": [1, "two", None]}},
        "webhook_url": f"http://demo-webhook:8080/{path}",
    }


async def create_payment(
    client: httpx.AsyncClient, scenario: str, body: dict | None = None
) -> dict:
    """Создаёт проверяемый платёж через API с уникальным ключом сценария."""
    response = await client.post(
        route, json=body or payload(), headers={"Idempotency-Key": f"{run_id}-{scenario}"}
    )
    check(response.status_code == 202, f"{scenario}: POST 202")
    payment = response.json()
    UUID(payment["payment_id"])
    return payment


async def wait_payment(
    client: httpx.AsyncClient, payment_id: str, *, attempts: int | None = None
) -> dict:
    """Ожидает завершения доставки или исчерпания попыток через GET API."""
    deadline = time.monotonic() + 100
    while time.monotonic() < deadline:
        response = await client.get(f"{route}/{payment_id}")
        if response.status_code != 200:
            raise AssertionError(f"GET payment failed: {response.status_code}")
        payment = response.json()
        if payment["webhook_delivered_at"] is not None or (
            attempts is not None and payment["attempts_completed"] == attempts
        ):
            return payment
        await asyncio.sleep(0.2)
    raise AssertionError(f"Payment did not finish: {payment_id}")


async def get_saved_payment(scenario: str) -> Payment:
    """Находит только платёж текущего запуска для межпроцессной проверки сбоя."""
    async with session_factory() as session:
        return (
            await session.scalars(
                select(Payment).where(Payment.idempotency_key == f"{run_id}-{scenario}")
            )
        ).one()


async def seed_payment(scenario: str, status: str, path: str) -> str:
    """Готовит минимальный платёж с предсказуемым шлюзом непосредственно в БД."""
    while True:
        payment_id = uuid4()
        failed = int.from_bytes(hashlib.sha256(payment_id.bytes).digest()[:8]) % 10 == 0
        if failed == (status == "failed"):
            break
    request = PaymentCreateRequest.model_validate(payload(path))
    payment = Payment(
        id=payment_id, **PaymentCreateDto.from_request(request, f"{run_id}-{scenario}").model_dump()
    )
    async with session_factory.begin() as session:
        session.add(payment)
        event = PaymentEventDto(payment_id=payment_id)
        OutboxRepository(session).create(
            OutboxCreateDto(
                routing_key="payments.new",
                payload=event.model_dump(mode="json"),
                available_at=datetime.now(UTC),
            )
        )
    return str(payment_id)


async def rabbit_request(
    client: httpx.AsyncClient, method: str, path: str, **kwargs
) -> httpx.Response:
    """Обращается к локальному management API без вывода учётных данных."""
    return await client.request(
        method,
        management_url + path,
        auth=(os.environ["RABBITMQ_USER"], os.environ["RABBITMQ_PASSWORD"]),
        **kwargs,
    )


async def publish_message(client: httpx.AsyncClient, body: dict | str) -> None:
    """Публикует тестовый дубль или некорректное сообщение через HTTP брокера."""
    response = await rabbit_request(
        client,
        "POST",
        "/api/exchanges/%2F/payments/publish",
        json={
            "properties": {"delivery_mode": 2, "content_type": "application/json"},
            "routing_key": "payments.new",
            "payload": body if isinstance(body, str) else json.dumps(body),
            "payload_encoding": "string",
        },
    )
    check(response.status_code == 200 and response.json()["routed"], "RabbitMQ routed test event")


async def contracts(client: httpx.AsyncClient) -> None:
    """Проверяет авторизацию, валидацию, идемпотентность и сериализацию API."""
    health = await client.get("/health")
    check(
        health.status_code == 200 and health.json() == {"status": "ok"}, "Authenticated health: 200"
    )
    schema = await client.get("/openapi.json")
    check(
        schema.status_code == 200 and schema.json()["security"] == [{"ApiKey": []}],
        "Authenticated OpenAPI: security scheme",
    )
    async with httpx.AsyncClient(base_url=base_url, trust_env=False) as anonymous:
        for path in (route, "/health", "/openapi.json"):
            for headers in ({}, {"X-API-Key": "wrong"}):
                check(
                    (await anonymous.get(path, headers=headers)).status_code == 401,
                    f"{path}: unauthorized 401",
                )
    for currency in ("RUB", "USD", "EUR"):
        body = payload(currency=currency)
        created = await create_payment(client, currency, body)
        payment_id = created["payment_id"]
        check(
            created["status"] == "pending" and bool(created["created_at"]),
            f"{currency}: pending and timestamp",
        )
        response = await client.get(f"{route}/{payment_id}")
        check(
            response.status_code == 200 and response.json()["currency"] == currency,
            f"{currency}: GET 200 and currency",
        )
        detail = response.json()
        check(
            Decimal(detail["amount"]) == Decimal(body["amount"])
            and detail["metadata"] == body["metadata"],
            f"{currency}: Decimal and nested JSON",
        )
    body = payload()
    headers = {"Idempotency-Key": f"{run_id}-concurrent"}
    responses = await asyncio.gather(
        *[client.post(route, json=body, headers=headers) for _ in range(10)]
    )
    payment_ids = {response.json()["payment_id"] for response in responses}
    check(
        all(response.status_code == 202 for response in responses) and len(payment_ids) == 1,
        "Concurrent POSTs return one payment",
    )
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(Payment)
            .where(Payment.idempotency_key == headers["Idempotency-Key"])
        )
        event_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.payload["payment_id"].astext == next(iter(payment_ids)))
        )
    check(count == 1 and event_count == 1, "One payment and one transactional outbox event")
    equivalent = await client.post(route, json=body | {"amount": "123.450"}, headers=headers)
    check(
        equivalent.status_code == 202 and equivalent.json()["payment_id"] in payment_ids,
        "Equivalent Decimal reuses payment",
    )
    conflict = await client.post(route, json=body | {"amount": "124.45"}, headers=headers)
    check(conflict.status_code == 409, "Different payload for same key: 409")
    for key in (None, "", "x" * 129):
        check(
            (
                await client.post(
                    route, json=body, headers={} if key is None else {"Idempotency-Key": key}
                )
            ).status_code
            == 422,
            "Invalid idempotency header: 422",
        )
    for changes in (
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "10000000000000"},
        {"amount": "NaN"},
        {"currency": "GBP"},
        {"metadata": {"large": "a" * 17_000}},
        {"unexpected": True},
        {"webhook_url": "not-a-url"},
        {"webhook_url": "http://localhost:8080/"},
        {"webhook_url": "https://user:secret@webhook.site/a"},
        {"webhook_url": "https://webhook.site/a#fragment"},
        {"webhook_url": "https://webhook.site:1234/a"},
    ):
        response = await client.post(
            route, json=body | changes, headers={"Idempotency-Key": uuid4().hex}
        )
        check(response.status_code == 422, f"Invalid {next(iter(changes))}: 422")
    defaults = {key: value for key, value in body.items() if key not in ("description", "metadata")}
    created = await create_payment(client, "defaults", defaults)
    detail = (await client.get(f"{route}/{created['payment_id']}")).json()
    check(
        detail["description"] == "" and detail["metadata"] == {},
        "Optional fields default correctly",
    )
    maximum_amount = "9999999999999.99"
    maximum = await create_payment(client, "maximum", body | {"amount": maximum_amount})
    maximum_detail = await client.get(f"{route}/{maximum['payment_id']}")
    check(
        maximum_detail.status_code == 200 and maximum_detail.json()["amount"] == maximum_amount,
        "Maximum amount preserved exactly",
    )
    check((await client.get(f"{route}/{uuid4()}")).status_code == 404, "Unknown payment: 404")
    check((await client.get(f"{route}/invalid")).status_code == 422, "Invalid payment UUID: 422")
    for scenario in ("RUB", "USD", "EUR", "concurrent", "defaults", "maximum"):
        saved = await get_saved_payment(scenario)
        final = await wait_payment(client, str(saved.id))
        check(
            final["status"] in ("succeeded", "failed")
            and final["webhook_delivered_at"] is not None,
            f"{scenario}: asynchronous completion",
        )


async def processing(client: httpx.AsyncClient) -> None:
    """Проверяет исходы шлюза, HTTP-повторы, redirect, дубли и DLQ."""
    examples = [
        ("success", "succeeded", "success", 1),
        ("decline", "failed", "success", 1),
        ("flaky", "succeeded", "flaky", 3),
        ("fail", "succeeded", "fail", 3),
        ("redirect", "succeeded", "redirect", 3),
    ]
    for scenario, status, path, attempts in examples:
        payment_id = await seed_payment(scenario, status, path)
        final = await wait_payment(client, payment_id, attempts=attempts)
        check(
            final["status"] == status and bool(final["processed_at"]),
            f"{scenario}: expected gateway result",
        )
        check(final["attempts_completed"] == attempts, f"{scenario}: expected attempt count")
        delivered = path not in ("fail", "redirect")
        check(bool(final["webhook_delivered_at"]) == delivered, f"{scenario}: delivery state")
        received = (await client.get("http://demo-webhook:8080/records")).json()[payment_id]
        check(
            len(received) == attempts
            and all(item["body"]["status"] == status for item in received),
            f"{scenario}: signed HTTP webhooks received",
        )
        if attempts > 1:
            intervals = [
                received[index]["received_at"] - received[index - 1]["received_at"]
                for index in range(1, len(received))
            ]
            check(intervals[0] >= 1.9 and intervals[1] >= 3.9, f"{scenario}: HTTP retry intervals")
            async with session_factory() as session:
                events = list(
                    (
                        await session.scalars(
                            select(Outbox)
                            .where(Outbox.payload["payment_id"].astext == payment_id)
                            .order_by(Outbox.created_at)
                        )
                    ).all()
                )
            delays = [
                (row.available_at - row.created_at).total_seconds()
                for row in events
                if row.routing_key == "payments.new" and row.payload["attempt"] > 1
            ]
            check(
                len(delays) == 2 and delays[0] >= 1.9 and delays[1] >= 3.9,
                f"{scenario}: exponential durable delays",
            )
        await publish_message(client, {"version": 1, "payment_id": payment_id, "attempt": 1})
        await asyncio.sleep(0.5)
        repeated = (await client.get("http://demo-webhook:8080/records")).json()[payment_id]
        check(len(repeated) == attempts, f"{scenario}: duplicate does not resend webhook")
    await publish_message(client, {"invalid": "message"})
    await publish_message(client, "{broken-json")
    unknown_id = str(uuid4())
    await publish_message(client, {"version": 1, "payment_id": unknown_id, "attempt": 1})
    deadline = time.monotonic() + 15
    found = set()
    malformed = False
    malformed_json = False
    while time.monotonic() < deadline:
        response = await rabbit_request(
            client,
            "POST",
            "/api/queues/%2F/payments.dlq/get",
            json={"count": 100, "ackmode": "ack_requeue_true", "encoding": "auto"},
        )
        if response.status_code != 200:
            raise AssertionError("Could not inspect DLQ")
        for message in response.json():
            try:
                event = json.loads(message["payload"])
            except json.JSONDecodeError:
                malformed_json = True
                continue
            malformed |= "invalid" in event
            if "payment_id" in event:
                found.add(event["payment_id"])
        expected = {str((await get_saved_payment(name)).id) for name in ("fail", "redirect")} | {
            unknown_id
        }
        if expected <= found and malformed and malformed_json:
            break
        await asyncio.sleep(0.5)
    check(
        expected <= found and malformed and malformed_json,
        "DLQ contains exhausted, malformed and unknown-payment messages",
    )


async def outage(client: httpx.AsyncClient, action: str, scenario: str) -> None:
    """Проверяет приём pending и восстановление после внешней остановки процесса."""
    if action == "prepare":
        created = await create_payment(client, scenario)
        detail = await client.get(f"{route}/{created['payment_id']}")
        check(
            detail.status_code == 200 and detail.json()["status"] == "pending",
            f"{scenario}: accepted while unavailable",
        )
    elif action == "inflight":
        saved = await get_saved_payment(scenario)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            queue = await rabbit_request(client, "GET", "/api/queues/%2F/payments.new")
            detail = await client.get(f"{route}/{saved.id}")
            if queue.status_code == 200 and queue.json().get("messages_unacknowledged", 0) > 0:
                check(detail.status_code == 200, f"{scenario}: GET during processing")
                return
            await asyncio.sleep(0.1)
        raise AssertionError("Consumer did not receive an unacknowledged event")
    elif action == "pending":
        saved = await get_saved_payment(scenario)
        detail = await client.get(f"{route}/{saved.id}")
        check(
            detail.status_code == 200 and detail.json()["status"] == "pending",
            f"{scenario}: remains pending during outage",
        )
    else:
        saved = await get_saved_payment(scenario)
        final = await wait_payment(client, str(saved.id))
        check(
            final["status"] in ("succeeded", "failed") and bool(final["webhook_delivered_at"]),
            f"{scenario}: recovered and delivered",
        )


async def main() -> None:
    """Выбирает проверку и завершает процесс ненулевым кодом при любом нарушении."""
    mode = sys.argv[1] if len(sys.argv) > 1 else "contracts"
    try:
        async with httpx.AsyncClient(
            base_url=base_url,
            headers={"X-API-Key": settings.api_key.get_secret_value()},
            timeout=10,
            trust_env=False,
        ) as client:
            if mode == "contracts":
                await contracts(client)
            elif mode == "processing":
                await processing(client)
            elif mode == "outage":
                await outage(client, sys.argv[2], sys.argv[3])
            else:
                raise ValueError("Unknown verification mode")
        print(f"Verified {len(results)} assertions ({mode})", flush=True)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
