"""Одноразовая настройка политики RabbitMQ перед запуском рабочих процессов."""

import asyncio
from urllib.parse import unquote, urlsplit

import httpx

from src.config import get_settings


async def main() -> None:
    """Отделяет инфраструктурный requeue от трёх сохранённых бизнес-попыток."""
    settings = get_settings()
    address = urlsplit(settings.rabbitmq_url.get_secret_value())
    async with httpx.AsyncClient(trust_env=False, timeout=10) as client:
        response = await client.put(
            settings.rabbitmq_management_url + "/api/policies/%2F/payments-recovery",
            auth=(unquote(address.username or ""), unquote(address.password or "")),
            json={
                "pattern": "^payments\\.new$",
                "definition": {"delivery-limit": -1},
                "priority": 1,
                "apply-to": "quorum_queues",
            },
        )
        response.raise_for_status()


if __name__ == "__main__":
    asyncio.run(main())
