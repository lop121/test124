from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from pamqp.commands import Basic
from sqlalchemy import select

from src.config import get_settings
from src.models import Outbox
from src.outbox import publish_next_event
from src.services.payments import PaymentsService
from tests.test_payments import payment_request


async def test_outbox_waits_for_confirmation_and_recovers(sessions) -> None:
    """Сбой брокера не теряет событие, а подтверждённая публикация отмечается в БД."""
    service = PaymentsService(sessions, get_settings())
    await service.create_payment(payment_request(), "outbox-test")
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("offline")
    assert await publish_next_event(sessions, broker)
    async with sessions.begin() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is None
        assert event.publish_attempts == 1
        assert event.available_at > datetime.now(UTC)
        event.available_at = datetime.now(UTC)
    broker.publish.side_effect = None
    broker.publish.return_value = Basic.Ack()
    assert await publish_next_event(sessions, broker)
    assert not await publish_next_event(sessions, broker)
    async with sessions() as session:
        event = await session.scalar(select(Outbox))
        assert event.published_at is not None
        assert broker.publish.call_args.kwargs["message_id"] == str(event.id)
        assert broker.publish.call_args.kwargs["persist"] is True
        assert broker.publish.call_args.kwargs["mandatory"] is True


async def test_crash_after_publish_before_commit_republishes_same_event(sessions) -> None:
    """При падении после отправки событие повторяется с прежним message_id."""
    import asyncio

    service = PaymentsService(sessions, get_settings())
    await service.create_payment(payment_request(), "outbox-crash")
    broker = AsyncMock()
    broker.publish.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await publish_next_event(sessions, broker)
    message_id = broker.publish.call_args.kwargs["message_id"]
    broker.publish.side_effect = None
    broker.publish.return_value = Basic.Ack()
    assert await publish_next_event(sessions, broker)
    assert broker.publish.call_args.kwargs["message_id"] == message_id


async def test_two_outbox_publishers_skip_locked_rows(sessions) -> None:
    """Второй издатель пропускает событие, уже заблокированное первым."""
    import asyncio

    service = PaymentsService(sessions, get_settings())
    await service.create_payment(payment_request(), "outbox-lock")
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_publish(*args, **kwargs):
        """Удерживает публикацию до параллельного обращения второго издателя."""
        entered.set()
        await release.wait()
        return Basic.Ack()

    broker = AsyncMock()
    broker.publish.side_effect = slow_publish
    first = asyncio.create_task(publish_next_event(sessions, broker))
    await asyncio.wait_for(entered.wait(), timeout=5)
    try:
        assert not await publish_next_event(sessions, broker)
    finally:
        release.set()
        await first
    broker.publish.assert_awaited_once()
