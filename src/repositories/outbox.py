from sqlalchemy import func, select

from src.models.outbox import Outbox
from src.repositories.base import BaseRepository
from src.schemas.payments import OutboxCreateDto


class OutboxRepository(BaseRepository[Outbox]):
    """Сохранение событий и конкурентный выбор готовых публикаций."""

    model = Outbox

    def create(self, event: OutboxCreateDto) -> Outbox:
        """Добавляет событие в ту же транзакцию, что и бизнес-изменение."""
        row = Outbox(**event.model_dump())
        self.session.add(row)
        return row

    async def lock_next_ready(self) -> Outbox | None:
        """Выбирает одно событие без ожидания блокировок других издателей."""
        statement = (
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
            .order_by(Outbox.available_at, Outbox.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        return await self.session.scalar(statement)
