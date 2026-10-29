from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from src.models.payment import Payment
from src.repositories.base import BaseRepository
from src.schemas.payments import PaymentCreateDto


class PaymentsRepository(BaseRepository[Payment]):
    """Чтение, конкурентная вставка и блокировка платежей."""

    model = Payment

    async def create(self, payment: PaymentCreateDto) -> Payment | None:
        """Вставляет платёж или возвращает отсутствие при занятом ключе."""
        statement = (
            insert(Payment)
            .values(id=uuid4(), **payment.model_dump())
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return await self.session.scalar(statement)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment:
        """Читает победившую вставку после ожидания уникального ограничения."""
        statement = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self.session.scalars(statement)).one()

    async def lock_by_id(self, payment_id: UUID) -> Payment | None:
        """Блокирует платёж до завершения текущей транзакции обработки."""
        statement = select(Payment).where(Payment.id == payment_id).with_for_update()
        return await self.session.scalar(statement)
