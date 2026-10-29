from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from src.database import Base


class BaseRepository[T: Base]:
    """Общие операции репозиториев без управления транзакциями."""

    model: type[T]

    def __init__(self, session: AsyncSession) -> None:
        """Привязывает репозиторий к сессии текущей операции."""
        self.session = session

    async def get_by_id(self, entity_id: UUID) -> T | None:
        """Находит сущность по первичному ключу."""
        return await self.session.get(self.model, entity_id)
