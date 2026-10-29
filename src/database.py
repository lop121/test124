from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from src.config import get_settings


class Base(DeclarativeBase):
    """Базовый класс ORM-моделей сервиса."""


engine = create_async_engine(get_settings().database_url.get_secret_value(), pool_pre_ping=True)
session_factory = async_sessionmaker(engine, expire_on_commit=False)
