from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.openapi.utils import get_openapi
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config import get_settings
from src.database import engine, session_factory
from src.logging import configure_logging
from src.routers.payments import router
from src.security import ApiKeyMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Настраивает логи и освобождает ресурсы при завершении API."""
    configure_logging()
    yield
    await engine.dispose()


app = FastAPI(
    title="Asynchronous Payments",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)
app.add_middleware(ApiKeyMiddleware, api_key=get_settings().api_key.get_secret_value())
app.include_router(router)


def get_protected_openapi() -> dict:
    """Описывает обязательный заголовок ключа в контракте OpenAPI."""
    if app.openapi_schema is None:
        schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
        schema.setdefault("components", {})["securitySchemes"] = {
            "ApiKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
        }
        schema["security"] = [{"ApiKey": []}]
        app.openapi_schema = schema
    return app.openapi_schema


app.openapi = get_protected_openapi


@app.get("/health", tags=["Operations"])
async def health() -> dict[str, str]:
    """Проверяет доступность базы данных для приёма платежей."""
    try:
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
    except (SQLAlchemyError, OSError) as error:
        raise HTTPException(503, "Database unavailable") from error
    return {"status": "ok"}
