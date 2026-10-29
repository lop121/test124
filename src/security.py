import hmac

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class ApiKeyMiddleware:
    """Проверяет статический ключ для каждого HTTP-адреса, включая OpenAPI."""

    def __init__(self, app: ASGIApp, api_key: str) -> None:
        """Сохраняет ключ без его включения в логи или ответы."""
        self.app = app
        self.api_key = api_key.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Отклоняет отсутствующий или неверный ключ до чтения тела запроса."""
        if scope["type"] == "http":
            provided = dict(scope["headers"]).get(b"x-api-key", b"")
            if not hmac.compare_digest(provided, self.api_key):
                response = JSONResponse(
                    {"detail": "Invalid API key"},
                    status_code=401,
                    headers={"WWW-Authenticate": "ApiKey"},
                )
                await response(scope, receive, send)
                return
        await self.app(scope, receive, send)
