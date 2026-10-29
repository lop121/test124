from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки процессов из переменных окружения."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: SecretStr
    rabbitmq_url: SecretStr
    rabbitmq_management_url: str = "http://rabbitmq:15672"
    api_key: SecretStr = Field(min_length=16)
    webhook_secret: SecretStr = Field(min_length=16)
    webhook_allowed_hosts: list[str] = ["demo-webhook", "webhook.site"]
    webhook_allow_http: bool = False
    webhook_timeout: float = Field(default=5, gt=0, le=30)
    retry_base_seconds: float = Field(default=2, ge=0.1, le=60)
    outbox_poll_seconds: float = Field(default=0.5, ge=0.05, le=10)


@lru_cache
def get_settings() -> Settings:
    """Возвращает проверенные настройки текущего процесса."""
    return Settings()
