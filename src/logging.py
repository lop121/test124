import sys

from loguru import logger


def configure_logging() -> None:
    """Включает структурированные логи без локальных переменных и секретов."""
    logger.remove()
    logger.add(sys.stdout, serialize=True, backtrace=False, diagnose=False)
