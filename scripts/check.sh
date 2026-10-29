#!/bin/sh
# Полная проверка в отдельном Compose-проекте без удаления томов.
set -eu

cd "$(dirname "$0")/.."
if [ ! -f .env ]; then
    cp .env.example .env
fi
export API_PORT=0
export RABBITMQ_MANAGEMENT_PORT=0
export VERIFY_RUN_ID="verify-$(date +%s)-$$"
project="payments-check-$$"

compose() {
    docker compose -p "$project" --profile demo --profile test --profile verify "$@"
}

cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [ "$result" -ne 0 ]; then
        # При ошибке выводим только состояние; тела запросов и секреты не печатаем.
        compose ps --all
    fi
    compose down
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

verify() {
    compose run --rm --no-deps -e "VERIFY_RUN_ID=$VERIFY_RUN_ID" verify "$@"
}

compose build api consumer outbox migrate broker-setup tests verify
compose up -d --wait api consumer outbox demo-webhook test-postgres

# Миграции проверяем только на отдельной тестовой БД до создания таблиц pytest.
compose run --rm --no-deps tests alembic upgrade head
compose run --rm --no-deps tests alembic check
compose run --rm --no-deps tests alembic downgrade base
compose run --rm --no-deps tests alembic upgrade head
compose run --rm --no-deps tests ruff check .
compose run --rm --no-deps tests ruff format --check --diff
compose run --rm --no-deps tests pytest -q --cov=src --cov-report=term-missing

verify python -m scripts.verify contracts
verify python -m scripts.verify processing

compose stop outbox
verify python -m scripts.verify outage prepare publisher-outage
verify python -m scripts.verify outage pending publisher-outage
compose start outbox
verify python -m scripts.verify outage check publisher-outage

compose stop rabbitmq
verify python -m scripts.verify outage prepare broker-outage
verify python -m scripts.verify outage pending broker-outage
compose start --wait rabbitmq
verify python -m scripts.verify outage check broker-outage

compose stop consumer
verify python -m scripts.verify outage prepare consumer-restart
compose start consumer
# Убиваем процесс с неподтверждённым сообщением во время 2–5 секунд обработки.
verify python -m scripts.verify outage inflight consumer-restart
compose kill -s SIGKILL consumer
compose up -d consumer
verify python -m scripts.verify outage check consumer-restart

echo "All checks passed. Temporary containers removed on exit; volumes preserved."
