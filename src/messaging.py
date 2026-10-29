from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import QueueType

from src.config import get_settings

exchange = RabbitExchange("payments", durable=True)
dead_exchange = RabbitExchange("payments.dead", durable=True)
work_queue = RabbitQueue(
    "payments.new",
    durable=True,
    queue_type=QueueType.QUORUM,
    arguments={
        "x-dead-letter-exchange": "payments.dead",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
dead_queue = RabbitQueue("payments.dlq", durable=True, queue_type=QueueType.QUORUM)


def create_broker() -> RabbitBroker:
    """Создаёт брокер с подтверждениями публикации и без логирования тел сообщений."""
    return RabbitBroker(
        get_settings().rabbitmq_url.get_secret_value(),
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        logger=None,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    """Объявляет обменники и обе очереди до первой публикации."""
    for queue, target in ((dead_queue, dead_exchange), (work_queue, exchange)):
        declared_exchange = await broker.declare_exchange(target)
        declared_queue = await broker.declare_queue(queue)
        await declared_queue.bind(declared_exchange, routing_key=queue.name)
