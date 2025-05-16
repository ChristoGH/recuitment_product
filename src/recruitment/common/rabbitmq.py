import logging
import os
from collections.abc import AsyncGenerator
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection

RABBIT_QUEUE = "recruitment_urls"

logger = logging.getLogger(__name__)

_connection: Optional[AbstractConnection] = None


async def get_rabbitmq_connection() -> AbstractConnection:
    global _connection
    if _connection is None or _connection.is_closed:
        _connection = await aio_pika.connect_robust(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=int(os.getenv("RABBITMQ_PORT", 5672)),
            login=os.getenv("RABBITMQ_USER", "guest"),
            password=os.getenv("RABBITMQ_PASSWORD", "guest"),
            heartbeat=60,
        )
        logger.info(
            f"Successfully connected to RabbitMQ at {os.getenv('RABBITMQ_HOST', 'rabbitmq')}"
        )
    return _connection


async def get_channel() -> AbstractChannel:
    """Get a channel from the RabbitMQ connection."""
    connection = await get_rabbitmq_connection()
    if connection is None:
        raise RuntimeError("Failed to establish RabbitMQ connection")
    channel = await connection.channel()
    await channel.declare_queue(RABBIT_QUEUE, durable=True)
    return channel


class RabbitMQConnection:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        user: str = "guest",
        password: str = "guest",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection: Optional[AbstractConnection] = None
        self.channel: Optional[AbstractChannel] = None
        self.queue: Optional[aio_pika.Queue] = None

    async def connect(self) -> None:
        """Connect to RabbitMQ and set up channel and queue."""
        try:
            self.connection = await aio_pika.connect_robust(
                host=self.host, port=self.port, login=self.user, password=self.password
            )
            if self.connection is None:
                raise RuntimeError("Failed to establish RabbitMQ connection")
            self.channel = await self.connection.channel()
            if self.channel is None:
                raise RuntimeError("Failed to create RabbitMQ channel")
            self.queue = await self.channel.declare_queue(RABBIT_QUEUE, durable=True)
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True)
            raise

    async def close(self) -> None:
        """Close the RabbitMQ connection."""
        if self.connection:
            await self.connection.close()
            logger.info("Closed RabbitMQ connection")

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ."""
        return bool(self.connection and not self.connection.is_closed)

    async def publish_url(self, url: str) -> None:
        """Publish a URL to the queue."""
        if not self.is_connected():
            raise RuntimeError("Not connected to RabbitMQ")
        if self.channel is None:
            raise RuntimeError("Channel is not initialized")
        message = aio_pika.Message(url.encode())
        await self.channel.default_exchange.publish(message, routing_key=RABBIT_QUEUE)

    async def consume_urls(self) -> AsyncGenerator[str, None]:
        """Consume URLs from the queue."""
        if not self.is_connected():
            raise RuntimeError("Not connected to RabbitMQ")
        if self.queue is None:
            raise RuntimeError("Queue is not initialized")
        async with self.queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    url = message.body.decode()
                    yield url
