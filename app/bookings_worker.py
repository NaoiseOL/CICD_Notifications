import aio_pika
import asyncio
import json
import os
from .config import RABBIT_URL, QUEUE_NAME

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "bookings_events_topic"
QUEUE_NAME = "bookings_notifications"

async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()

    # Declare the topic exchange
    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC
    )

    # Declare the queue
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    # BIND THE QUEUE TO THE EXCHANGE (this was missing)
    await queue.bind(exchange, routing_key="booking.*")

    print(f"[Bookings_WORKER] Listening on queue '{QUEUE_NAME}'...")

    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                event = json.loads(message.body)
                print("[Bookings_WORKER] Received event:", event)