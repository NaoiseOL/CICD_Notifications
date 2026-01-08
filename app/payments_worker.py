import aio_pika
import asyncio
import json
import os
from .config import RABBIT_URL, QUEUE_NAME

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "payments_events_topic"
QUEUE_NAME = "payments_notifications"


def build_notification(event_type: str, event: dict):
    pretty_event = json.dumps(event, indent=2)

    title = f"New Payment: {event_type}"
    body = (
        f"A new Payment-related event has been received.\n\n"
        f"Event Type: {event_type}\n"
        f"Details:\n{pretty_event}\n"
    )

    return title, body


async def send_notification(title: str, body: str):
    print("\n----------- NOTIFICATION -----------")
    print(f"{title}")
    print(body)
    print("------------------------------------\n", flush=True)


async def main():
    connection = None

    while connection is None:
        try:
            print("[WORKER] Connecting to RabbitMQ...", flush=True)
            connection = await aio_pika.connect_robust(RABBIT_URL)
        except Exception:
            await asyncio.sleep(2)

    channel = await connection.channel()
    exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC)
    queue = await channel.declare_queue(QUEUE_NAME, durable=True)

    bound = False
    while not bound:
        try:
            await queue.bind(exchange, routing_key="payment.*")
            bound = True
            print("[WORKER] Queue bound to exchange", flush=True)
        except Exception:
            await asyncio.sleep(1)

    print(f"[WORKER] Listening on queue '{QUEUE_NAME}'...", flush=True)

    async with queue.iterator() as q:
        async for message in q:
            async with message.process():
                event = json.loads(message.body)
                event_type = message.routing_key

                print(f"[WORKER] {event_type}: {event}", flush=True)

                title, body = build_notification(event_type, event)
                await send_notification(title, body)