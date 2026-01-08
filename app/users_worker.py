import aio_pika
import asyncio
import json
import os

RABBIT_URL = os.getenv("RABBIT_URL")
EXCHANGE_NAME = "users_events_topic"
QUEUE_NAME = "users_notifications"

async def send_email(email: str, topic: str, body: str):
    print("\n-----------Email-----------")
    print(f"To: {email}")
    print(f"Subject: {topic}")
    print(body)
    print("-------------------------------\n", flush=True)

def create_email(event_type: str, event: dict):
    email = (
        event.get("email")
        or "unknown@example.com"
    )

    topic = f"Notification: {event_type}"

    real_event = json.dumps(event, indent=2)

    body= (
        f"We have received a new event from our system. \n\n"
        f"Event Type: {event_type}\n"
        f"Payload:\n{real_event}\n"
    )

    return email, topic, body


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
            await queue.bind(exchange, routing_key="user.*")
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

                print(f"[WORKER]{event_type}: {event}", flush=True)

                email, topic, body = create_email(event_type, event)

                await send_email(email, topic, body)