import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .bookings_worker import main as bookings
from .payments_worker import main as payments
from .users_worker import main as users

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def start_workers():
    # Launch each worker as a background task
    asyncio.create_task(bookings())
    asyncio.create_task(payments())
    asyncio.create_task(users())

@app.get("/health")
def health():
    return {"status": "ok"}