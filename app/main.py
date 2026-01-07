from fastapi import FastAPI
import asyncio
from .users_worker import main as users_worker

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(users_worker())

@app.get("/health")
def health():
    return {"status": "ok"}
