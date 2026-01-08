from fastapi import FastAPI
import asyncio
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from .users_worker import main as users_worker

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # dev-friendly; tighten in prod
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}
