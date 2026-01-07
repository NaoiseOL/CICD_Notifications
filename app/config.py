import os

RABBIT_URL = os.getenv("RABBIT_URL")
QUEUE_NAME = "users_notifications"

FORWARD_ENDPOINTS = [ "http://localhost:8000/internal/notify", ]