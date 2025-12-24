import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "move.logged")

PARQUET_DIR = os.getenv("PARQUET_DIR", "/data/parquet")
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", 1000))
