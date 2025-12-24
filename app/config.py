import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "move.logged")

PARQUET_DIR = os.getenv("PARQUET_DIR", "/data/parquet")
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", 1000))
