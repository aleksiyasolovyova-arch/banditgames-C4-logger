import os

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "move.logged")
RABBITMQ_GAME_QUEUE = os.getenv("RABBITMQ_GAME_QUEUE", "game.finished")

# Storage Configuration
PARQUET_DIR = os.getenv("PARQUET_DIR", "/data/parquet")
DATASET_DIR = os.getenv("DATASET_DIR", "/data/datasets")
FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", 100000))

# Dataset Configuration
DATASET_VERSION = os.getenv("DATASET_VERSION", "v1")
MIN_GAMES_FOR_EXPORT = int(os.getenv("MIN_GAMES_FOR_EXPORT", 100))

# Exchange Names
EXCHANGE_NAME = "ai_player.events"

# DVC Configuration
USE_DVC = os.getenv("USE_DVC", "true").lower() == "true"
DVC_REMOTE = os.getenv("DVC_REMOTE", "minio")

# MinIO Configuration (for DVC)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datasets")

# Orchestration (disabled - using volume mounts)
UPLOAD_TO_ORCHESTRATION = os.getenv("UPLOAD_TO_ORCHESTRATION", "false").lower() == "true"