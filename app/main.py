import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.buffer import MoveBuffer
from app.parquet_writer import write_parquet
from app.consumer import RabbitConsumer
from app.config import *
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("logger")
buffer = MoveBuffer(FLUSH_SIZE)

def start_consumer():
    logger.info("start_consumer() called")
    consumer = RabbitConsumer(buffer)
    consumer.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("LIFESPAN STARTUP BEGIN")

    thread = threading.Thread(
        target=start_consumer,
        daemon=True
    )
    thread.start()

    logger.info("LIFESPAN STARTUP END")

    yield

    logger.info("LIFESPAN SHUTDOWN")
app = FastAPI(lifespan=lifespan)

@app.post("/flush")
def flush_now():
    events = buffer.flush()
    path = write_parquet(events, PARQUET_DIR)
    return {
        "flushed_events": len(events),
        "file": path
    }
