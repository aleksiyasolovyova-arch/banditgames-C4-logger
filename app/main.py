import threading
from fastapi import FastAPI
from buffer import MoveBuffer
from parquet_writer import write_parquet
from consumer import RabbitConsumer
from config import *

buffer = MoveBuffer(FLUSH_SIZE)
app = FastAPI()

@app.post("/flush")
def flush_now():
    events = buffer.flush()
    path = write_parquet(events, PARQUET_DIR)
    return {
        "flushed_events": len(events),
        "file": path
    }

def start_consumer():
    consumer = RabbitConsumer(buffer)
    consumer.start()

if __name__ == "__main__":
    threading.Thread(target=start_consumer, daemon=True).start()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
