import json
import time
import pika
import logging

from app.buffer import MoveBuffer
from app.parquet_writer import write_parquet
from app.config import *

logger = logging.getLogger("logger")

EXCHANGE_NAME = "ai_player.events"
ROUTING_KEY = "move.logged"

class RabbitConsumer:
    def __init__(self, buffer: MoveBuffer):
        self.buffer = buffer
        self.connection = self._connect_with_retry()
        self.channel = self.connection.channel()

        # 1️⃣ Declare exchange (must match producer)
        self.channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type="topic",
            durable=True
        )

        # 2️⃣ Declare queue
        self.channel.queue_declare(
            queue=RABBITMQ_QUEUE,
            durable=True
        )

        # 3️⃣ Bind queue to exchange with routing key
        self.channel.queue_bind(
            exchange=EXCHANGE_NAME,
            queue=RABBITMQ_QUEUE,
            routing_key=ROUTING_KEY
        )

        logger.info(
            f"Queue '{RABBITMQ_QUEUE}' bound to exchange "
            f"'{EXCHANGE_NAME}' with routing key '{ROUTING_KEY}'"
        )

    def _connect_with_retry(self):
        credentials = pika.PlainCredentials(
            RABBITMQ_USER,
            RABBITMQ_PASSWORD
        )

        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=60
        )

        while True:
            try:
                logger.info("Connecting to RabbitMQ...")
                return pika.BlockingConnection(parameters)
            except pika.exceptions.AMQPConnectionError:
                logger.warning("RabbitMQ not ready, retrying in 5 seconds...")
                time.sleep(5)

    def start(self):
        def callback(ch, method, properties, body):
            event = json.loads(body)

            logger.info(
                f"Received move.logged event "
                f"(gameId={event.get('gameId')}, "
                f"moveIndex={event.get('moveIndex')})"
            )

            should_flush = self.buffer.add(event)

            if should_flush:
                events = self.buffer.flush()
                write_parquet(events, PARQUET_DIR)
                logger.info(f"Flushed {len(events)} events to Parquet")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=100)
        self.channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=callback
        )

        logger.info("Started consuming move.logged events")
        self.channel.start_consuming()
