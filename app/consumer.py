import json
import time
import pika
from app.buffer import MoveBuffer
from app.parquet_writer import write_parquet
from app.config import *

class RabbitConsumer:
    def __init__(self, buffer: MoveBuffer):
        self.buffer = buffer
        self.connection = self._connect_with_retry()
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

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
                print("Connecting to RabbitMQ...")
                return pika.BlockingConnection(parameters)
            except pika.exceptions.AMQPConnectionError:
                print("RabbitMQ not ready, retrying in 5 seconds...")
                time.sleep(5)

    def start(self):
        def callback(ch, method, properties, body):
            event = json.loads(body)

            should_flush = self.buffer.add(event)

            if should_flush:
                events = self.buffer.flush()
                write_parquet(events, PARQUET_DIR)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=100)
        self.channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=callback
        )

        print("Started consuming move.logged events")
        self.channel.start_consuming()
