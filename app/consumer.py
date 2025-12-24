import json
import pika
from buffer import MoveBuffer
from parquet_writer import write_parquet
from config import *

class RabbitConsumer:
    def __init__(self, buffer: MoveBuffer):
        self.buffer = buffer
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

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
        self.channel.start_consuming()
