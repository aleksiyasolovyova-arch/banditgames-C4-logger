import json
import time
import pika
import logging
from typing import Dict

from app.buffer import MoveBuffer, GameOutcomeBuffer
from app.parquet_writer import write_parquet
from app.config import *

logger = logging.getLogger("logger")

MOVE_ROUTING_KEY = "move.logged"
GAME_ROUTING_KEY = "game.finished"

# Define both exchanges
AI_PLAYER_EXCHANGE = "ai_player.events"  # For move.logged from AI player
CONNECT4_EXCHANGE = "connect4.events"  # For game.finished from backend


class RabbitConsumer:
    """Consumer for RabbitMQ events including moves and game outcomes."""

    def __init__(self, move_buffer: MoveBuffer, outcome_buffer: GameOutcomeBuffer):
        self.move_buffer = move_buffer
        self.outcome_buffer = outcome_buffer
        self.connection = self._connect_with_retry()
        self.channel = self.connection.channel()

        # Declare BOTH exchanges
        self.channel.exchange_declare(
            exchange=AI_PLAYER_EXCHANGE,
            exchange_type="topic",
            durable=True
        )

        self.channel.exchange_declare(
            exchange=CONNECT4_EXCHANGE,
            exchange_type="topic",
            durable=True
        )

        # Setup move queue (from ai_player.events)
        self._setup_move_queue()

        # Setup game outcome queue (from connect4.events)
        self._setup_game_queue()

    def _setup_move_queue(self):
        """Setup queue for move.logged events from ai_player.events."""
        self.channel.queue_declare(
            queue=RABBITMQ_QUEUE,
            durable=True
        )

        self.channel.queue_bind(
            exchange=AI_PLAYER_EXCHANGE,  # Bind to ai_player.events
            queue=RABBITMQ_QUEUE,
            routing_key=MOVE_ROUTING_KEY
        )

        logger.info(
            f"Queue '{RABBITMQ_QUEUE}' bound to exchange "
            f"'{AI_PLAYER_EXCHANGE}' with routing key '{MOVE_ROUTING_KEY}'"
        )

    def _setup_game_queue(self):
        """Setup queue for game.finished events from connect4.events."""
        self.channel.queue_declare(
            queue=RABBITMQ_GAME_QUEUE,
            durable=True
        )

        self.channel.queue_bind(
            exchange=CONNECT4_EXCHANGE,  # Bind to connect4.events
            queue=RABBITMQ_GAME_QUEUE,
            routing_key=GAME_ROUTING_KEY
        )

        logger.info(
            f"Queue '{RABBITMQ_GAME_QUEUE}' bound to exchange "
            f"'{CONNECT4_EXCHANGE}' with routing key '{GAME_ROUTING_KEY}'"
        )

    def _connect_with_retry(self):
        """Connect to RabbitMQ with retry logic."""
        credentials = pika.PlainCredentials(
            RABBITMQ_USER,
            RABBITMQ_PASSWORD
        )

        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=300
        )

        while True:
            try:
                logger.info("Connecting to RabbitMQ...")
                connection = pika.BlockingConnection(parameters)
                logger.info("Successfully connected to RabbitMQ")
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                logger.warning(f"RabbitMQ not ready: {e}, retrying in 5 seconds...")
                time.sleep(5)

    def _handle_move_event(self, ch, method, properties, body):
        """Handle move.logged events."""
        try:
            event = json.loads(body)

            logger.info(
                f"Received move.logged event "
                f"(gameId={event.get('gameId')}, "
                f"moveIndex={event.get('moveIndex')})"
            )

            # Validate required fields
            if not self._validate_move_event(event):
                logger.warning(f"Invalid move event: {event.get('eventId')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            should_flush = self.move_buffer.add(event)

            if should_flush:
                events = self.move_buffer.flush()
                path = write_parquet(events, PARQUET_DIR)
                logger.info(f"Flushed {len(events)} events to {path}")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode move event: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing move event: {e}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _handle_game_outcome_event(self, ch, method, properties, body):
        """Handle game.finished events."""
        try:
            raw_outcome = json.loads(body)

            logger.info(
                f"Received game.finished event "
                f"(gameId={raw_outcome.get('gameId')}, "
                f"phase={raw_outcome.get('phase')})"
            )

            # Validate required fields
            if not self._validate_outcome_event(raw_outcome):
                logger.warning(f"Invalid outcome event: {raw_outcome.get('gameId')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            # Transform to standardized format
            outcome = self._transform_outcome_event(raw_outcome)

            self.outcome_buffer.add_game_outcome(outcome)

            stats = self.outcome_buffer.stats()
            logger.info(
                f"Game outcomes: {stats['total_games']} games, "
                f"{stats['wins']} wins, {stats['losses']} losses, "
                f"{stats['draws']} draws (win rate: {stats['win_rate']:.2%})"
            )

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode game outcome: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception as e:
            logger.error(f"Error processing game outcome: {e}", exc_info=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _validate_move_event(self, event: Dict) -> bool:
        """Validate that a move event has required fields."""
        required_fields = ['gameId', 'moveIndex', 'actionTaken']
        return all(field in event for field in required_fields)

    def _validate_outcome_event(self, outcome: Dict) -> bool:
        """Validate that a game outcome event has required fields."""
        required_fields = ['gameId', 'phase']
        return all(field in outcome for field in required_fields)

    def _transform_outcome_event(self, raw_outcome: Dict) -> Dict:
        """
        Transform game.finished event to standardized format.

        Converts your backend's format to the logging service format.
        """
        # Determine outcome based on phase and winner
        phase = raw_outcome.get('phase', '').upper()
        winner = raw_outcome.get('winner')

        # Map phase to outcome
        if phase == 'FINISHED' or phase == 'PLAYER_WON':
            if winner:
                outcome_str = 'WIN'
            else:
                outcome_str = 'DRAW'
        elif phase == 'DRAW':
            outcome_str = 'DRAW'
        else:
            outcome_str = phase  # Keep original phase if unknown

        # Extract winner information
        winner_name = None
        if winner and isinstance(winner, dict):
            winner_name = winner.get('name') or winner.get('id')

        # Convert durationSeconds to milliseconds
        duration_ms = raw_outcome.get('durationSeconds', 0) * 1000

        return {
            'gameId': raw_outcome.get('gameId'),
            'outcome': outcome_str,
            'winner': winner_name,
            'totalMoves': raw_outcome.get('totalMoves'),
            'gameDurationMs': duration_ms,
            'timestamp': raw_outcome.get('timestamp'),
            'phase': raw_outcome.get('phase'),
            'player1': 'player1',
            'player2': 'player2',
            'player1Type': 'ai',
            'player2Type': 'ai',
        }

    def start(self):
        """Start consuming events from both queues."""
        # Configure QoS
        self.channel.basic_qos(prefetch_count=100)

        # Consume move events
        self.channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=self._handle_move_event
        )

        # Consume game outcome events
        self.channel.basic_consume(
            queue=RABBITMQ_GAME_QUEUE,
            on_message_callback=self._handle_game_outcome_event
        )

        logger.info("Started consuming events from RabbitMQ")

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            self.channel.stop_consuming()
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            raise
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Connection closed")