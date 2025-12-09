"""
Kafka module using confluent-kafka library.
Implements consumer for audit event processing with support for the new topics registry.
"""

import json
import threading
from datetime import date, datetime
from decimal import Decimal
from typing import Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from app.core.config import settings
from app.core.logging import get_logger
from app.core.topics import KafkaTopics

logger = get_logger(__name__)


def json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def delivery_report(err, msg):
    """Callback for message delivery confirmation."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(
            f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


class KafkaProducer:
    """Kafka Producer wrapper using confluent-kafka."""

    _instance: Producer | None = None

    @classmethod
    def get_producer(cls) -> Producer | None:
        """Get or create producer instance."""
        if not settings.KAFKA_ENABLED:
            return None

        if cls._instance is None:
            cls._instance = Producer(
                {
                    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                    "acks": "all",
                    "retries": 3,
                    "retry.backoff.ms": 100,
                    "enable.idempotence": True,
                    "client.id": "audit-service-producer",
                    "request.timeout.ms": 30000,
                }
            )
            logger.info(
                f"Kafka producer initialized: {settings.KAFKA_BOOTSTRAP_SERVERS}"
            )

        return cls._instance

    @classmethod
    def produce(cls, topic: str, value: dict, key: str | None = None) -> bool:
        """Produce a message to Kafka."""
        if not settings.KAFKA_ENABLED:
            logger.debug(f"Kafka disabled, skipping message to {topic}")
            return False

        producer = cls.get_producer()
        if producer is None:
            return False

        try:
            value_bytes = json.dumps(value, default=json_serializer).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None

            producer.produce(
                topic=topic,
                value=value_bytes,
                key=key_bytes,
                callback=delivery_report,
            )
            producer.poll(0)
            return True
        except Exception as e:
            logger.error(f"Failed to produce message: {e}")
            return False

    @classmethod
    def flush(cls, timeout: float = 10.0):
        """Flush all pending messages."""
        if cls._instance:
            remaining = cls._instance.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(f"Failed to deliver {remaining} message(s)")

    @classmethod
    def close(cls):
        """Close the producer."""
        if cls._instance:
            cls._instance.flush(timeout=10)
            cls._instance = None
            logger.info("Kafka producer closed")


class KafkaConsumer:
    """Kafka Consumer wrapper using confluent-kafka with background thread."""

    _consumer: Consumer | None = None
    _running: bool = False
    _thread: threading.Thread | None = None
    _message_handler: Callable[[dict, str], None] | None = None

    @classmethod
    def _create_consumer(cls) -> Consumer:
        """Create a new consumer instance."""
        return Consumer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": settings.KAFKA_GROUP_ID,
                "auto.offset.reset": settings.KAFKA_AUTO_OFFSET_RESET,
                "enable.auto.commit": settings.KAFKA_ENABLE_AUTO_COMMIT,
                "auto.commit.interval.ms": settings.KAFKA_AUTO_COMMIT_INTERVAL_MS,
                "session.timeout.ms": settings.KAFKA_SESSION_TIMEOUT_MS,
                "max.poll.interval.ms": settings.KAFKA_MAX_POLL_INTERVAL_MS,
            }
        )

    @classmethod
    def _consume_loop(cls):
        """Background thread consumer loop."""
        logger.info("Kafka consumer loop started")

        while cls._running:
            try:
                msg = cls._consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - not an error
                        logger.debug(
                            f"Reached end of partition {msg.topic()}[{msg.partition()}]"
                        )
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        logger.warning(f"Unknown topic or partition: {msg.error()}")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                # Get topic name
                topic = msg.topic()

                # Deserialize message
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message from {topic}: {e}")
                    continue

                # Log message metadata
                logger.info(
                    f"Received message from {topic}[{msg.partition()}]@{msg.offset()}"
                )

                # Handle message - pass topic to handler
                if cls._message_handler:
                    try:
                        cls._message_handler(value, topic)
                    except Exception as e:
                        logger.error(f"Error handling message from {topic}: {e}")

            except KafkaException as e:
                logger.error(f"Kafka exception: {e}")
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {e}")

        logger.info("Kafka consumer loop stopped")

    @classmethod
    def get_topics_to_subscribe(cls) -> list[str]:
        """
        Get the list of topics to subscribe to.
        Uses the topics registry if available, otherwise falls back to config.
        """
        try:
            # Use the topics registry
            return KafkaTopics.all_subscribed_topics()
        except Exception:
            # Fallback to config
            return settings.kafka_topics_list

    @classmethod
    async def start(cls):
        """Start the Kafka consumer in a background thread."""
        if not settings.KAFKA_ENABLED:
            logger.info("Kafka is disabled, skipping consumer initialization")
            return

        if cls._running:
            logger.warning("Kafka consumer already running")
            return

        # Set the message handler
        from app.core.audit_service import handle_kafka_event

        cls._message_handler = handle_kafka_event

        # Create consumer and subscribe to topics
        cls._consumer = cls._create_consumer()

        # Get topics from registry
        topics = cls.get_topics_to_subscribe()

        if not topics:
            logger.warning("No topics to subscribe to")
            return

        cls._consumer.subscribe(topics)
        logger.info(f"Subscribed to {len(topics)} topics: {topics[:10]}...")

        # Start background thread
        cls._running = True
        cls._thread = threading.Thread(target=cls._consume_loop, daemon=True)
        cls._thread.start()

        logger.info(
            f"Kafka consumer started: {settings.KAFKA_BOOTSTRAP_SERVERS}, "
            f"group: {settings.KAFKA_GROUP_ID}"
        )

    @classmethod
    async def stop(cls):
        """Stop the Kafka consumer."""
        if not cls._running:
            return

        cls._running = False

        # Wait for thread to finish
        if cls._thread and cls._thread.is_alive():
            cls._thread.join(timeout=5.0)

        # Close consumer
        if cls._consumer:
            cls._consumer.close()
            cls._consumer = None

        logger.info("Kafka consumer stopped")

    @classmethod
    def is_running(cls) -> bool:
        """Check if the consumer is running."""
        return cls._running


class KafkaAdmin:
    """Kafka Admin operations using confluent-kafka."""

    _client: AdminClient | None = None

    @classmethod
    def get_client(cls) -> AdminClient | None:
        """Get or create admin client instance."""
        if not settings.KAFKA_ENABLED:
            return None

        if cls._client is None:
            cls._client = AdminClient(
                {"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS}
            )

        return cls._client

    @classmethod
    def create_topic(
        cls,
        topic_name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ) -> bool:
        """Create a topic if it doesn't exist."""
        client = cls.get_client()
        if client is None:
            return False

        try:
            metadata = client.list_topics(timeout=10)

            if topic_name in metadata.topics:
                logger.info(f"Topic '{topic_name}' already exists")
                return True

            new_topic = NewTopic(
                topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )

            futures = client.create_topics([new_topic])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"Topic '{topic}' created")
                except Exception as e:
                    logger.error(f"Failed to create topic '{topic}': {e}")
                    return False

            return True
        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            return False

    @classmethod
    def create_audit_topics(cls) -> bool:
        """Create all audit-related topics."""
        topics = KafkaTopics.all_subscribed_topics()
        success = True
        for topic in topics:
            if not cls.create_topic(topic):
                success = False
        return success

    @classmethod
    def list_topics(cls) -> list[str]:
        """List all topics."""
        client = cls.get_client()
        if client is None:
            return []

        try:
            metadata = client.list_topics(timeout=10)
            return list(metadata.topics.keys())
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return []

    @classmethod
    def topic_exists(cls, topic_name: str) -> bool:
        """Check if a topic exists."""
        return topic_name in cls.list_topics()


def health_check() -> dict:
    """Check Kafka connectivity status."""
    if not settings.KAFKA_ENABLED:
        return {"kafka_enabled": False, "status": "disabled"}

    try:
        client = KafkaAdmin.get_client()
        if client:
            metadata = client.list_topics(timeout=5)
            return {
                "kafka_enabled": True,
                "status": "healthy",
                "broker_count": len(metadata.brokers),
                "topic_count": len(metadata.topics),
                "consumer_running": KafkaConsumer.is_running(),
            }
    except Exception as e:
        return {
            "kafka_enabled": True,
            "status": "unhealthy",
            "error": str(e),
            "consumer_running": KafkaConsumer.is_running(),
        }

    return {"kafka_enabled": True, "status": "unknown"}
