import json
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from faker import Faker

from services.data_generator import generate_invoices
from utils.config import get_kafka_config
from utils.logger import logger


def delivery_callback(err, msg):
    """Callback for Kafka message delivery confirmation"""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.debug(
            f"Message delivered to {msg.topic()}[{msg.partition()}] "
            f"@ offset {msg.offset()}"
        )


def create_kafka_topic(bootstrap_servers: str, topic_name: str) -> bool:
    """Create Kafka topic if it doesn't exist"""
    try:
        admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

        # Check if topic already exists
        metadata = admin_client.list_topics(timeout=10)
        if topic_name in metadata.topics:
            logger.info(f"Topic '{topic_name}' already exists")
            return True

        # Create new topic
        new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)

        fs = admin_client.create_topics([new_topic])

        # Wait for topic creation to complete
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"✅ Topic '{topic}' created successfully")
                return True
            except Exception as e:
                logger.error(f"❌ Failed to create topic '{topic}': {e}")
                return False

    except Exception as e:
        logger.error(f"❌ Error creating topic: {e}")
        return False


def main() -> None:
    load_dotenv()

    faker = Faker()

    # Kafka configuration
    kafka_config = get_kafka_config()
    bootstrap = kafka_config.bootstrap_servers
    topic = kafka_config.topic

    logger.info(f"Connecting to Kafka at: {bootstrap}")

    # Create topic if it doesn't exist
    logger.info("Creating Kafka topic...")
    if not create_kafka_topic(bootstrap, topic):
        logger.error("Failed to create topic. Exiting.")
        return

    # Configure producer
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "lab-producer",
            "acks": "all",
        }
    )

    try:
        message_count = 0
        logger.info("Starting to produce messages...")

        while True:
            # Generate and send invoice
            event = generate_invoices(faker)

            producer.produce(
                topic,
                key=event.invoice_id,
                value=json.dumps(event.to_dict()),
                callback=delivery_callback,
            )
            producer.poll(0)  # Trigger delivery reports

            message_count += 1
            logger.info(
                f"🧾 Produced message #{message_count}: invoice_id={event.invoice_id} | "
                f"total_amount={event.total_amount} | category={event.product_category}"
            )

            if message_count % 10 == 0:
                logger.info(f"Total messages produced: {message_count}")

            time.sleep(10.0)

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        producer.flush(10.0)
        logger.info(f"Producer shutdown complete. Total messages: {message_count}")


if __name__ == "__main__":
    main()