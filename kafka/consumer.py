from datetime import UTC, datetime
import json

from confluent_kafka import Consumer
from dotenv import load_dotenv

from services.database import (
    create_invoices_table,
    get_postgres_dsn,
    insert_invoice,
)
from utils.config import get_kafka_config
from utils.logger import logger
from utils.models import InvoicesData, InvoicesEvent


def main() -> None:
    load_dotenv()

    # Initialize PostgreSQL connection
    dsn = get_postgres_dsn()
    if not create_invoices_table(dsn):
        logger.error("Failed to create invoices table. Exiting.")
        return

    # Configure Kafka consumer with production-ready settings
    kafka_config = get_kafka_config()
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "group.id": "invoice-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    logger.info(f"Connecting to Kafka at: {kafka_config.bootstrap_servers}")
    logger.info(
        f"Consumer config: group_id={consumer_config['group.id']}, "
        f"auto_commit={consumer_config['enable.auto.commit']}"
    )

    consumer = Consumer(consumer_config)
    consumer.subscribe([kafka_config.topic])
    logger.info(f"Subscribed to topic: {kafka_config.topic}")

    rows_written = 0
    messages_processed = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                event_dict = json.loads(msg.value().decode("utf-8"))
                messages_processed += 1

                # Ensure timestamp is valid
                ts = event_dict.get("timestamp")
                if isinstance(ts, str):
                    try:
                        datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    except Exception:
                        ts = datetime.now(UTC).isoformat()

                # Parse event with Pydantic model for attribute access
                event = InvoicesEvent(**event_dict)

                # Convert to invoice data for database insertion
                invoice_data = event.to_invoices_data()

                if insert_invoice(dsn, invoice_data):
                    rows_written += 1
                    logger.info(
                        f"✅ Inserted invoice_id={invoice_data.invoice_id} | "
                        f"order_id={invoice_data.order_id} | "
                        f"amount={invoice_data.total_amount} "
                        f"(row #{rows_written})"
                    )

                if messages_processed % 10 == 0:
                    logger.info(
                        f"Progress: {messages_processed} messages processed, "
                        f"{rows_written} rows written"
                    )

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info(
            f"Shutdown. Final stats: {messages_processed} messages, "
            f"{rows_written} rows written"
        )
    finally:
        consumer.close()


if __name__ == "__main__":
    main()