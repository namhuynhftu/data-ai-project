"""Kafka to MinIO CDC Sink - Production optimized."""
import json
import logging
from datetime import datetime
from io import BytesIO

from confluent_kafka import Consumer, KafkaError
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaToMinIOSink:
    """Stream CDC events from Kafka to MinIO with time partitioning."""

    def __init__(self, topics: list, bucket: str = "cdc-events"):
        self.topics = topics
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:29092",
            "group.id": "minio-sink-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.minio = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        self.bucket = bucket
        if not self.minio.bucket_exists(bucket):
            self.minio.make_bucket(bucket)

    def _write(self, topic: str, msg: dict):
        """Write event to MinIO with time-based partitioning."""
        dt = datetime.fromisoformat(
            msg.get("timestamp", datetime.utcnow().isoformat()).replace(
                "Z", "+00:00"
            )
        )
        path = f"{topic}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        filename = f"{path}/event_{dt.strftime('%H%M%S')}_{msg.get('op', 'c')}.json"
        data = json.dumps(msg, default=str).encode()
        self.minio.put_object(
            self.bucket, filename, BytesIO(data), len(data)
        )
        return filename

    def run(self):
        """Consume and store CDC events continuously."""
        logger.info(f"🚀 Kafka→MinIO sink: {self.topics}")
        self.consumer.subscribe(self.topics)
        count = 0

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if not msg or msg.error():
                    if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode())
                    enriched = {
                        "source_topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        **value,
                    }
                    filename = self._write(msg.topic(), enriched)
                    self.consumer.commit(msg)
                    count += 1
                    if count % 10 == 0:
                        logger.info(f"📊 Processed {count} events")
                except Exception as e:
                    logger.error(f"Failed: {e}")
        except KeyboardInterrupt:
            logger.info(f"⏹️  Stopped ({count} events)")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    sink = KafkaToMinIOSink(
        topics=[
            "postgres.streaming.users",
            "postgres.streaming.transactions",
            "postgres.streaming.detailed_transactions",
        ]
    )
    sink.run()
