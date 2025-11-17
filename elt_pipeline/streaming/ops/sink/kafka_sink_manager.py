"""Kafka to MinIO CDC Sink - Production optimized with Avro support."""
import json
import logging
from datetime import datetime
from io import BytesIO

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from minio import Minio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaToMinIOSink:
    """Stream CDC events from Kafka to MinIO with time partitioning and Avro support."""

    def __init__(self, topics: list, bucket: str = "cdc-events"):
        self.topics = topics
        
        # Setup Schema Registry client
        schema_registry_conf = {'url': 'http://localhost:8081'}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Setup Kafka consumer
        self.consumer = Consumer({
            "bootstrap.servers": "localhost:29092",
            "group.id": "minio-sink-group-avro",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        
        # Setup MinIO client
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
        """Write event to MinIO with unique ID to prevent duplicates."""
        dt = datetime.fromisoformat(
            msg.get("timestamp", datetime.utcnow().isoformat()).replace(
                "Z", "+00:00"
            )
        )
        path = f"{topic}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        
        # Extract unique ID from CDC event payload
        payload = msg.get("payload", {})
        after = payload.get("after", {}) if isinstance(payload, dict) else {}
        
        # Try to get primary key (transaction_id, user_id, or detail_id)
        unique_id = (
            after.get("transaction_id") or 
            after.get("user_id") or 
            after.get("detail_id") or
            f"{msg.get('offset', 0)}"  # Fallback to offset
        )
        
        op = payload.get("op", "c") if isinstance(payload, dict) else "c"
        filename = f"{path}/{unique_id}_{op}.json"
        
        # Check if already exists (idempotent write)
        try:
            self.minio.stat_object(self.bucket, filename)
            logger.debug(f"Object {filename} exists, skipping duplicate")
            return filename
        except:
            pass  # Object doesn't exist, proceed
        
        data = json.dumps(msg, default=str).encode()
        self.minio.put_object(self.bucket, filename, BytesIO(data), len(data))
        logger.debug(f"Wrote {filename}")
        return filename

    def run(self):
        """Consume and store CDC events continuously."""
        logger.info(f"Kafka→MinIO sink: {self.topics}")
        self.consumer.subscribe(self.topics)
        count = 0
        
        # Cache for deserializers per topic
        deserializers = {}

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if not msg or msg.error():
                    if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Error: {msg.error()}")
                    continue

                try:
                    topic = msg.topic()
                    
                    # Get or create deserializer for this topic
                    if topic not in deserializers:
                        # Get schema for the value from Schema Registry
                        subject = f"{topic}-value"
                        try:
                            schema_str = self.schema_registry_client.get_latest_version(subject).schema.schema_str
                            deserializers[topic] = AvroDeserializer(
                                self.schema_registry_client,
                                schema_str
                            )
                            logger.info(f"Loaded schema for {subject}")
                        except Exception as e:
                            logger.error(f"Failed to load schema for {subject}: {e}")
                            continue
                    
                    # Deserialize Avro message
                    ctx = SerializationContext(topic, MessageField.VALUE)
                    value = deserializers[topic](msg.value(), ctx)
                    
                    # Enrich with metadata
                    enriched = {
                        "source_topic": topic,
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "payload": value,  # Deserialized Avro data
                    }
                    
                    filename = self._write(topic, enriched)
                    self.consumer.commit(msg)
                    count += 1
                    if count % 10 == 0:
                        logger.info(f"Processed {count} events")
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
        except KeyboardInterrupt:
            logger.info(f"Stopped ({count} events)")
        finally:
            self.consumer.close()

