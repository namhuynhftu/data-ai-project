#!/usr/bin/env python3
"""
Test script to verify Kafka and PostgreSQL setup
"""

import sys

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

from utils.config import get_kafka_config, get_postgres_config
from utils.logger import logger

try:
    import psycopg2 as psycopg
except ImportError:
    logger.error("❌ psycopg not installed. Run: uv add psycopg")
    sys.exit(1)


def test_kafka_connection():
    """Test Kafka connection and basic operations"""
    try:
        kafka_config = get_kafka_config()
        bootstrap = kafka_config.bootstrap_servers
        logger.info(f"Testing Kafka connection to {bootstrap}")

        # Test producer connection
        producer = Producer({"bootstrap.servers": bootstrap})
        producer.flush(5)
        logger.info("✅ Kafka producer connection successful")

        # Test consumer connection
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "test_group",
                "auto.offset.reset": "earliest",
            }
        )
        consumer.close()
        logger.info("✅ Kafka consumer connection successful")

        return True

    except Exception as e:
        logger.error(f"❌ Kafka test failed: {e}")
        return False


def test_postgres_connection():
    """Test PostgreSQL connection and basic operations"""
    try:
        pg_config = get_postgres_config()
        dsn = f"postgresql://{pg_config.user}:{pg_config.password}@{pg_config.host}:{pg_config.port}/{pg_config.database}"
        logger.info(
            f"Testing PostgreSQL connection to {pg_config.host}:{pg_config.port}"
        )

        with (
            psycopg.connect(dsn, autocommit=True) as conn,
            conn.cursor() as cur,
        ):
            # Test basic query
            cur.execute("SELECT version()")
            version = cur.fetchone()
            logger.info(f"✅ PostgreSQL connection successful: {version[0][:50]}...")

            # Test table creation
            cur.execute("""
                CREATE TABLE IF NOT EXISTS test_setup (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Test insert
            cur.execute(
                "INSERT INTO test_setup (message) VALUES (%s)",
                ("Setup test successful",),
            )

            # Test select
            cur.execute("SELECT COUNT(*) FROM test_setup")
            count = cur.fetchone()
            logger.info(f"✅ PostgreSQL operations successful: {count[0]} test records")

        return True

    except Exception as e:
        logger.error(f"❌ PostgreSQL test failed: {e}")
        return False


def main():
    """Run all tests"""
    load_dotenv()

    logger.info("🚀 Starting setup verification...")

    kafka_ok = test_kafka_connection()
    postgres_ok = test_postgres_connection()

    logger.info("\n" + "=" * 50)
    logger.info("📊 SETUP VERIFICATION RESULTS")
    logger.info("=" * 50)

    if kafka_ok:
        logger.info("✅ Kafka: Ready")
    else:
        logger.error("❌ Kafka: Failed")

    if postgres_ok:
        logger.info("✅ PostgreSQL: Ready")
    else:
        logger.error("❌ PostgreSQL: Failed")

    if kafka_ok and postgres_ok:
        logger.info("\n🎉 All systems ready! You can now run the lab.")
        logger.info("📖 Next steps:")
        logger.info("   1. Start consumer: python consumer.py")
        logger.info("   2. Start producer: python producer.py")
        logger.info("   3. Monitor with Kafdrop: http://localhost:9000")
    else:
        logger.error("\n⚠️  Some systems failed. Please check the setup.")
        sys.exit(1)


if __name__ == "__main__":
    main()