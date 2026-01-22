import psycopg2 as psycopg

from utils.config import get_postgres_config
from utils.logger import logger
from utils.models import InvoicesData


def get_postgres_dsn() -> str:
    """Build PostgreSQL connection string from environment variables."""
    config = get_postgres_config()
    dsn = f"postgresql://{config.user}:{config.password}@{config.host}:{config.port}/{config.database}"
    logger.info(f"PostgreSQL connection configured for {config.host}:{config.port}")
    return dsn


def create_invoices_table(dsn: str) -> bool:
    """Create the kafka_streaming schema and invoices table if they don't exist."""
    try:
        with psycopg.connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Create schema
                cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS kafka_streaming
                """)
                logger.info("Schema kafka_streaming created/verified successfully")
                
                # Create invoices table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS kafka_streaming.invoices (
                        invoice_id VARCHAR(50) PRIMARY KEY,
                        order_id VARCHAR(50) NOT NULL,
                        customer_id VARCHAR(50) NOT NULL,
                        seller_id VARCHAR(50) NOT NULL,
                        product_category VARCHAR(100),
                        item_price DECIMAL(10,2) NOT NULL,
                        freight_value DECIMAL(10,2) NOT NULL,
                        total_amount DECIMAL(10,2) NOT NULL,
                        payment_type VARCHAR(50),
                        payment_installments INTEGER,
                        order_status VARCHAR(50),
                        customer_city VARCHAR(100),
                        customer_state VARCHAR(50),
                        seller_city VARCHAR(100),
                        seller_state VARCHAR(50),
                        timestamp TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes for common query patterns
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoices_order_id 
                    ON kafka_streaming.invoices(order_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoices_customer_id 
                    ON kafka_streaming.invoices(customer_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoices_timestamp 
                    ON kafka_streaming.invoices(timestamp)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoices_order_status 
                    ON kafka_streaming.invoices(order_status)
                """)
            
        logger.info("Invoices table created/verified successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create invoices table: {e}")
        return False


def insert_invoice(dsn: str, invoice_data: InvoicesData) -> bool:
    """Insert an invoice into the database."""
    try:
        with psycopg.connect(dsn) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                """
                INSERT INTO kafka_streaming.invoices
                    (invoice_id, order_id, customer_id, seller_id, product_category,
                     item_price, freight_value, total_amount, payment_type,
                     payment_installments, order_status, customer_city, customer_state,
                     seller_city, seller_state, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (invoice_id) DO NOTHING
            """,
                (
                    invoice_data.invoice_id,
                    invoice_data.order_id,
                    invoice_data.customer_id,
                    invoice_data.seller_id,
                    invoice_data.product_category,
                    invoice_data.item_price,
                    invoice_data.freight_value,
                    invoice_data.total_amount,
                    invoice_data.payment_type,
                    invoice_data.payment_installments,
                    invoice_data.order_status,
                    invoice_data.customer_city,
                    invoice_data.customer_state,
                    invoice_data.seller_city,
                    invoice_data.seller_state,
                    invoice_data.timestamp,
                ),
            )
            return True
    except Exception as e:
        logger.error(f"Failed to insert invoice: {e}", exc_info=True)
        return False    