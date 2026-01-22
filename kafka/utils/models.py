"""
Type definitions for the Kafka streaming lab.

This module contains Pydantic model definitions for structured data used across
the application, providing better type safety, validation, and IDE support.
"""

from pydantic import BaseModel, ConfigDict


class PostgresConfig(BaseModel):
    """PostgreSQL connection configuration."""

    model_config = ConfigDict(frozen=True, protected_namespaces=())

    user: str
    password: str
    host: str
    port: str
    database: str
    schema: str


class KafkaConfig(BaseModel):
    """Kafka connection configuration."""

    model_config = ConfigDict(frozen=True)

    bootstrap_servers: str
    topic: str


class InvoicesData(BaseModel):
    """
    Invoice data structure for database operations.

    Fields match the invoices table schema in kafka_streaming.
    """

    model_config = ConfigDict(strict=True)

    invoice_id: str
    order_id: str
    customer_id: str
    seller_id: str
    product_category: str
    item_price: float
    freight_value: float
    total_amount: float
    payment_type: str
    payment_installments: int
    order_status: str
    customer_city: str
    customer_state: str
    seller_city: str
    seller_state: str
    timestamp: str

    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return self.model_dump()


class InvoicesEvent(BaseModel):
    """
    Extended invoice event structure for Kafka messages.

    Includes additional metadata fields that are part of the event
    but may not be stored in the database.
    """

    model_config = ConfigDict(strict=True)

    invoice_id: str
    order_id: str
    customer_id: str
    seller_id: str
    product_category: str
    item_price: float
    freight_value: float
    total_amount: float
    payment_type: str
    payment_installments: int
    order_status: str
    customer_city: str
    customer_state: str
    seller_city: str
    seller_state: str
    timestamp: str
    source_system: str
    processing_time_ms: int

    def to_dict(self) -> dict:
        """Convert model to dictionary."""
        return self.model_dump()

    def to_invoices_data(self) -> InvoicesData:
        """Convert event to invoice data for database operations."""
        return InvoicesData(
            invoice_id=self.invoice_id,
            order_id=self.order_id,
            customer_id=self.customer_id,
            seller_id=self.seller_id,
            product_category=self.product_category,
            item_price=self.item_price,
            freight_value=self.freight_value,
            total_amount=self.total_amount,
            payment_type=self.payment_type,
            payment_installments=self.payment_installments,
            order_status=self.order_status,
            customer_city=self.customer_city,
            customer_state=self.customer_state,
            seller_city=self.seller_city,
            seller_state=self.seller_state,
            timestamp=self.timestamp,
        )