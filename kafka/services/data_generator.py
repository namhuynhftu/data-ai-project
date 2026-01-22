from datetime import datetime
import random

from faker import Faker

from utils.models import InvoicesEvent


def generate_invoices(faker: Faker) -> InvoicesEvent:
    """Generate e-commerce invoice data based on Olist marketplace"""

    # E-commerce product categories from Olist dataset
    categories = [
        "health_beauty",
        "computers_accessories",
        "furniture_decor",
        "sports_leisure",
        "housewares",
        "bed_bath_table",
        "auto",
        "garden_tools",
        "toys",
        "watches_gifts",
        "telephony",
        "electronics",
        "fashion_bags_accessories",
        "cool_stuff",
        "books_general_interest",
    ]

    # Order statuses from Olist
    order_statuses = [
        "delivered",
        "shipped",
        "processing",
        "invoiced",
        "approved",
    ]

    # Payment types
    payment_types = [
        "credit_card",
        "boleto",
        "voucher",
        "debit_card",
    ]

    # Generate realistic amounts
    item_price = round(random.uniform(10.0, 500.0), 2)
    freight_value = round(random.uniform(5.0, 50.0), 2)
    total_amount = item_price + freight_value

    # Generate payment installments (common in Brazil)
    payment_installments = random.choice([1, 2, 3, 6, 10, 12])

    return InvoicesEvent(
        invoice_id=faker.uuid4(),
        order_id=faker.uuid4(),
        customer_id=faker.uuid4(),
        seller_id=faker.uuid4(),
        product_category=random.choice(categories),
        item_price=item_price,
        freight_value=freight_value,
        total_amount=total_amount,
        payment_type=random.choice(payment_types),
        payment_installments=payment_installments,
        order_status=random.choice(order_statuses),
        customer_city=faker.city(),
        customer_state=faker.state_abbr(),
        seller_city=faker.city(),
        seller_state=faker.state_abbr(),
        timestamp=datetime.now().isoformat(),
        source_system="kafka_producer",
        processing_time_ms=random.randint(50, 500),
    )