import json
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict

from faker import Faker
from elt_pipeline.logger_utils import get_data_ingestion_logger, StreamingOperation

# Setup centralized logging
logger = get_data_ingestion_logger()

class FakeDataGenerator:
    def __init__(self, create_directories=False):
        self.faker = Faker()
        # Only set data_dir path without creating it
        self.data_dir = Path("./elt_pipeline/streaming/data/external")
        
        # Only create directories if explicitly requested (default: False)
        if create_directories:
            self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Predefined lists for realistic data
        self.transaction_types = ['purchase', 'refund', 'transfer', 'withdrawal', 'deposit', 'payment']
        self.payment_methods = ['credit_card', 'debit_card', 'bank_transfer', 'paypal', 'apple_pay', 'google_pay', 'cash']
        self.merchant_categories = ['grocery', 'restaurant', 'gas_station', 'retail', 'online', 'entertainment', 'travel', 'healthcare', 'education', 'utilities']
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'BRL', 'MXN']
        self.status_options = ['completed', 'pending', 'failed', 'cancelled', 'processing']
    
    def generate_user_data(self, num_records: int = 1000) -> List[Dict]:
        """Generate fake user data"""

        user_data = []
        for _ in range(num_records):
            user = {
                "id": self.faker.uuid4(),
                "name": self.faker.name(),
                "email": self.faker.email(),
                "address": self.faker.address(),
                "created_at": datetime.now().isoformat()
            }
            user_data.append(user)
        return user_data

    def generate_transaction_data(self, num_records: int = 1000, user_ids: List[str] = None) -> List[Dict]:
        """Generate fake transaction data with optional user_ids for foreign key relationships"""

        transaction_data = []
        
        # If user_ids not provided, generate some fake ones
        if not user_ids:
            user_ids = [self.faker.uuid4() for _ in range(min(100, num_records))]
        
        for _ in range(num_records):
            transaction = {
                "transaction_id": self.faker.uuid4(),
                "user_id": random.choice(user_ids),  # Use provided user_ids for foreign key relationship
                "amount": round(self.faker.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
                "currency": self.faker.currency_code(),
                "timestamp": datetime.now().isoformat()
            }
            transaction_data.append(transaction)
        return transaction_data
    
    def generate_detailed_transaction_data(self, num_records: int = 100, user_ids: List[str] = None) -> List[Dict]:
        """Generate detailed fake transaction data with comprehensive fields"""
        
        transaction_data = []
        
        # If user_ids not provided, generate some fake ones
        if not user_ids:
            user_ids = [self.faker.uuid4() for _ in range(min(20, num_records))]
        
        for _ in range(num_records):
            # Generate realistic timestamp within last 6 months
            days_ago = random.randint(0, 180)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            transaction_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            
            # Choose transaction type and adjust amount accordingly
            transaction_type = random.choice(self.transaction_types)
            
            # Adjust amount based on transaction type
            if transaction_type == 'refund':
                amount = -round(random.uniform(10.0, 500.0), 2)
            elif transaction_type == 'withdrawal':
                amount = -round(random.uniform(20.0, 1000.0), 2)
            elif transaction_type == 'deposit':
                amount = round(random.uniform(100.0, 5000.0), 2)
            elif transaction_type == 'transfer':
                amount = -round(random.uniform(50.0, 2000.0), 2)
            else:  # purchase, payment
                amount = -round(random.uniform(5.0, 800.0), 2)
            
            # Generate currency (bias towards USD)
            currency = random.choices(
                self.currencies, 
                weights=[40, 15, 10, 8, 5, 5, 3, 4, 3, 7], 
                k=1
            )[0]
            
            # Status distribution (most completed)
            status = random.choices(
                self.status_options,
                weights=[85, 8, 4, 2, 1],
                k=1
            )[0]
            
            transaction = {
                "transaction_id": self.faker.uuid4(),
                "user_id": random.choice(user_ids),
                "amount": amount,
                "currency": currency,
                "transaction_type": transaction_type,
                "payment_method": random.choice(self.payment_methods),
                "merchant_name": self.faker.company(),
                "merchant_category": random.choice(self.merchant_categories),
                "description": self.faker.catch_phrase(),
                "status": status,
                "timestamp": transaction_time.isoformat(),
                "location": {
                    "city": self.faker.city(),
                    "state": self.faker.state(),
                    "country": self.faker.country(),
                    "latitude": float(self.faker.latitude()),
                    "longitude": float(self.faker.longitude())
                },
                "metadata": {
                    "ip_address": self.faker.ipv4(),
                    "user_agent": self.faker.user_agent(),
                    "device_type": random.choice(['mobile', 'desktop', 'tablet']),
                    "channel": random.choice(['online', 'mobile_app', 'in_store', 'atm', 'phone']),
                    "reference_number": self.faker.bothify(text='REF-########'),
                    "fee_amount": round(random.uniform(0.0, 5.0), 2) if random.random() > 0.7 else 0.0
                }
            }
            
            # Add card details for card payments
            if transaction['payment_method'] in ['credit_card', 'debit_card']:
                transaction['card_details'] = {
                    "last_four_digits": self.faker.credit_card_number()[-4:],
                    "card_type": random.choice(['Visa', 'MasterCard', 'American Express', 'Discover']),
                    "issuing_bank": self.faker.company()
                }
            
            # Add merchant details for in-store purchases
            if transaction['metadata']['channel'] == 'in_store':
                transaction['store_details'] = {
                    "store_id": self.faker.bothify(text='STORE-####'),
                    "terminal_id": self.faker.bothify(text='TERM-###'),
                    "cashier_id": self.faker.bothify(text='CASH-###')
                }
            
            transaction_data.append(transaction)
        
        return transaction_data
    
    def get_user_schema(self) -> Dict:
        """Get the schema definition for user data"""
        return {
            "table_name": "users",
            "description": "User account information and demographics",
            "columns": {
                "id": {
                    "type": "STRING",
                    "description": "Unique user identifier (UUID)",
                    "nullable": False,
                    "primary_key": True
                },
                "name": {
                    "type": "STRING",
                    "description": "User's full name",
                    "nullable": False,
                    "max_length": 255
                },
                "email": {
                    "type": "STRING",
                    "description": "User's email address",
                    "nullable": False,
                    "max_length": 255,
                    "unique": True
                },
                "address": {
                    "type": "STRING",
                    "description": "User's physical address",
                    "nullable": True,
                    "max_length": 500
                },
                "created_at": {
                    "type": "TIMESTAMP",
                    "description": "Account creation timestamp",
                    "nullable": False
                }
            },
            "indexes": ["email"],
            "partitions": ["created_at"]
        }
    
    def get_transaction_schema(self) -> Dict:
        """Get the schema definition for basic transaction data"""
        return {
            "table_name": "transactions",
            "description": "Basic transaction records",
            "columns": {
                "transaction_id": {
                    "type": "STRING",
                    "description": "Unique transaction identifier (UUID)",
                    "nullable": False,
                    "primary_key": True
                },
                "user_id": {
                    "type": "STRING",
                    "description": "Associated user identifier",
                    "nullable": False,
                    "foreign_key": "users.id"
                },
                "amount": {
                    "type": "DECIMAL(10,2)",
                    "description": "Transaction amount",
                    "nullable": False
                },
                "currency": {
                    "type": "STRING",
                    "description": "Currency code (ISO 4217)",
                    "nullable": False,
                    "max_length": 3
                },
                "timestamp": {
                    "type": "TIMESTAMP",
                    "description": "Transaction timestamp",
                    "nullable": False
                }
            },
            "indexes": ["user_id", "timestamp"],
            "partitions": ["timestamp"]
        }
    
    def get_detailed_transaction_schema(self) -> Dict:
        """Get the schema definition for detailed transaction data"""
        return {
            "table_name": "detailed_transactions",
            "description": "Comprehensive transaction records with metadata",
            "columns": {
                "transaction_id": {
                    "type": "STRING",
                    "description": "Unique transaction identifier (UUID)",
                    "nullable": False,
                    "primary_key": True
                },
                "user_id": {
                    "type": "STRING",
                    "description": "Associated user identifier",
                    "nullable": False,
                    "foreign_key": "users.id"
                },
                "amount": {
                    "type": "DECIMAL(10,2)",
                    "description": "Transaction amount (negative for debits)",
                    "nullable": False
                },
                "currency": {
                    "type": "STRING",
                    "description": "Currency code (ISO 4217)",
                    "nullable": False,
                    "max_length": 3
                },
                "transaction_type": {
                    "type": "STRING",
                    "description": "Type of transaction",
                    "nullable": False,
                    "enum": ["purchase", "refund", "transfer", "withdrawal", "deposit", "payment"]
                },
                "payment_method": {
                    "type": "STRING",
                    "description": "Payment method used",
                    "nullable": False,
                    "enum": ["credit_card", "debit_card", "bank_transfer", "paypal", "apple_pay", "google_pay", "cash"]
                },
                "merchant_name": {
                    "type": "STRING",
                    "description": "Merchant or company name",
                    "nullable": True,
                    "max_length": 255
                },
                "merchant_category": {
                    "type": "STRING",
                    "description": "Merchant category",
                    "nullable": True,
                    "enum": ["grocery", "restaurant", "gas_station", "retail", "online", "entertainment", "travel", "healthcare", "education", "utilities"]
                },
                "description": {
                    "type": "STRING",
                    "description": "Transaction description",
                    "nullable": True,
                    "max_length": 500
                },
                "status": {
                    "type": "STRING",
                    "description": "Transaction status",
                    "nullable": False,
                    "enum": ["completed", "pending", "failed", "cancelled", "processing"]
                },
                "timestamp": {
                    "type": "TIMESTAMP",
                    "description": "Transaction timestamp",
                    "nullable": False
                },
                "location": {
                    "type": "OBJECT",
                    "description": "Transaction location details",
                    "nullable": True,
                    "properties": {
                        "city": {"type": "STRING", "max_length": 100},
                        "state": {"type": "STRING", "max_length": 100},
                        "country": {"type": "STRING", "max_length": 100},
                        "latitude": {"type": "FLOAT"},
                        "longitude": {"type": "FLOAT"}
                    }
                },
                "metadata": {
                    "type": "OBJECT",
                    "description": "Transaction metadata",
                    "nullable": True,
                    "properties": {
                        "ip_address": {"type": "STRING", "max_length": 45},
                        "user_agent": {"type": "STRING", "max_length": 500},
                        "device_type": {"type": "STRING", "enum": ["mobile", "desktop", "tablet"]},
                        "channel": {"type": "STRING", "enum": ["online", "mobile_app", "in_store", "atm", "phone"]},
                        "reference_number": {"type": "STRING", "max_length": 20},
                        "fee_amount": {"type": "DECIMAL(5,2)"}
                    }
                },
                "card_details": {
                    "type": "OBJECT",
                    "description": "Card payment details (if applicable)",
                    "nullable": True,
                    "properties": {
                        "last_four_digits": {"type": "STRING", "max_length": 4},
                        "card_type": {"type": "STRING", "enum": ["Visa", "MasterCard", "American Express", "Discover"]},
                        "issuing_bank": {"type": "STRING", "max_length": 255}
                    }
                },
                "store_details": {
                    "type": "OBJECT",
                    "description": "Store details for in-store transactions",
                    "nullable": True,
                    "properties": {
                        "store_id": {"type": "STRING", "max_length": 20},
                        "terminal_id": {"type": "STRING", "max_length": 20},
                        "cashier_id": {"type": "STRING", "max_length": 20}
                    }
                }
            },
            "indexes": ["user_id", "timestamp", "transaction_type", "status"],
            "partitions": ["timestamp", "transaction_type"]
        }
    
    def get_all_schemas(self) -> Dict:
        """Get all schema definitions"""
        return {
            "users": self.get_user_schema(),
            "transactions": self.get_transaction_schema(),
            "detailed_transactions": self.get_detailed_transaction_schema()
        }
    
    def save_schemas(self, output_dir: str = None) -> Path:
        """Save all schemas to a JSON file"""
        if output_dir is None:
            output_dir = self.data_dir
        else:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
        
        schemas = self.get_all_schemas()
        schema_file = output_dir / "transaction_schemas.json"
        
        with open(schema_file, "w") as f:
            json.dump(schemas, f, indent=4)
        
        return schema_file
    
    def save_data_as_json(self, data: List[Dict], file_name: str) -> Path:
        """Save generated data to a JSON file"""

        file_path = self.data_dir / file_name
        
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        return file_path
    
    def save_data_as_csv(self, data: List[Dict], file_name: str) -> Path:
        """Save generated data to a CSV file"""

        file_path = self.data_dir / file_name
        
        if data:
            keys = data[0].keys()
            with open(file_path, "w", newline='') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
        return file_path
    
def main():
    """Main function to generate and save fake data."""
    generator = FakeDataGenerator()

    try:
        logger.info("Starting fake data generation", records_requested={"users": 100, "transactions": 100, "detailed_transactions": 200})
        
        # Generate user data
        with StreamingOperation(logger, "data_generation", event_type="user_data") as op:
            user_data = generator.generate_user_data(100)
            op.records_processed = len(user_data)
            
        user_json_path = generator.save_data_as_json(user_data, "fake_user_data.json")
        user_csv_path = generator.save_data_as_csv(user_data, "fake_user_data.csv")
        logger.info("User data successfully generated and saved", 
                   json_path=str(user_json_path), 
                   csv_path=str(user_csv_path),
                   records_count=len(user_data))

        # Generate basic transaction data
        with StreamingOperation(logger, "data_generation", event_type="transaction_data") as op:
            transaction_data = generator.generate_transaction_data(100)
            op.records_processed = len(transaction_data)
            
        transaction_json_path = generator.save_data_as_json(transaction_data, "fake_transaction_data.json")
        transaction_csv_path = generator.save_data_as_csv(transaction_data, "fake_transaction_data.csv")
        logger.info("Basic transaction data successfully generated and saved",
                   json_path=str(transaction_json_path),
                   csv_path=str(transaction_csv_path), 
                   records_count=len(transaction_data))
        
        # Generate detailed transaction data
        user_ids = [user["id"] for user in user_data[:20]]  # Use some actual user IDs
        with StreamingOperation(logger, "data_generation", event_type="detailed_transaction_data") as op:
            detailed_transactions = generator.generate_detailed_transaction_data(200, user_ids)
            op.records_processed = len(detailed_transactions)
            
        detailed_json_path = generator.save_data_as_json(detailed_transactions, "detailed_transaction_data.json")
        detailed_csv_path = generator.save_data_as_csv(detailed_transactions, "detailed_transaction_data.csv")
        logger.info("Detailed transaction data successfully generated and saved",
                   json_path=str(detailed_json_path),
                   csv_path=str(detailed_csv_path),
                   records_count=len(detailed_transactions))
        
        # Generate and save schemas
        schema_path = generator.save_schemas()
        logger.info("Schema definitions saved", schema_path=str(schema_path))
        
        # Log overall summary
        logger.info("🎉 Fake data generation completed successfully",
                   total_users=len(user_data),
                   total_basic_transactions=len(transaction_data),
                   total_detailed_transactions=len(detailed_transactions),
                   files_generated=6)  # 3 JSON + 3 CSV files
    
    except Exception as e:
        logger.error("Fake data generation failed", error=str(e), error_type=type(e).__name__)
        raise
