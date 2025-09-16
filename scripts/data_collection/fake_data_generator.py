import json
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from faker import Faker

class FakeDataGenerator:
    def __init__(self):
        self.faker = Faker()
        self.data_dir = Path("data/external")
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_user_data(self, num_records: int =100) -> List[Dict]:
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
    
    def generate_transaction_data(self, num_records: int =100) -> List[Dict]:
        """Generate fake transaction data"""

        transaction_data = []
        for _ in range(num_records):
            transaction = {
                "transaction_id": self.faker.uuid4(),
                "user_id": self.faker.uuid4(),
                "amount": round(self.faker.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
                "currency": self.faker.currency_code(),
                "timestamp": datetime.now().isoformat()
            }
            transaction_data.append(transaction)
        return transaction_data
    
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
        user_data = generator.generate_user_data(100)
        user_json_path = generator.save_data_as_json(user_data, "fake_user_data.json")
        user_csv_path = generator.save_data_as_csv(user_data, "fake_user_data.csv")
        logging.info(f"User data successfully saved to {user_json_path} and {user_csv_path}")

        transaction_data = generator.generate_transaction_data(100)
        transaction_json_path = generator.save_data_as_json(transaction_data, "fake_transaction_data.json")
        transaction_csv_path = generator.save_data_as_csv(transaction_data, "fake_transaction_data.csv")
        logging.info(f"Transaction data successfully saved to {transaction_json_path} and {transaction_csv_path}")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()