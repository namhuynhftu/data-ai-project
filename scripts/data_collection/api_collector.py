import json 
import logging
import os
import time 
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

# Load environment variables.
load_dotenv()

class APIDataCollector:
    def __init__(self, api_name: str, api_key: Optional[str] = None):
        self.api_name = api_name
        self.api_url = os.getenv("API_URL")
        self.max_records = int(os.getenv("MAX_RECORDS", 100))
        self.batch_size = int(os.getenv("BATCH_SIZE", 10))
        self.max_retries = int(os.getenv("MAX_RETRIES", 3))        
        self.data_dir = Path("data/external")
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def collect_data(self, end_point: str) -> List[Dict]:
        """Collect data from the API endpoint"""

        collector = requests.get(f"{self.api_url}/{end_point}").json()
        all_data = []
        total_records = min(len(collector), self.max_records)   

        for start in range(0, total_records, self.batch_size):
            end = min(start + self.batch_size, total_records)
            batch = collector[start:end]
            all_data.extend(batch)

            # To respect API rate limits
            time.sleep(1)  

        return all_data

    def save_data(self, data: List[Dict], file_name: str) -> Path:
        """Save collected data to a JSON file"""

        file_date = datetime.now().strftime("%Y%m%d")
        file_path = self.data_dir / f"{file_name}_{file_date}.json"

        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
        return file_path

def main():
    """Main function to collect and save data from the API."""
    collector = APIDataCollector("user_data_api")

    try:
        data = collector.collect_data("/users")
        file_path = collector.save_data(data, "user_data")
        logging.info(f"Data successfully saved to {file_path}")
    except Exception as e:
        logging.error(f"An error occurred: {e}") 
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()