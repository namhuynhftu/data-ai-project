import logging

import pandas as pd
from sqlalchemy import create_engine

from utils.data_loader import DataLoader

class MySQLLoader(DataLoader):
    """Get MySQL connection"""

    def get_db_connection(self):
        conn_info = (
            f"mysql+pymysql://{self.params['user']}:{self.params['password']}"
            f"@{self.params['host']}:{self.params['port']}/{self.params['database']}"
        )
        db_connection = create_engine(conn_info)
        logging.info(f"Connecting to MySQL with {conn_info}")
        return db_connection
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        """Extract data from MySQL database"""
        conn = self.get_db_connection()
        df = pd.read_sql(sql, conn)
        logging.info(f"Extracted {len(df)} rows from MySQL")
        return df

    def load_data(self, pd_data: pd.DataFrame, params: dict) -> int:
        """Load data into MySQL database"""
        pass

    def get_watermark(self, table_name, watermark: str) -> str:
        """Get watermark from"""
        pass
