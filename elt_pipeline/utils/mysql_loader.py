import logging
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
from typing import Dict, Any, Optional

from .data_loader import DataLoader


class MySQLLoader(DataLoader):
    """MySQL specific data loader implementation."""

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._engine = None

    def get_db_connection(self):
        """Establish MySQL connection."""
        try:
            if self._engine is None:
                # For development with Docker MySQL, we'll use SSL parameters
                ssl_args = {
                    'ssl': {
                        'check_hostname': False,
                        'verify_mode': 0  # ssl.CERT_NONE equivalent
                    }
                }
                
                connection_string = (
                    f"mysql+pymysql://{self.params['user']}:{self.params['password']}"
                    f"@{self.params['host']}:{self.params['port']}/{self.params['database']}"
                )
                
                self.logger.info(f"Connecting to MySQL with mysql+pymysql://***:***@{self.params['host']}:{self.params['port']}/{self.params['database']}")
                self._engine = create_engine(connection_string, connect_args=ssl_args)
                
                # Test connection
                with self._engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    
            return self._engine
        except Exception as e:
            self.logger.error(f"MySQL connection failed: {e}")
            raise
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        """Extract data from MySQL database"""
        try:
            self.logger.info(f"Executing SQL: {sql}")
            
            # Use direct pymysql connection for pandas
            connection = pymysql.connect(
                host=self.params['host'],
                port=self.params['port'],
                user=self.params['user'],
                password=self.params['password'],
                database=self.params['database'],
                ssl={'check_hostname': False, 'verify_mode': 0}
            )
            
            df = pd.read_sql(sql, connection)
            connection.close()
                
            self.logger.info(f"Successfully extracted {len(df)} rows")
            return df
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            raise
    
    def load_data(self, pd_data: pd.DataFrame, params: Dict[str, Any]) -> int:
        """Load DataFrame to MySQL database"""
        try:
            engine = self.get_db_connection()
            
            # Use the engine directly for to_sql, which handles connections properly
            pd_data.to_sql(
                name=params['table_name'],
                con=engine,
                if_exists=params.get('if_exists', 'append'),
                index=False,
                chunksize=params.get('chunksize', 1000)
            )
            
            self.logger.info(f"Loaded {len(pd_data)} rows to MySQL")
            return len(pd_data)
        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            raise
    
    def get_watermark(self, table_name: str, watermark: str) -> Optional[str]:
        """Get watermark value for incremental loading"""
        try:
            sql = f"SELECT MAX({watermark}) FROM {table_name}"
            result = self.extract_data(sql)
            return str(result.iloc[0, 0]) if not result.empty and result.iloc[0, 0] is not None else None
        except Exception as e:
            self.logger.error(f"Watermark retrieval failed: {e}")
            return None

    def __enter__(self):
        """Context manager entry."""
        self.get_db_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with connection cleanup."""
        if self._engine:
            self._engine.dispose()
