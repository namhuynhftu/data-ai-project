import logging
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
from typing import Dict, Any, Optional

from elt_pipeline.batch.utils.data_loader import DataLoader


class MySQLLoader(DataLoader):
    """MySQL specific data loader implementation."""

    def __init__(self, params: Dict[str, Any]):
        super().__init__(params)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._engine = None

    def get_db_connection(self, params: Dict[str, Any] = None) -> Any:
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
            # Use pymysql directly for better compatibility with SSL
            import pymysql
            import ssl
            
            # Create SSL context for MySQL connection
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            connection = pymysql.connect(
                host=self.params['host'],
                port=self.params['port'], 
                user=self.params['user'],
                password=self.params['password'],
                database=self.params['database'],
                charset='utf8mb4',
                ssl=ssl_context,
                ssl_verify_cert=False,
                ssl_verify_identity=False
            )
            
            df = pd.read_sql(sql, con=connection)
            connection.close()
            
            self.logger.info(f"Successfully extracted {len(df)} rows from database")
            return df
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            raise
    
    def load_data(self, pd_data: pd.DataFrame, params: Dict[str, Any]) -> int:
        """Load DataFrame to MySQL database"""
        pass
    
    def get_watermark(self, table_name: str, watermark: str) -> Optional[str]:
        """Get watermark value for incremental loading"""
        try:
            engine = self.get_db_connection()
            query = text(f"SELECT MAX({watermark}) AS max_watermark FROM {table_name}")
            with engine.connect() as conn:
                result = conn.execute(query).fetchone()
                return result['max_watermark'] if result and result['max_watermark'] is not None else None
        except Exception as e:
            self.logger.error(f"Failed to get watermark: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.get_db_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with connection cleanup."""
        if self._engine:
            self._engine.dispose()
