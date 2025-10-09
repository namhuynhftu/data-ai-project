import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from elt_pipeline.batch.utils.data_loader import DataLoader

logger = logging.getLogger(__name__)

class PSQLLoader(DataLoader):

    def get_db_connection(self):
        """Get PostgreSQL connection"""

        conn_info = (
            f"postgresql+psycopg2://{self.params['user']}:{self.params['password']}"
            f"@{self.params['host']}:{self.params['port']}/{self.params['database']}"
        )
        logging.info(f"Connecting to PostgreSQL with {conn_info}")
        db_connection = create_engine(conn_info)
        return db_connection

    def extract_data(self, sql: str) -> pd.DataFrame:
        """Extract data from PostgreSQL database"""
        pass

    def load_data(self, pd_data: pd.DataFrame, params: dict) -> int:
        """Load data into PostgreSQL database using raw SQL"""
        import psycopg2
        from psycopg2.extras import execute_values
        import numpy as np
        
        try:
            # Create direct psycopg2 connection
            conn = psycopg2.connect(
                host=self.params['host'],
                port=self.params['port'],
                database=self.params['database'],
                user=self.params['user'],
                password=self.params['password']
            )
            
            cursor = conn.cursor()
            
            # Get columns and data
            columns = params.get('columns')
            table_name = params.get('target_tbl')
            
            # Add schema prefix if schema is configured
            if 'schema' in self.params and self.params['schema']:
                full_table_name = f"{self.params['schema']}.{table_name}"
            else:
                full_table_name = table_name
            
            # Prepare column names and placeholders
            column_names = ', '.join(columns)
            
            # Clean the data - replace NaN values with None for PostgreSQL
            clean_data = pd_data[columns].copy()
            # Handle multiple types of NaN values - use where to replace NaN with None
            clean_data = clean_data.where(pd.notnull(clean_data), None)
            # Also handle string representations of NaN
            clean_data = clean_data.replace({'NaN': None, 'nan': None, 'null': None, '': None})
            
            # Convert DataFrame to list of tuples
            data_tuples = [tuple(row) for row in clean_data.values]
            
            # Insert data using execute_values for better performance
            insert_query = f"INSERT INTO {full_table_name} ({column_names}) VALUES %s"
            execute_values(cursor, insert_query, data_tuples, page_size=1000)
            
            # Commit transaction
            conn.commit()
            
            # Get actual count of rows inserted
            actual_rows = len(data_tuples)
            
            logging.info(f"Successfully inserted {actual_rows} rows into {full_table_name}")
            
            # Cleanup
            cursor.close()
            conn.close()
            
            return actual_rows
        
        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            logging.error(f"Error loading data: {str(e)}")
            raise


    def get_watermark(self, table_name, watermark: str) -> str:
        """Get watermark from PostgreSQL table"""
        conn = self.get_db_connection()
        query = f"SELECT MAX({watermark}) AS max_watermark FROM {table_name}"
        result = pd.read_sql(query, conn)
        return result["max_watermark"].iloc[0] if not result.empty else None