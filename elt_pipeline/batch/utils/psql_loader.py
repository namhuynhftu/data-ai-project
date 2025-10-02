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
        """Load data into PostgreSQL database"""
        load_datetime = datetime.now().strftime("%Y_%m_%d")
        temp_table = f"{params.get('target_tbl')}_temp_{load_datetime}"

        conn = self.get_db_connection()
        with conn.connect() as cursor:
            # Create temporary table 
            temp_tbl_query = f"""
                CREATE TEMP TABLE IF NOT EXISTS {temp_table} (LIKE {params.get('target_tbl')} INCLUDING ALL);
            """
            cursor.execute(temp_tbl_query)

            # Insert data into temporary table
            pd_data[params.get('columns')].to_sql(
                temp_table, 
                conn, 
                if_exists="replace", 
                chunksize=10000,
                method='multi',
                index=False)

            logging.info(f"Temporary table {temp_table} created.")

            # Check was data in temp table
        with conn.connect() as cursor:

            count_query = f"SELECT COUNT(1) as count FROM {temp_table};"
            count_result = cursor.execute(count_query)
            if count_result["count"].iloc[0] == 0:
                logging.warning(f"No data found in temporary table {temp_table}. Aborting load.")
            else:
                logging.info(f"Inserted {count_result['count'].iloc[0]} rows into temporary table {temp_table}.")
    
            # Upsert data
            if params.get("primary_key"):
                condition = " AND ".join(
                    [f"{params.get('target_tbl')}.{key} = {params.get('source_tbl')}.{key}" for key in params.get("primary_key")]
                )
                upsert_query = f"""
                    BEGIN TRANSACTION;
                    DELETE FROM {params.get('target_tbl')}
                    USING {temp_table} 
                    WHERE {condition};

                    INSERT INTO {params.get('target_tbl')} ({', '.join(params.get('columns'))})
                """


    def get_watermark(self, table_name, watermark: str) -> str:
        """Get watermark from PostgreSQL table"""
        conn = self.get_db_connection()
        query = f"SELECT MAX({watermark}) AS max_watermark FROM {table_name}"
        result = pd.read_sql(query, conn)
        return result["max_watermark"].iloc[0] if not result.empty else None