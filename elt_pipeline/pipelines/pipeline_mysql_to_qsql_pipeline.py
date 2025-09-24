import json 
import os
from datetime import datetime

from elt_pipeline.ops.extract_data_from_mysql import extract_data_from_mysql
from elt_pipeline.ops.extract_data_from_mysql import save_data_to_lake
from elt_pipeline.ops.load_data_to_psql import load_data_to_psql

def get_config():

    start = datetime(2025,12,1)
    start_date = start.strftime("%Y-%m-%d")
    with open("pipelines/metadata/mssql2psql_ingestion.json", "r") as f:
        metadata = json.load(f)
    
    partition_map = {}

    for table, run_config in metadata:
        partition_map[f"Extract_mysql_{table}"] = {"config": {"created_at": start_date}}

    run_config = {
        "op" : partition_map
    }

    return run_config

def pipeline_mysql_to_qsql_pipeline():
    """Read metadata"""

    with open("pipelines/metadata/mssql2psql_ingestion.json", "r") as f:
        metadata = json.load(f)
    
    op_output = []

    for table, run_config in metadata:
        # Source
        run_config["data_source"] = "ecommerce"
        run_config["domain_name"] = "marketing"
        run_config["source_db_params"] = {
            "host": os.getenv("MYSQL_HOST"),
            "port": os.getenv("MYSQL_PORT"),
            "database": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER"),
            "password": os.getenv("MYSQL_PASSWORD")
        }

    pd_data = extract_data_from_mysql(f"extract_mysql_{table}", run_config)
    load_to_lake = save_data_to_lake(pd_data)

if __name__ == "__main__":
    pipeline_mysql_to_qsql_pipeline()