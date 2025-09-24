from pathlib import Path

from airflow.utils.context import Context

from elt_pipeline.utils.mysql_loader import MySQLLoader
from elt_pipeline.utils.psql_loader import PSQLLoader

# Factory Method
def get_data_loader(db_type: str, params: dict):
    if db_type == "mysql":
        return MySQLLoader(params)
    elif db_type == "postgresql":
        return PSQLLoader(params)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def extract_data_from_mysql(context: Context, run_config):
    """Extract data from MySQL"""

    updated_at = run_config.get("updated_at")
    if updated_at is None or updated_at == "":
        context.log.info("No updated_at provided, extracting all data.")
        return None
    context.log.info(f"Extracting data updated after {updated_at}")

    # Choose extract strategy
    sql_stm = f"""
        SELECT * FROM {run_config.get('source_tbl')}
        WHERE 1=1
    """

    if run_config.get("strategy") == "incremental_by_partition":
        if updated_at != "init_dump":
            sql_stm += f""" AND CAST({run_config.get('partition')} AS DATE)= '{updated_at}'"""

    elif run_config.get("strategy") == "incremental_by_watermark":
        data_loader = get_data_loader(run_config.get("db_provider"), run_config.get("target_db_params"))

        watermark = data_loader.get_watermark(
            f"{run_config.get('target_schema')}.{run_config.get('target_tbl')}",
            run_config.get("watermark")
        )

        watermark = updated_at if watermark is None or watermark > updated_at else watermark
        if updated_at != "init_dump":
            sql_stm += f""" AND {run_config.get('watermark')} >= '{watermark}'"""
        
    context.log.info(f"Extracting data with SQL: {sql_stm}")
    db_loader = MySQLLoader(run_config.get("source_db_params"))
    pd_data = db_loader.extract_data(sql_stm)
    context.log.info(f"Extracted {len(pd_data)} rows from MySQL")

    # Update params
    run_config.update({
        "updated_at" : updated_at, 
        "data" : pd_data,
        "path" : f"bronze/{run_config.get('data_source')}/{run_config.get('domain_name')}/{run_config.get('source_tbl')}"
    })
    return pd_data

def save_data_to_lake(data_frame, context: Context, run_config) -> Path:
    """Save extracted data to data lake in parquet format"""
    if data_frame is None or len(data_frame) == 0:
        context.log.info("No data extracted from MySQL, skipping save to data lake.")
        return None
    
    # Save data to data lake
    context.log.info(f"Saving data to data lake at {run_config.get('path')}")
    pd_stag = data_frame
    context.log.info(f"Dataframe shape: {pd_stag.shape}")

    timestamp = run_config.get("updated_at").replace("-", "_").replace(":", "_").replace(" ", "_")
    file_path = f"data_lake/{run_config.get('path')}_{timestamp}.parquet"
    pd_stag.to_parquet(
        file_path,
        index=False,
        compression='gzip', 
        engine='pyarrow', 
        overwrite=True
    )
    context.log.info(f"Saved {len(pd_stag)} rows to {file_path}")

    return file_path