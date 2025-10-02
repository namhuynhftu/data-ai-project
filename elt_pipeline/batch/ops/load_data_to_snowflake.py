import pandas as pd

from utils.psql_loader import PSQLLoader

def load_data_to_psql(context, upstream):
    if upstream is None:
        context.log.info("No data extracted from MySQL, skipping load to PostgreSQL.")
        return None
    
    # Load data to target PostgreSQL
    context.log.info(f"Loading data to PostgreSQL table {upstream.get('target_tbl')}")
    context.log.info(f"Extracting data from {upstream.get('path')}, total rows: {len(upstream.get('data'))}")

    pd_stag = pd.read_csv(
        upstream.get('path'),
        sep=",",
        compression='gzip',
        dtype=upstream.get('load_dtypes')
        )
    context.log.info(f"Read {len(pd_stag)} rows from {upstream.get('path')}")

    if len(pd_stag) == 0:
        context.log.info("No data to load, skipping.")
        return "No data to load, skipping."
    
    # Execute load
    db_loader = PSQLLoader(upstream.get("target_db_params"))
    rows_loaded = db_loader.load_data(pd_stag, upstream)
    context.log.info(f"Loaded {rows_loaded} rows to PostgreSQL table {upstream.get('target_tbl')}")
    return rows_loaded

