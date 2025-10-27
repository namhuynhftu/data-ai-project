# Metadata Files

This folder contains metadata files used by the ETL pipeline.

## loaded_at.json

Tracks incremental load timestamps for tables using the `incremental_by_watermark` strategy.

### Structure

```json
{
  "metadata_description": "Tracks incremental load timestamps for tables using incremental_by_watermark strategy",
  "last_updated": "2025-10-27T00:00:00",
  "tables": {
    "table_name": {
      "load_from": null,
      "load_at": "2018-10-01 00:00:00",
      "watermark_column": "timestamp_column",
      "strategy": "incremental_by_watermark"
    }
  }
}
```
``` default date value: 2018-01-01 00:00:00 ```

### How It Works

1. **Initial Load (load_from = null)**:
   - When `load_from` is `null`, the pipeline performs the first incremental load
   - Extracts data from the beginning up to the `load_at` timestamp
   - SQL: `WHERE watermark_column <= 'load_at'`

2. **After First Load**:
   - Pipeline updates: `load_from = load_at` (previous load_at value)
   - Pipeline updates: `load_at = current_timestamp` or new watermark value
   
3. **Subsequent Loads**:
   - Extracts data between `load_from` (exclusive) and `load_at` (inclusive)
   - SQL: `WHERE watermark_column > 'load_from' AND watermark_column <= 'load_at'`
   - After load: `load_from = load_at`, `load_at = new_timestamp`

### Example Flow

**Run 1** (Initial Load):
```json
{
  "load_from": null,
  "load_at": "2018-10-01 00:00:00"
}
```
- Loads: all data where `watermark_column <= '2018-10-01 00:00:00'`

**After Run 1** (Auto-updated):
```json
{
  "load_from": "2018-10-01 00:00:00",
  "load_at": "2025-10-27 12:00:00"
}
```

**Run 2** (Incremental Load):
- Loads: data where `watermark_column > '2018-10-01 00:00:00' AND watermark_column <= '2025-10-27 12:00:00'`

**After Run 2** (Auto-updated):
```json
{
  "load_from": "2025-10-27 12:00:00",
  "load_at": "2025-10-27 18:00:00"
}
```

### Configuration

The initial `load_at` value should be set to a date that makes sense for your data:
- For historical data: use the earliest date you want to load (e.g., "2018-01-01 00:00:00")
- For recent data: use a more recent date (e.g., "2024-01-01 00:00:00")

### Tables Tracked

Currently tracking these tables with incremental strategy:
- `olist_order_items_dataset` (watermark: `shipping_limit_date`)
- `olist_order_reviews_dataset` (watermark: `review_creation_date`)
- `olist_orders_dataset` (watermark: `order_purchase_timestamp`)

### Utility Functions

See `elt_pipeline/batch/utils/loaded_at_tracker.py` for helper functions:
- `get_table_loaded_at(table_name)` - Get current load_from and load_at
- `update_table_loaded_at(table_name, new_watermark)` - Update after successful load
- `initialize_table_loaded_at(table_name, initial_load_at, watermark_column)` - Add new table
