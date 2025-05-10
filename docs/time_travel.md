# Time Travel with Apache Iceberg

This document explains how to use Apache Iceberg's time travel capabilities in the Climate Data Analytics Platform.

## What is Time Travel?

Time travel is a feature that allows you to query historical versions of your data. Apache Iceberg maintains snapshots of your tables, which represent the state of the table at specific points in time. With time travel, you can:

- Query data as it existed at a specific point in time
- Compare data between different versions
- Recover from accidental data modifications or deletions
- Audit changes to understand data lineage

Iceberg provides time travel through two main approaches:
1. **Snapshot ID** - Query a specific table snapshot by its ID
2. **Timestamp** - Query a table as it existed at a specific timestamp

## Time Travel with AWS Athena

The project supports Iceberg time travel through Athena using the implementation in `src/query_engine/athena/time_travel_example.py`. Athena supports Iceberg time travel through SQL extensions:

```sql
-- Query the latest version (default behavior)
SELECT * FROM climate_data.observations LIMIT 10;

-- Query a specific snapshot by ID
SELECT * FROM climate_data.observations FOR VERSION AS OF 6465591695946837774 LIMIT 10;

-- Query a snapshot as of a timestamp
SELECT * FROM climate_data.observations FOR TIMESTAMP AS OF TIMESTAMP '2023-07-01 12:00:00' LIMIT 10;
```

To get the list of available snapshots, you can query the metadata tables:

```sql
-- List all snapshots of a table
SELECT
    committed_at,
    snapshot_id,
    parent_id,
    operation,
    manifest_list
FROM "climate_data"."observations$snapshots"
ORDER BY snapshot_id DESC;
```

## Time Travel with DuckDB

The project also supports time travel through DuckDB using the implementation in `src/query_engine/duckdb/time_travel_example.py`. DuckDB supports Iceberg time travel through its Iceberg extension:

```sql
-- List all snapshots of a table
SELECT * FROM iceberg_scan_snapshots('s3://climate-lake-iceberg-tables/climate_data/observations');

-- Query a specific snapshot by ID
SELECT * FROM iceberg_scan_snapshot('s3://climate-lake-iceberg-tables/climate_data/observations', 3821550127947880959) LIMIT 10;

-- Query a snapshot as of a timestamp
SELECT * FROM iceberg_scan_timestamp('s3://climate-lake-iceberg-tables/climate_data/observations', TIMESTAMP '2023-07-01 12:00:00') LIMIT 10;
```

## Using the Time Travel Utility Scripts

The project includes utility scripts to make time travel operations easy:

### AWS Athena Example

```bash
# List snapshots for a table
python -m src.query_engine.athena.time_travel_example --table observations --list-snapshots

# Query a table at a specific snapshot ID
python -m src.query_engine.athena.time_travel_example --table observations --snapshot-id 3821550127947880959

# Query a table as it existed at a specific timestamp
python -m src.query_engine.athena.time_travel_example --table observations --timestamp "2023-07-01 12:00:00"

# Query a table as it existed some time ago
python -m src.query_engine.athena.time_travel_example --table observations --minutes-ago 60

# Add filtering to your query
python -m src.query_engine.athena.time_travel_example --table observations --snapshot-id 3821550127947880959 --where "year = 2022 AND month = 1"
```

### DuckDB Example

```bash
# List snapshots for a table
python -m src.query_engine.duckdb.time_travel_example --table observations --list-snapshots

# Query a table at a specific snapshot ID
python -m src.query_engine.duckdb.time_travel_example --table observations --snapshot-id 3821550127947880959

# Query a table as it existed at a specific timestamp
python -m src.query_engine.duckdb.time_travel_example --table observations --timestamp "2023-07-01 12:00:00"

# Query a table as it existed some time ago
python -m src.query_engine.duckdb.time_travel_example --table observations --minutes-ago 60

# Add filtering to your query
python -m src.query_engine.duckdb.time_travel_example --table observations --snapshot-id 3821550127947880959 --where "year = 2022 AND month = 1"
```

## Implementation Details

The time travel implementation relies on environment variables for configuration:

- `AWS_REGION`: AWS region for Athena/S3 (default: "ap-southeast-2")
- `NAMESPACE`: Database namespace (default: "climate_data")
- `ICEBERG_TABLES_BUCKET`: S3 bucket for Iceberg tables (default: "climate-lake-iceberg-tables")

For DuckDB, the implementation also:
- Automatically loads the Iceberg extension
- Configures S3 credentials if available in the environment
- Formats results in a tabular format for easy reading

## Retention of Historical Data

By default, Iceberg retains table snapshots indefinitely. However, you can configure snapshot retention policies to control how long historical data is kept.

To expire snapshots in AWS Athena:

```sql
-- Expire snapshots older than a timestamp
ALTER TABLE climate_data.observations EXECUTE EXPIRE_SNAPSHOTS(TIMESTAMP => TIMESTAMP '2023-01-01 00:00:00');

-- Expire snapshots and keep only the last 10 snapshots
ALTER TABLE climate_data.observations EXECUTE EXPIRE_SNAPSHOTS(RETAIN_LAST => 10);
```

In DuckDB, you can use the iceberg extension functions:

```sql
-- Expire snapshots
CALL iceberg_expire_snapshots('s3://climate-lake-iceberg-tables/climate_data/observations', TIMESTAMP '2023-01-01 00:00:00');
```

## Performance Considerations

Time travel to older snapshots may involve reading files that have been physically deleted but are still tracked in metadata for historical snapshots. This can impact query performance for very old snapshots. Consider using appropriate snapshot expiration policies to balance historical data needs with performance requirements. 