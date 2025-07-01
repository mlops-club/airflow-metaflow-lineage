CREATE TABLE IF NOT EXISTS {{ glue_database }}.yellow_rides_hourly_forecast (
    day INT,
    hour INT,
    pulocationid INT,
    total_rides INT
)
LOCATION 's3://{{ datalake_bucket_name }}/yellow_rides_hourly_forecast/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
