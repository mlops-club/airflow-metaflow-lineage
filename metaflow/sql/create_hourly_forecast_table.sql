CREATE TABLE IF NOT EXISTS {{ glue_database }}.yellow_rides_hourly_forecast (
    forecast_created_at TIMESTAMP,
    year INT,
    month INT,
    day INT,
    hour INT,
    pulocationid INT,
    forecast_value INT
)
LOCATION 's3://{{ s3_bucket }}/iceberg/yellow_rides_hourly_forecast/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
