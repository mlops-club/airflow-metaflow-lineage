-- -- Create the actuals table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS {{ glue_database }}.yellow_rides_hourly_actuals (
--     forecast_date DATE,
--     year INT,
--     month INT, 
--     day INT,
--     hour INT,
--     pulocationid INT,
--     total_rides INT,
--     created_at TIMESTAMP
-- )
-- LOCATION 's3://{{ s3_bucket }}/iceberg/yellow_rides_hourly_actuals/'
-- TBLPROPERTIES (
--     'table_type' = 'ICEBERG',
--     'format' = 'parquet',
--     'write_compression' = 'snappy'
-- );

-- Calculate and merge/upsert hourly ride actuals for the specified date range
MERGE INTO {{ glue_database }}.yellow_rides_hourly_actuals AS target
USING (
    SELECT 
        DATE(tpep_pickup_datetime) as forecast_date,
        year(tpep_pickup_datetime) as year,
        month(tpep_pickup_datetime) as month,
        day(tpep_pickup_datetime) as day,
        hour(tpep_pickup_datetime) as hour,
        pulocationid,
        count(*) as total_rides,
        current_timestamp as created_at
    FROM {{ glue_database }}.raw_yellow
    WHERE tpep_pickup_datetime >= TIMESTAMP '{{ start_datetime }}'
      AND tpep_pickup_datetime < TIMESTAMP '{{ end_datetime }}'
      AND pulocationid IS NOT NULL
    GROUP BY 
        DATE(tpep_pickup_datetime),
        year(tpep_pickup_datetime),
        month(tpep_pickup_datetime),
        day(tpep_pickup_datetime),
        hour(tpep_pickup_datetime),
        pulocationid
) AS source
ON (
    target.forecast_date = source.forecast_date
    AND target.hour = source.hour 
    AND target.pulocationid = source.pulocationid
)
WHEN MATCHED THEN
    UPDATE SET 
        total_rides = source.total_rides,
        created_at = source.created_at
WHEN NOT MATCHED THEN
    INSERT (forecast_date, year, month, day, hour, pulocationid, total_rides, created_at)
    VALUES (source.forecast_date, source.year, source.month, source.day, source.hour, source.pulocationid, source.total_rides, source.created_at);
