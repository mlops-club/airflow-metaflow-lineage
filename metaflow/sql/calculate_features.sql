-- Create the actuals table if it doesn't exist
CREATE TABLE IF NOT EXISTS {{ glue_database }}.yellow_rides_hourly_actuals (
    year INT,
    day INT,
    hour INT,
    pulocationid INT,
    total_rides INT
)
LOCATION 's3://{{ datalake_bucket_name }}/iceberg/yellow_rides_hourly_forecast/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);

-- Calculate and merge/upsert hourly ride actuals for the specified date range
MERGE INTO {{ glue_database }}.yellow_rides_hourly_actuals AS target
USING (
    SELECT 
        year(tpep_pickup_datetime) as year,
        day(tpep_pickup_datetime) as day,
        hour(tpep_pickup_datetime) as hour,
        pulocationid,
        count(*) as total_rides
    FROM {{ glue_database }}.raw_yellow
    WHERE tpep_pickup_datetime >= TIMESTAMP '{{ start_datetime }}'
      AND tpep_pickup_datetime < TIMESTAMP '{{ end_datetime }}'
      AND pulocationid IS NOT NULL
    GROUP BY 
        year(tpep_pickup_datetime),
        day(tpep_pickup_datetime),
        hour(tpep_pickup_datetime),
        pulocationid
) AS source
ON (
    target.year = source.year
    AND target.day = source.day 
    AND target.hour = source.hour 
    AND target.pulocationid = source.pulocationid
)
WHEN MATCHED THEN
    UPDATE SET total_rides = source.total_rides
WHEN NOT MATCHED THEN
    INSERT (year, day, hour, pulocationid, total_rides)
    VALUES (source.year, source.day, source.hour, source.pulocationid, source.total_rides);
