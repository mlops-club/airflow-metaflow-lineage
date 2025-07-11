-- Calculate and merge/upsert hourly ride actuals for the specified date range
MERGE INTO {{ glue_database }}.yellow_rides_hourly_actuals AS target
USING (
    SELECT 
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
        year(tpep_pickup_datetime),
        month(tpep_pickup_datetime),
        day(tpep_pickup_datetime),
        hour(tpep_pickup_datetime),
        pulocationid
) AS source
ON (
    target.year = source.year
    AND target.month = source.month
    AND target.day = source.day
    AND target.hour = source.hour 
    AND target.pulocationid = source.pulocationid
)
WHEN MATCHED THEN
    UPDATE SET 
        total_rides = source.total_rides,
        created_at = source.created_at
WHEN NOT MATCHED THEN
    INSERT (year, month, day, hour, pulocationid, total_rides, created_at)
    VALUES (source.year, source.month, source.day, source.hour, source.pulocationid, source.total_rides, source.created_at);
