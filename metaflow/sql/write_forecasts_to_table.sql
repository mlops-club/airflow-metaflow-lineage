-- Insert seasonal naive forecasts into the forecast table in an idempotent manner
MERGE INTO {{ glue_database }}.yellow_rides_hourly_forecast AS target
USING (
    -- Seasonal naive forecast: predict based on the same day/hour/location from 7 days ago
    WITH forecast_targets AS (
        -- Generate forecast targets for the next 24 hours from as_of_datetime
        SELECT 
            TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq as forecast_datetime,
            YEAR(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq) as year,
            MONTH(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq) as month,
            DAY(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq) as day,
            HOUR(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq) as hour,
            pulocationid
        FROM (
            SELECT ROW_NUMBER() OVER () - 1 as seq
            FROM {{ glue_database }}.yellow_rides_hourly_actuals 
            LIMIT {{ predict_horizon_hours }}
        ) AS hours
        CROSS JOIN (
            SELECT DISTINCT pulocationid 
            FROM {{ glue_database }}.yellow_rides_hourly_actuals
            WHERE DATE(CONCAT(CAST(year AS VARCHAR), '-', 
                             LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                             LPAD(CAST(day AS VARCHAR), 2, '0'))) >= DATE(SUBSTR('{{ as_of_datetime }}', 1, 10)) - INTERVAL '{{ lookback_days }}' DAY
        ) AS locations
    ),
    seasonal_forecasts AS (
        SELECT 
            ft.year,
            ft.month,
            ft.day,
            ft.hour,
            ft.pulocationid,
            COALESCE(actuals.total_rides, 0) as forecast_value,
            TIMESTAMP '{{ as_of_datetime }}' as forecast_created_at
        FROM forecast_targets ft
        LEFT JOIN {{ glue_database }}.yellow_rides_hourly_actuals actuals
            ON DATE(CONCAT(CAST(actuals.year AS VARCHAR), '-', 
                           LPAD(CAST(actuals.month AS VARCHAR), 2, '0'), '-',
                           LPAD(CAST(actuals.day AS VARCHAR), 2, '0'))) = DATE(ft.forecast_datetime - INTERVAL '7' DAY)
            AND actuals.hour = ft.hour
            AND actuals.pulocationid = ft.pulocationid
    )
    SELECT * FROM seasonal_forecasts
) AS source
ON (
    target.year = source.year
    AND target.month = source.month
    AND target.day = source.day
    AND target.hour = source.hour
    AND target.pulocationid = source.pulocationid
    AND target.forecast_created_at = source.forecast_created_at
)
WHEN MATCHED THEN
    UPDATE SET 
        forecast_value = source.forecast_value
WHEN NOT MATCHED THEN
    INSERT (forecast_created_at, year, month, day, hour, pulocationid, forecast_value)
    VALUES (source.forecast_created_at, source.year, source.month, source.day, source.hour, source.pulocationid, source.forecast_value);
