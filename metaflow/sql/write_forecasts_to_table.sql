-- Insert seasonal naive forecasts into the forecast table in an idempotent manner
MERGE INTO {{ glue_database }}.yellow_rides_hourly_forecast AS target
USING (
    -- Seasonal naive forecast: predict based on the same day/hour/location from 7 days ago
    WITH forecast_targets AS (
        -- Generate forecast targets for the next 24 hours from as_of_datetime
        SELECT 
            TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq as forecast_datetime,
            DATE(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * seq) as forecast_date,
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
            WHERE forecast_date >= DATE(TIMESTAMP '{{ as_of_datetime }}') - INTERVAL '{{ lookback_days }}' DAY
        ) AS locations
    ),
    seasonal_forecasts AS (
        SELECT 
            ft.forecast_date,
            ft.year,
            ft.month,
            ft.day,
            ft.hour,
            ft.pulocationid,
            'seasonal_naive' as forecast_type,
            COALESCE(actuals.total_rides, 0) as forecast_value,
            TIMESTAMP '{{ as_of_datetime }}' as forecast_created_at,
            TIMESTAMP '{{ as_of_datetime }}' as created_at
        FROM forecast_targets ft
        LEFT JOIN {{ glue_database }}.yellow_rides_hourly_actuals actuals
            ON actuals.forecast_date = DATE(ft.forecast_datetime - INTERVAL '7' DAY)
            AND actuals.hour = ft.hour
            AND actuals.pulocationid = ft.pulocationid
    )
    SELECT * FROM seasonal_forecasts
) AS source
ON (
    target.forecast_date = source.forecast_date
    AND target.hour = source.hour
    AND target.pulocationid = source.pulocationid
    AND target.forecast_type = source.forecast_type
    AND target.forecast_created_at = source.forecast_created_at
)
WHEN MATCHED THEN
    UPDATE SET 
        forecast_value = source.forecast_value,
        created_at = source.created_at
WHEN NOT MATCHED THEN
    INSERT (forecast_date, forecast_created_at, year, month, day, hour, pulocationid, forecast_type, forecast_value, created_at)
    VALUES (source.forecast_date, source.forecast_created_at, source.year, source.month, source.day, source.hour, source.pulocationid, source.forecast_type, source.forecast_value, source.created_at);
