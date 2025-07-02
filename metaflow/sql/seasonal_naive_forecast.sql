-- Seasonal naive forecast: predict based on the same day/hour/location from 7 days ago
WITH 
-- Generate hour sequence for forecast horizon
hour_sequence AS (
    SELECT ROW_NUMBER() OVER () - 1 as hour_offset
    FROM {{ glue_database }}.yellow_rides_hourly_actuals 
    LIMIT {{ predict_horizon_hours }}
),

-- Get active locations from recent data
active_locations AS (
    SELECT DISTINCT pulocationid 
    FROM {{ glue_database }}.yellow_rides_hourly_actuals
    WHERE year >= YEAR(TIMESTAMP '{{ as_of_datetime }}' - INTERVAL '{{ lookback_days }}' DAY)
      AND month >= MONTH(TIMESTAMP '{{ as_of_datetime }}' - INTERVAL '{{ lookback_days }}' DAY)
      AND day >= DAY(TIMESTAMP '{{ as_of_datetime }}' - INTERVAL '{{ lookback_days }}' DAY)
),

-- Generate forecast target timestamps and extract components
forecast_targets AS (
    SELECT 
        TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * hs.hour_offset as forecast_datetime,
        YEAR(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * hs.hour_offset) as year,
        MONTH(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * hs.hour_offset) as month,
        DAY(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * hs.hour_offset) as day,
        HOUR(TIMESTAMP '{{ as_of_datetime }}' + INTERVAL '1' HOUR * hs.hour_offset) as hour,
        al.pulocationid
    FROM hour_sequence hs
    CROSS JOIN active_locations al
),

-- Generate seasonal forecasts by looking back 7 days
seasonal_forecasts AS (
    SELECT 
        ft.year,
        ft.month,
        ft.day,
        ft.hour,
        ft.pulocationid,
        COALESCE(actuals.total_rides, 0) as forecast_value
    FROM forecast_targets ft
    LEFT JOIN {{ glue_database }}.yellow_rides_hourly_actuals actuals
        ON actuals.year = YEAR(ft.forecast_datetime - INTERVAL '7' DAY)
        AND actuals.month = MONTH(ft.forecast_datetime - INTERVAL '7' DAY)
        AND actuals.day = DAY(ft.forecast_datetime - INTERVAL '7' DAY)
        AND actuals.hour = ft.hour
        AND actuals.pulocationid = ft.pulocationid
)

SELECT 
    year,
    month,
    day,
    hour,
    pulocationid,
    forecast_value
FROM seasonal_forecasts
ORDER BY year, month, day, hour, pulocationid;
