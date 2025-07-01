-- Prepare training data by selecting records from actuals table
-- from as_of_datetime - lookback_days back to ensure we have sufficient history
SELECT 
    forecast_date,
    year,
    month,
    day,
    hour,
    pulocationid,
    total_rides
FROM {{ glue_database }}.yellow_rides_hourly_actuals
WHERE forecast_date >= DATE('{{ training_start_date }}')
  AND forecast_date < DATE('{{ as_of_datetime }}')
ORDER BY forecast_date, hour, pulocationid;
