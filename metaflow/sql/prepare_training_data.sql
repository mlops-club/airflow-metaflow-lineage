-- Prepare training data by selecting records from actuals table
-- from as_of_datetime - lookback_days to as_of_datetime to ensure we have sufficient history
SELECT 
    pulocationid,
    year,
    month,
    day,
    hour,
    total_rides
FROM {{ glue_database }}.yellow_rides_hourly_actuals
WHERE DATE(CONCAT(CAST(year AS VARCHAR), '-', 
                  LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                  LPAD(CAST(day AS VARCHAR), 2, '0'))) >= DATE(SUBSTR('{{ as_of_datetime }}', 1, 10)) - INTERVAL '{{ lookback_days }}' DAY
  AND DATE(CONCAT(CAST(year AS VARCHAR), '-', 
                  LPAD(CAST(month AS VARCHAR), 2, '0'), '-',
                  LPAD(CAST(day AS VARCHAR), 2, '0'))) < DATE(SUBSTR('{{ as_of_datetime }}', 1, 10))
ORDER BY pulocationid, year, month, day, hour;
