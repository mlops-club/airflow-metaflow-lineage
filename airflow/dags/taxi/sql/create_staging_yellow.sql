CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi.staging_yellow (
    vendorid INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    ratecodeid DOUBLE,
    store_and_fwd_flag STRING,
    pulocationid INT,
    dolocationid INT,
    payment_type BIGINT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    cbd_congestion_fee DOUBLE,
    airport_fee DOUBLE
)
STORED AS PARQUET
LOCATION 's3://nyc-taxi-datalake-glue-nyc-taxi/staging/yellow/'
TBLPROPERTIES ("parquet.compress"="SNAPPY");
