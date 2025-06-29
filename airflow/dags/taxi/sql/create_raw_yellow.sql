CREATE TABLE IF NOT EXISTS nyc_taxi.raw_yellow (
    unique_row_id         STRING,
    filename              STRING,
    ingest_timestamp      TIMESTAMP,
    vendorid              INT,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       BIGINT,
    trip_distance         DOUBLE,
    ratecodeid            BIGINT,
    store_and_fwd_flag    STRING,
    pulocationid          INT,
    dolocationid          INT,
    payment_type          BIGINT,
    fare_amount           DOUBLE,
    extra                 DOUBLE,
    mta_tax               DOUBLE,
    tip_amount            DOUBLE,
    tolls_amount          DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount          DOUBLE,
    congestion_surcharge  DOUBLE,
    cbd_congestion_fee    DOUBLE,
    airport_fee           DOUBLE
)
PARTITIONED BY (day(tpep_pickup_datetime))
LOCATION 's3://nyc-taxi-datalake-glue-nyc-taxi/raw_yellow/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
