CREATE TABLE IF NOT EXISTS {{ var.value.get("datalake-glue-database") }}.raw_weather (
    unique_row_id STRING,
    filename STRING,
    ingest_timestamp TIMESTAMP,
    region_type STRING,
    region_code BIGINT,
    region_name STRING,
    year BIGINT,
    month BIGINT,
    meteorological_element STRING,
    day_01 DOUBLE, day_02 DOUBLE, day_03 DOUBLE, day_04 DOUBLE, day_05 DOUBLE,
    day_06 DOUBLE, day_07 DOUBLE, day_08 DOUBLE, day_09 DOUBLE, day_10 DOUBLE,
    day_11 DOUBLE, day_12 DOUBLE, day_13 DOUBLE, day_14 DOUBLE, day_15 DOUBLE,
    day_16 DOUBLE, day_17 DOUBLE, day_18 DOUBLE, day_19 DOUBLE, day_20 DOUBLE,
    day_21 DOUBLE, day_22 DOUBLE, day_23 DOUBLE, day_24 DOUBLE, day_25 DOUBLE,
    day_26 DOUBLE, day_27 DOUBLE, day_28 DOUBLE, day_29 DOUBLE, day_30 DOUBLE,
    day_31 DOUBLE
)
PARTITIONED BY (year, month, meteorological_element)
LOCATION 's3://{{ var.value.get("datalake-s3-bucket") }}/raw_weather/'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_compression' = 'snappy'
);
