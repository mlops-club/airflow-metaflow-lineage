MERGE INTO nyc_taxi.raw_yellow AS target
USING (
    SELECT 
        LOWER(TO_HEX(MD5(TO_UTF8(
            CONCAT(
                COALESCE(CAST(vendorid AS VARCHAR), ''), 
                COALESCE(CAST(tpep_pickup_datetime AS VARCHAR), ''), 
                COALESCE(CAST(tpep_dropoff_datetime AS VARCHAR), ''), 
                COALESCE(CAST(pulocationid AS VARCHAR), ''), 
                COALESCE(CAST(dolocationid AS VARCHAR), ''), 
                COALESCE(CAST(fare_amount AS VARCHAR), ''), 
                COALESCE(CAST(trip_distance AS VARCHAR), ''), 
                COALESCE(CAST(total_amount AS VARCHAR), ''), 
                COALESCE(CAST(improvement_surcharge AS VARCHAR), ''), 
                COALESCE(CAST(congestion_surcharge AS VARCHAR), ''), 
                COALESCE(CAST(airport_fee AS VARCHAR), ''), 
                COALESCE(CAST(cbd_congestion_fee AS VARCHAR), ''), 
                COALESCE(CAST(passenger_count AS VARCHAR), ''), 
                COALESCE(CAST(ratecodeid AS VARCHAR), ''), 
                COALESCE(store_and_fwd_flag, ''), 
                COALESCE(CAST(payment_type AS VARCHAR), '')
            )
        )))) AS unique_row_id,
        regexp_extract("$path", '.*/([^/]+)$', 1)       AS filename,
        current_timestamp                               AS ingest_timestamp,
        vendorid, 
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        cbd_congestion_fee,
        airport_fee
    FROM nyc_taxi.staging_yellow
) AS source
ON target.unique_row_id = source.unique_row_id
WHEN MATCHED THEN 
    UPDATE SET 
        -- Update all values to the latest source values
        vendorid              = source.vendorid,
        tpep_pickup_datetime  = source.tpep_pickup_datetime,
        tpep_dropoff_datetime = source.tpep_dropoff_datetime,
        passenger_count       = source.passenger_count,
        trip_distance         = source.trip_distance,
        ratecodeid            = source.ratecodeid,
        store_and_fwd_flag    = source.store_and_fwd_flag,
        pulocationid          = source.pulocationid,
        dolocationid          = source.dolocationid,
        payment_type          = source.payment_type,
        fare_amount           = source.fare_amount,
        extra                 = source.extra,
        mta_tax               = source.mta_tax,
        tip_amount            = source.tip_amount,
        tolls_amount          = source.tolls_amount,
        improvement_surcharge = source.improvement_surcharge,
        total_amount          = source.total_amount,
        congestion_surcharge  = source.congestion_surcharge,
        cbd_congestion_fee    = source.cbd_congestion_fee,
        airport_fee           = source.airport_fee,
        filename              = source.filename,
        ingest_timestamp      = source.ingest_timestamp
WHEN NOT MATCHED THEN 
    INSERT VALUES (
        source.unique_row_id,
        source.filename,
        source.ingest_timestamp,
        source.vendorid,
        source.tpep_pickup_datetime,
        source.tpep_dropoff_datetime,
        source.passenger_count,
        source.trip_distance,
        source.ratecodeid,
        source.store_and_fwd_flag,
        source.pulocationid,
        source.dolocationid,
        source.payment_type,
        source.fare_amount,
        source.extra,
        source.mta_tax,
        source.tip_amount,
        source.tolls_amount,
        source.improvement_surcharge,
        source.total_amount,
        source.congestion_surcharge,
        source.cbd_congestion_fee,
        source.airport_fee
    );
