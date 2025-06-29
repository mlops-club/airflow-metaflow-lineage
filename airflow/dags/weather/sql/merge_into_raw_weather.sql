MERGE INTO nyc_taxi.raw_weather AS target
USING (
    SELECT 
        LOWER(TO_HEX(MD5(TO_UTF8(
            CONCAT(
                COALESCE(region_type, ''),
                COALESCE(CAST(region_code AS VARCHAR), ''),
                COALESCE(CAST(year AS VARCHAR), ''),
                COALESCE(CAST(month AS VARCHAR), ''),
                COALESCE(meteorological_element, '')
            )
        )))) AS unique_row_id,
        regexp_extract("$path", '.*/([^/]+)$', 1) AS filename,
        current_timestamp AS ingest_timestamp,
        region_type,
        region_code,
        region_name,
        year,
        month,
        meteorological_element,
        CASE WHEN day_01 = -999.99 THEN NULL ELSE day_01 END AS day_01,
        CASE WHEN day_02 = -999.99 THEN NULL ELSE day_02 END AS day_02,
        CASE WHEN day_03 = -999.99 THEN NULL ELSE day_03 END AS day_03,
        CASE WHEN day_04 = -999.99 THEN NULL ELSE day_04 END AS day_04,
        CASE WHEN day_05 = -999.99 THEN NULL ELSE day_05 END AS day_05,
        CASE WHEN day_06 = -999.99 THEN NULL ELSE day_06 END AS day_06,
        CASE WHEN day_07 = -999.99 THEN NULL ELSE day_07 END AS day_07,
        CASE WHEN day_08 = -999.99 THEN NULL ELSE day_08 END AS day_08,
        CASE WHEN day_09 = -999.99 THEN NULL ELSE day_09 END AS day_09,
        CASE WHEN day_10 = -999.99 THEN NULL ELSE day_10 END AS day_10,
        CASE WHEN day_11 = -999.99 THEN NULL ELSE day_11 END AS day_11,
        CASE WHEN day_12 = -999.99 THEN NULL ELSE day_12 END AS day_12,
        CASE WHEN day_13 = -999.99 THEN NULL ELSE day_13 END AS day_13,
        CASE WHEN day_14 = -999.99 THEN NULL ELSE day_14 END AS day_14,
        CASE WHEN day_15 = -999.99 THEN NULL ELSE day_15 END AS day_15,
        CASE WHEN day_16 = -999.99 THEN NULL ELSE day_16 END AS day_16,
        CASE WHEN day_17 = -999.99 THEN NULL ELSE day_17 END AS day_17,
        CASE WHEN day_18 = -999.99 THEN NULL ELSE day_18 END AS day_18,
        CASE WHEN day_19 = -999.99 THEN NULL ELSE day_19 END AS day_19,
        CASE WHEN day_20 = -999.99 THEN NULL ELSE day_20 END AS day_20,
        CASE WHEN day_21 = -999.99 THEN NULL ELSE day_21 END AS day_21,
        CASE WHEN day_22 = -999.99 THEN NULL ELSE day_22 END AS day_22,
        CASE WHEN day_23 = -999.99 THEN NULL ELSE day_23 END AS day_23,
        CASE WHEN day_24 = -999.99 THEN NULL ELSE day_24 END AS day_24,
        CASE WHEN day_25 = -999.99 THEN NULL ELSE day_25 END AS day_25,
        CASE WHEN day_26 = -999.99 THEN NULL ELSE day_26 END AS day_26,
        CASE WHEN day_27 = -999.99 THEN NULL ELSE day_27 END AS day_27,
        CASE WHEN day_28 = -999.99 THEN NULL ELSE day_28 END AS day_28,
        CASE WHEN day_29 = -999.99 THEN NULL ELSE day_29 END AS day_29,
        CASE WHEN day_30 = -999.99 THEN NULL ELSE day_30 END AS day_30,
        CASE WHEN day_31 = -999.99 THEN NULL ELSE day_31 END AS day_31
    FROM nyc_taxi.staging_weather
) AS source
ON target.unique_row_id = source.unique_row_id
WHEN MATCHED THEN 
    UPDATE SET 
        region_type = source.region_type,
        region_code = source.region_code,
        region_name = source.region_name,
        year = source.year,
        month = source.month,
        meteorological_element = source.meteorological_element,
        day_01 = source.day_01, day_02 = source.day_02, day_03 = source.day_03,
        day_04 = source.day_04, day_05 = source.day_05, day_06 = source.day_06,
        day_07 = source.day_07, day_08 = source.day_08, day_09 = source.day_09,
        day_10 = source.day_10, day_11 = source.day_11, day_12 = source.day_12,
        day_13 = source.day_13, day_14 = source.day_14, day_15 = source.day_15,
        day_16 = source.day_16, day_17 = source.day_17, day_18 = source.day_18,
        day_19 = source.day_19, day_20 = source.day_20, day_21 = source.day_21,
        day_22 = source.day_22, day_23 = source.day_23, day_24 = source.day_24,
        day_25 = source.day_25, day_26 = source.day_26, day_27 = source.day_27,
        day_28 = source.day_28, day_29 = source.day_29, day_30 = source.day_30,
        day_31 = source.day_31,
        filename = source.filename,
        ingest_timestamp = source.ingest_timestamp
WHEN NOT MATCHED THEN 
    INSERT VALUES (
        source.unique_row_id,
        source.filename,
        source.ingest_timestamp,
        source.region_type,
        source.region_code,
        source.region_name,
        source.year,
        source.month,
        source.meteorological_element,
        source.day_01, source.day_02, source.day_03, source.day_04, source.day_05,
        source.day_06, source.day_07, source.day_08, source.day_09, source.day_10,
        source.day_11, source.day_12, source.day_13, source.day_14, source.day_15,
        source.day_16, source.day_17, source.day_18, source.day_19, source.day_20,
        source.day_21, source.day_22, source.day_23, source.day_24, source.day_25,
        source.day_26, source.day_27, source.day_28, source.day_29, source.day_30,
        source.day_31
    );
