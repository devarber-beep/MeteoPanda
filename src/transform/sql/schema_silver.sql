CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.weather_cleaned AS
SELECT
    CAST(date AS DATE) AS date,
    city,
    region,
    station,
    lat,
    lon,
    CAST(strftime('%Y', date) AS INT) AS year,
    tavg AS temp_avg_c,
    tmin AS temp_min_c,
    tmax AS temp_max_c,
    prcp AS precip_mm,
    wdir AS wind_dir_deg,
    wspd AS wind_avg_kmh,
    wpgt AS wind_gust_kmh,
    pres AS pressure_hpa,
    snow AS snow_depth_cm,
    tsun AS sunshine_min,
    rhum AS humidity_percent,
    source,
    CURRENT_TIMESTAMP AS ingestion_date
FROM __UNION_ALL_BRONZE_TABLES__
WHERE
    date IS NOT NULL
    AND tavg IS NOT NULL
    AND tmin IS NOT NULL
    AND tmax IS NOT NULL
    --AND tavg BETWEEN -50 AND 60
    --AND tmin BETWEEN -60 AND 60
    --AND tmax BETWEEN -60 AND 70

