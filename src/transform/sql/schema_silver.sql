CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.weather_cleaned AS
SELECT
    DATE(date) AS date,
    city,
    region,
    strftime('%Y', date) AS year,
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
    source,
    ingestion_date,
    lat,
    lon,
    alt
FROM bronze.weather_with_metadata
WHERE
    date IS NOT NULL
    AND tavg IS NOT NULL
    AND tmin IS NOT NULL
    AND tmax IS NOT NULL;

