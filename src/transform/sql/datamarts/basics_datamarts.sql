CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.city_yearly_summary AS
SELECT
    city,
    region,
    station,
    year,
    CAST(strftime('%m', date) AS INT) AS month,
    COUNT(*) AS days,
    ROUND(AVG(temp_avg_c), 2) AS avg_temp,
    ROUND(MAX(temp_max_c), 2) AS max_temp,
    ROUND(MIN(temp_min_c), 2) AS min_temp,
    ROUND(SUM(precip_mm), 2) AS total_precip,
    ROUND(SUM(sunshine_min), 2) AS total_sunshine,
    ROUND(AVG(humidity_percent), 2) AS avg_humidity,
    ROUND(AVG(lat), 6) AS lat,
    ROUND(AVG(lon), 6) AS lon
FROM silver.weather_cleaned
GROUP BY city, region, station, year, month;