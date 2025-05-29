CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.city_yearly_summary AS
SELECT
    city,
    region,
    year,
    strftime('%m', date) AS month,
    COUNT(*) AS days,
    ROUND(AVG(temp_avg_c), 2) AS avg_temp,
    ROUND(MAX(temp_max_c), 2) AS max_temp,
    ROUND(MIN(temp_min_c), 2) AS min_temp,
    ROUND(SUM(precip_mm), 2) AS total_precip,
    ROUND(SUM(sunshine_min), 2) AS total_sunshine
FROM silver.weather_cleaned
GROUP BY city, region, year, month;

CREATE OR REPLACE TABLE gold.city_extreme_days AS
SELECT *, strftime('%m', date) AS month
FROM silver.weather_cleaned
WHERE
    temp_max_c > 40 OR temp_min_c < 0 OR precip_mm > 50