CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.climate_profiles AS 
SELECT 
  city, 
  ROUND(AVG(temp_avg_c), 1) AS avg_temp,
  MAX(temp_max_c) AS record_high,
  MIN(temp_min_c) AS record_low,
  SUM(precip_mm) AS yearly_precip,
  AVG(sunshine_min) AS avg_sunshine,
  ROUND(AVG(humidity_percent), 1) AS avg_humidity
FROM silver.weather_cleaned
GROUP BY city;


CREATE OR REPLACE TABLE gold.city_extreme_days AS
SELECT
    city,
    region,
    station,
    year,
    strftime('%m', date) AS month,
    ROUND(MAX(temp_max_c), 2) AS max_temp_month,
    ROUND(MIN(temp_min_c), 2) AS min_temp_month,
    ROUND(SUM(precip_mm), 2) AS total_precip_month
FROM silver.weather_cleaned
GROUP BY city, region, station, year, month;