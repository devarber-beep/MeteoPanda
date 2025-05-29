CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.climate_profiles AS 
SELECT 
  city, 
  ROUND(AVG(temp_avg_c), 1) AS avg_temp,
  MAX(temp_max_c) AS record_high,
  MIN(temp_min_c) AS record_low,
  SUM(precip_mm) AS yearly_precip,
  AVG(sunshine_min) AS avg_sunshine
FROM silver.weather_cleaned
GROUP BY city
