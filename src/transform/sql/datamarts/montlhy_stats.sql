CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.monthly_stats AS
SELECT 
  city, 
  year, 
  strftime('%m', date) AS month,
  ROUND(AVG(temp_avg_c), 2) AS avg_temp,
  SUM(precip_mm) AS total_precip,
  AVG(sunshine_min) AS avg_sunshine,
  AVG(snow_depth_cm) AS avg_snow_depth
FROM silver.weather_cleaned
GROUP BY city, year, month
