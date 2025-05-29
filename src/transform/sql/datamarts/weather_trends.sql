CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.weather_trends AS
SELECT 
  city,
  year,
  ROUND(AVG(temp_avg_c), 2) AS avg_temp,
  SUM(precip_mm) AS total_precip
FROM silver.weather_cleaned
GROUP BY city, year
