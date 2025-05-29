CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.min_precipitation AS
SELECT city, ROUND(SUM(precip_mm), 2) AS insight_precip,
strftime('%m', date) AS month
FROM silver.weather_cleaned     
WHERE month IN (6, 7, 8)
GROUP BY city, month
ORDER BY insight_precip DESC
LIMIT 5;


CREATE OR REPLACE TABLE gold.max_precipitation AS
SELECT city, ROUND(SUM(precip_mm), 2) AS insight_precip,
strftime('%m', date) AS month
FROM silver.weather_cleaned
WHERE month IN (12, 1, 2)
GROUP BY city, month
ORDER BY insight_precip ASC
LIMIT 5;
