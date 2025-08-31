-- Datamart de Análisis Estacional
-- Basado en datos de silver.weather_cleaned

CREATE OR REPLACE TABLE gold.seasonal_analysis AS
SELECT 
    city,
    region,
    station,
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    -- Clasificación de estaciones
    CASE 
        WHEN month IN (12, 1, 2) THEN 'Invierno'
        WHEN month IN (3, 4, 5) THEN 'Primavera'
        WHEN month IN (6, 7, 8) THEN 'Verano'
        WHEN month IN (9, 10, 11) THEN 'Otoño'
    END as season,
    
    -- Métricas por estación
    AVG(temp_avg_c) as avg_temp_season,
    MAX(temp_max_c) as max_temp_season,
    MIN(temp_min_c) as min_temp_season,
    SUM(precip_mm) as total_precip_season,
    AVG(humidity_percent) as avg_humidity_season,
    AVG(wind_avg_kmh) as avg_wind_season,
    AVG(pressure_hpa) as avg_pressure_season,
    SUM(sunshine_min) as total_sunshine_season,
    AVG(snow_depth_cm) as avg_snow_season,
    
    -- Conteos
    COUNT(*) as total_days,
    COUNT(CASE WHEN temp_max_c > 30 THEN 1 END) as hot_days,
    COUNT(CASE WHEN temp_min_c < 0 THEN 1 END) as cold_days,
    COUNT(CASE WHEN precip_mm > 10 THEN 1 END) as rainy_days,
    COUNT(CASE WHEN wind_avg_kmh > 30 THEN 1 END) as windy_days,
    COUNT(CASE WHEN humidity_percent > 80 THEN 1 END) as humid_days,
    
    -- Estadísticas adicionales
    STDDEV(temp_avg_c) as temp_variability,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY temp_avg_c) as temp_75th_percentile,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY temp_avg_c) as temp_25th_percentile

FROM silver.weather_cleaned
    WHERE 
        temp_avg_c IS NOT NULL AND
        date IS NOT NULL
GROUP BY 
    city, region, station, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
ORDER BY 
    city, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date);
