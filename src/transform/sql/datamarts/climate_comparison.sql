-- Datamart de Comparación Climática
-- Compara ciudades entre sí y proporciona rankings

CREATE OR REPLACE TABLE gold.climate_comparison AS
WITH city_stats AS (
    SELECT 
        city,
        region,
        station,
        lat,
        lon,
        -- Métricas principales
        AVG(temp_avg_c) as avg_temp_city,
        MAX(temp_max_c) as max_temp_city,
        MIN(temp_min_c) as min_temp_city,
        SUM(precip_mm) as total_precip_city,
        AVG(humidity_percent) as avg_humidity_city,
        AVG(wind_avg_kmh) as avg_wind_city,
        AVG(pressure_hpa) as avg_pressure_city,
        SUM(sunshine_min) as total_sunshine_city,
        
        -- Conteos
        COUNT(*) as total_days,
        COUNT(CASE WHEN temp_max_c > 30 THEN 1 END) as hot_days,
        COUNT(CASE WHEN temp_min_c < 0 THEN 1 END) as cold_days,
        COUNT(CASE WHEN precip_mm > 10 THEN 1 END) as rainy_days,
        COUNT(CASE WHEN wind_avg_kmh > 30 THEN 1 END) as windy_days,
        
        -- Estadísticas adicionales
        STDDEV(temp_avg_c) as temp_variability,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY temp_avg_c) as temp_90th_percentile,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY temp_avg_c) as temp_10th_percentile
        
    FROM silver.weather_cleaned
    WHERE 
        temp_avg_c IS NOT NULL AND
        lat IS NOT NULL AND 
        lon IS NOT NULL
    GROUP BY 
        city, region, station, lat, lon
)

SELECT 
    city,
    region,
    station,
    lat,
    lon,
    avg_temp_city,
    max_temp_city,
    min_temp_city,
    total_precip_city,
    avg_humidity_city,
    avg_wind_city,
    avg_pressure_city,
    total_sunshine_city,
    total_days,
    hot_days,
    cold_days,
    rainy_days,
    windy_days,
    temp_variability,
    temp_90th_percentile,
    temp_10th_percentile,
    
    -- Clasificación climática
    CASE 
        WHEN avg_temp_city > 20 AND total_precip_city < 500 THEN 'Clima Mediterráneo Seco'
        WHEN avg_temp_city > 18 AND total_precip_city < 800 THEN 'Clima Mediterráneo'
        WHEN avg_temp_city > 15 AND total_precip_city < 1000 THEN 'Clima Templado'
        WHEN avg_temp_city > 10 THEN 'Clima Fresco'
        ELSE 'Clima Frío'
    END as climate_classification,
    
    -- Rankings por temperatura (1 = más caliente)
    ROW_NUMBER() OVER (ORDER BY avg_temp_city DESC) as heat_rank_in_region,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY avg_temp_city DESC) as heat_rank_in_region,
    
    -- Rankings por precipitación (1 = más lluviosa)
    ROW_NUMBER() OVER (ORDER BY total_precip_city DESC) as precip_rank_overall,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_precip_city DESC) as precip_rank_in_region,
    
    -- Rankings por humedad (1 = más húmeda)
    ROW_NUMBER() OVER (ORDER BY avg_humidity_city DESC) as humidity_rank_overall,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY avg_humidity_city DESC) as humidity_rank_in_region,
    
    -- Rankings por viento (1 = más ventosa)
    ROW_NUMBER() OVER (ORDER BY avg_wind_city DESC) as wind_rank_overall,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY avg_wind_city DESC) as wind_rank_in_region,
    
    -- Rankings por sol (1 = más soleada)
    ROW_NUMBER() OVER (ORDER BY total_sunshine_city DESC) as sunshine_rank_overall,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_sunshine_city DESC) as sunshine_rank_in_region,
    
    -- Métricas calculadas
    ROUND(hot_days * 100.0 / total_days, 2) as pct_hot_days,
    ROUND(cold_days * 100.0 / total_days, 2) as pct_cold_days,
    ROUND(rainy_days * 100.0 / total_days, 2) as pct_rainy_days,
    ROUND(windy_days * 100.0 / total_days, 2) as pct_windy_days,
    
    -- Score climático (0-100, donde 100 es el clima más agradable)
    CASE 
        WHEN avg_temp_city BETWEEN 15 AND 25 AND 
             total_precip_city BETWEEN 300 AND 800 AND
             avg_humidity_city BETWEEN 40 AND 70 THEN 100
        WHEN avg_temp_city BETWEEN 10 AND 30 AND 
             total_precip_city BETWEEN 200 AND 1000 AND
             avg_humidity_city BETWEEN 30 AND 80 THEN 80
        WHEN avg_temp_city BETWEEN 5 AND 35 AND 
             total_precip_city BETWEEN 100 AND 1200 AND
             avg_humidity_city BETWEEN 20 AND 90 THEN 60
        ELSE 40
    END as climate_comfort_score

FROM city_stats
ORDER BY 
    region, avg_temp_city DESC;
