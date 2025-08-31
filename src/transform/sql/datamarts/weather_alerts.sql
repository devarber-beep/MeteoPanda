-- Datamart de Alertas Meteorológicas
-- Basado en datos de silver.weather_cleaned

CREATE OR REPLACE TABLE gold.weather_alerts AS
SELECT 
    city,
    region,
    station,
    date,
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    temp_max_c as temp_max,
    temp_min_c as temp_min,
    precip_mm,
    humidity_percent,
    wind_avg_kmh,
    pressure_hpa,
    sunshine_min,
    -- Clasificación de alertas por temperatura
    CASE 
        WHEN temp_max_c > 40 THEN 'ALERTA ROJA - Temperatura Extremadamente Alta'
        WHEN temp_max_c > 35 THEN 'ALERTA ROJA - Temperatura Muy Alta'
        WHEN temp_max_c > 30 THEN 'ALERTA NARANJA - Temperatura Alta'
        WHEN temp_max_c > 25 THEN 'ALERTA AMARILLA - Temperatura Elevada'
        WHEN temp_min_c < -10 THEN 'ALERTA ROJA - Temperatura Extremadamente Baja'
        WHEN temp_min_c < -5 THEN 'ALERTA NARANJA - Temperatura Baja'
        WHEN temp_min_c < 0 THEN 'ALERTA AMARILLA - Temperatura Fría'
        ELSE 'Normal'
    END as temperature_alert,
    
    -- Clasificación de alertas por precipitación
    CASE 
        WHEN precip_mm > 100 THEN 'ALERTA ROJA - Precipitación Extremadamente Alta'
        WHEN precip_mm > 50 THEN 'ALERTA NARANJA - Precipitación Alta'
        WHEN precip_mm > 25 THEN 'ALERTA AMARILLA - Precipitación Moderada'
        ELSE 'Normal'
    END as precipitation_alert,
    
    -- Clasificación de alertas por humedad
    CASE 
        WHEN humidity_percent > 95 THEN 'ALERTA AMARILLA - Humedad Muy Alta'
        WHEN humidity_percent < 20 THEN 'ALERTA AMARILLA - Humedad Muy Baja'
        ELSE 'Normal'
    END as humidity_alert,
    
    -- Alerta general (la más severa)
    CASE 
        WHEN temp_max_c > 40 OR temp_min_c < -10 OR precip_mm > 100 THEN 'ALERTA ROJA'
        WHEN temp_max_c > 35 OR temp_min_c < -5 OR precip_mm > 50 THEN 'ALERTA NARANJA'
        WHEN temp_max_c > 30 OR temp_min_c < 0 OR precip_mm > 25 OR humidity_percent > 95 OR humidity_percent < 20 THEN 'ALERTA AMARILLA'
        ELSE 'Normal'
    END as overall_alert,
    
    -- Severidad numérica (1-5)
    CASE 
        WHEN temp_max_c > 40 OR temp_min_c < -10 OR precip_mm > 100 THEN 5
        WHEN temp_max_c > 35 OR temp_min_c < -5 OR precip_mm > 50 THEN 4
        WHEN temp_max_c > 30 OR temp_min_c < 0 OR precip_mm > 25 THEN 3
        WHEN temp_max_c > 25 OR humidity_percent > 95 OR humidity_percent < 20 THEN 2
        ELSE 1
    END as alert_severity,
    
    -- Información adicional
    source,
    ingestion_date

FROM silver.weather_cleaned
WHERE 
    -- Solo incluir días con condiciones de alerta
    temp_max_c > 25 OR 
    temp_min_c < 0 OR 
    precip_mm > 25 OR 
    humidity_percent > 95 OR 
    humidity_percent < 20
ORDER BY date DESC, alert_severity DESC;
