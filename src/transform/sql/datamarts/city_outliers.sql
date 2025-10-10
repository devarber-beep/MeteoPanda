-- Datamart de outliers climáticos por ciudad
-- Identifica ciudades con valores extremos en variables clave (z-score absoluto)

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.city_outliers AS
WITH base AS (
    SELECT
        city,
        region,
        lat,
        lon,
        avg_temp_city,
        total_precip_city,
        avg_humidity_city,
        total_sunshine_city,
        avg_wind_city
    FROM gold.climate_comparison
), zs AS (
    SELECT
        *,
        (avg_temp_city - AVG(avg_temp_city) OVER ()) / NULLIF(STDDEV_SAMP(avg_temp_city) OVER (), 0) AS z_temp,
        (total_precip_city - AVG(total_precip_city) OVER ()) / NULLIF(STDDEV_SAMP(total_precip_city) OVER (), 0) AS z_precip,
        (avg_humidity_city - AVG(avg_humidity_city) OVER ()) / NULLIF(STDDEV_SAMP(avg_humidity_city) OVER (), 0) AS z_humidity,
        (total_sunshine_city - AVG(total_sunshine_city) OVER ()) / NULLIF(STDDEV_SAMP(total_sunshine_city) OVER (), 0) AS z_sun,
        (avg_wind_city - AVG(avg_wind_city) OVER ()) / NULLIF(STDDEV_SAMP(avg_wind_city) OVER (), 0) AS z_wind
    FROM base
), agg AS (
    SELECT
        city,
        region,
        lat,
        lon,
        z_temp,
        z_precip,
        z_humidity,
        z_sun,
        z_wind,
        -- Puntaje de outlier como máximo |z| entre métricas
        GREATEST(
            ABS(z_temp),
            ABS(z_precip),
            ABS(z_humidity),
            ABS(z_sun),
            ABS(z_wind)
        ) AS outlier_score
    FROM zs
)
SELECT
    city,
    region,
    lat,
    lon,
    ROUND(outlier_score, 4) AS outlier_score,
    CASE
        WHEN outlier_score >= 3 THEN 'Muy extremo'
        WHEN outlier_score >= 2 THEN 'Extremo'
        WHEN outlier_score >= 1.5 THEN 'Moderado'
        ELSE 'Normal'
    END AS outlier_level,
    -- Señalar qué variable domina el outlier (mayor |z|)
    CASE
        WHEN ABS(z_temp) >= ABS(z_precip) AND ABS(z_temp) >= ABS(z_humidity) AND ABS(z_temp) >= ABS(z_sun) AND ABS(z_temp) >= ABS(z_wind) THEN 'Temperatura'
        WHEN ABS(z_precip) >= ABS(z_temp) AND ABS(z_precip) >= ABS(z_humidity) AND ABS(z_precip) >= ABS(z_sun) AND ABS(z_precip) >= ABS(z_wind) THEN 'Precipitación'
        WHEN ABS(z_humidity) >= ABS(z_temp) AND ABS(z_humidity) >= ABS(z_precip) AND ABS(z_humidity) >= ABS(z_sun) AND ABS(z_humidity) >= ABS(z_wind) THEN 'Humedad'
        WHEN ABS(z_sun) >= ABS(z_temp) AND ABS(z_sun) >= ABS(z_precip) AND ABS(z_sun) >= ABS(z_humidity) AND ABS(z_sun) >= ABS(z_wind) THEN 'Sol'
        ELSE 'Viento'
    END AS dominant_variable
FROM agg
ORDER BY outlier_score DESC;


