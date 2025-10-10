-- Datamart de similitud entre ciudades basado en métricas climáticas agregadas
-- Fuente: gold.climate_comparison

CREATE SCHEMA IF NOT EXISTS gold;

-- Normalizar métricas clave por z-score y calcular similitud por distancia euclídea
CREATE OR REPLACE TABLE gold.city_similarity AS
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
), stats AS (
    SELECT
        *,
        CASE WHEN STDDEV_SAMP(avg_temp_city) OVER () > 0
             THEN (avg_temp_city - AVG(avg_temp_city) OVER ()) / STDDEV_SAMP(avg_temp_city) OVER ()
             ELSE 0 END AS z_temp,
        CASE WHEN STDDEV_SAMP(total_precip_city) OVER () > 0
             THEN (total_precip_city - AVG(total_precip_city) OVER ()) / STDDEV_SAMP(total_precip_city) OVER ()
             ELSE 0 END AS z_precip,
        CASE WHEN STDDEV_SAMP(avg_humidity_city) OVER () > 0
             THEN (avg_humidity_city - AVG(avg_humidity_city) OVER ()) / STDDEV_SAMP(avg_humidity_city) OVER ()
             ELSE 0 END AS z_humidity,
        CASE WHEN STDDEV_SAMP(total_sunshine_city) OVER () > 0
             THEN (total_sunshine_city - AVG(total_sunshine_city) OVER ()) / STDDEV_SAMP(total_sunshine_city) OVER ()
             ELSE 0 END AS z_sun,
        CASE WHEN STDDEV_SAMP(avg_wind_city) OVER () > 0
             THEN (avg_wind_city - AVG(avg_wind_city) OVER ()) / STDDEV_SAMP(avg_wind_city) OVER ()
             ELSE 0 END AS z_wind
    FROM base
), pairs AS (
    SELECT
        a.city AS city_a,
        a.region AS region_a,
        b.city AS city_b,
        b.region AS region_b,
        /* Distancia euclídea en el espacio z-score */
        SQRT(
            POWER(COALESCE(a.z_temp,0) - COALESCE(b.z_temp,0), 2) +
            POWER(COALESCE(a.z_precip,0) - COALESCE(b.z_precip,0), 2) +
            POWER(COALESCE(a.z_humidity,0) - COALESCE(b.z_humidity,0), 2) +
            POWER(COALESCE(a.z_sun,0) - COALESCE(b.z_sun,0), 2) +
            POWER(COALESCE(a.z_wind,0) - COALESCE(b.z_wind,0), 2)
        ) AS distance,
        /* Similitud simple como inverso de distancia (+epsilon para evitar div/0) */
        1.0 / (1e-6 + SQRT(
                POWER(COALESCE(a.z_temp,0) - COALESCE(b.z_temp,0), 2) +
                POWER(COALESCE(a.z_precip,0) - COALESCE(b.z_precip,0), 2) +
                POWER(COALESCE(a.z_humidity,0) - COALESCE(b.z_humidity,0), 2) +
                POWER(COALESCE(a.z_sun,0) - COALESCE(b.z_sun,0), 2) +
                POWER(COALESCE(a.z_wind,0) - COALESCE(b.z_wind,0), 2)
        )) AS similarity
    FROM stats a
    JOIN stats b
      ON a.city <> b.city
)
-- Para cada ciudad A nos quedamos con las K ciudades más similares (K=5)
, ranked AS (
    SELECT
        city_a,
        region_a,
        city_b,
        region_b,
        distance,
        similarity,
        ROW_NUMBER() OVER (PARTITION BY city_a ORDER BY distance ASC) AS rn
    FROM pairs
)
SELECT
    city_a AS city,
    region_a AS region,
    city_b AS similar_city,
    region_b AS similar_region,
    ROUND(distance, 4) AS similarity_distance,
    ROUND(similarity, 6) AS similarity_score,
    rn AS rank
FROM ranked
WHERE rn <= 5
ORDER BY city, rn;


