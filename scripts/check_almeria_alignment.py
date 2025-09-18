import duckdb
from pathlib import Path


def run_checks(db_path: str = 'meteopanda.duckdb') -> None:
    con = duckdb.connect(db_path)

    print("\n-- Check 1: Presence in silver.weather_cleaned (any 'almer%') --")
    q1 = """
    SELECT DISTINCT city
    FROM silver.weather_cleaned
    WHERE lower(city) LIKE '%almer%'
    ORDER BY 1;
    """
    print(con.execute(q1).df())

    print("\n-- Check 2: silver stats for Almer% (coords ranges) --")
    q2 = """
    SELECT
      city,
      COUNT(*) AS rows,
      MIN(lat) AS lat_min, MAX(lat) AS lat_max,
      MIN(lon) AS lon_min, MAX(lon) AS lon_max,
      SUM(CASE WHEN lat IS NULL OR lon IS NULL THEN 1 ELSE 0 END) AS null_coords
    FROM silver.weather_cleaned
    WHERE lower(city) LIKE '%almer%'
    GROUP BY city
    ORDER BY city;
    """
    print(con.execute(q2).df())

    print("\n-- Check 3: Presence in gold.city_yearly_summary (any 'almer%') --")
    q3 = """
    SELECT DISTINCT city
    FROM gold.city_yearly_summary
    WHERE lower(city) LIKE '%almer%'
    ORDER BY 1;
    """
    print(con.execute(q3).df())

    print("\n-- Check 4: gold stats for Almer% (coords ranges) --")
    q4 = """
    SELECT
      city,
      COUNT(*) AS rows,
      MIN(lat) AS lat_min, MAX(lat) AS lat_max,
      MIN(lon) AS lon_min, MAX(lon) AS lon_max
    FROM gold.city_yearly_summary
    WHERE lower(city) LIKE '%almer%'
    GROUP BY city
    ORDER BY city;
    """
    print(con.execute(q4).df())

    print("\n-- Check 5: Map coords (distinct city-lat-lon in silver) --")
    q5 = """
    SELECT DISTINCT city, lat, lon
    FROM silver.weather_cleaned
    WHERE lat IS NOT NULL AND lon IS NOT NULL AND lower(city) LIKE '%almer%'
    ORDER BY city;
    """
    print(con.execute(q5).df())

    print("\n-- Check 6: Alignment summary vs coords (case-insensitive) --")
    q6 = """
    WITH coords AS (
      SELECT DISTINCT city, lat, lon
      FROM silver.weather_cleaned
      WHERE lat IS NOT NULL AND lon IS NOT NULL
    ), summary AS (
      SELECT DISTINCT city
      FROM gold.city_yearly_summary
    )
    SELECT s.city AS city_in_summary, c.city AS city_in_coords
    FROM summary s
    LEFT JOIN coords c
      ON lower(s.city) = lower(c.city)
    WHERE lower(s.city) LIKE '%almer%';
    """
    print(con.execute(q6).df())

    print("\n-- Check 7: Month typing and sample values in gold --")
    q7 = """
    SELECT typeof(month) AS month_type, COUNT(*) AS cnt
    FROM gold.city_yearly_summary
    GROUP BY 1
    ORDER BY 2 DESC;
    """
    print(con.execute(q7).df())

    print("\n-- Check 8: Sample rows for Almer% (month, lat, lon) --")
    q8 = """
    SELECT city, year, month, lat, lon
    FROM gold.city_yearly_summary
    WHERE lower(city) LIKE '%almer%'
    ORDER BY year, month
    LIMIT 20;
    """
    print(con.execute(q8).df())

    con.close()


if __name__ == '__main__':
    db = 'meteopanda.duckdb'
    if not Path(db).exists():
        raise SystemExit(f"Database file not found: {db}")
    run_checks(db)


