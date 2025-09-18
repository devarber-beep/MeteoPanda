import duckdb
from pathlib import Path


def main(db_path: str = 'meteopanda.duckdb') -> None:
    if not Path(db_path).exists():
        raise SystemExit(f"Database file not found: {db_path}")

    con = duckdb.connect(db_path)

    print("-- silver.weather_cleaned: schema --")
    schema_df = con.execute("PRAGMA table_info('silver.weather_cleaned')").df()
    print(schema_df)

    print("\n-- silver.weather_cleaned: 5-row sample (key columns) --")
    sample_df = con.execute(
        """
        SELECT date, city, region, station, lat, lon,
               temp_avg_c, temp_min_c, temp_max_c,
               precip_mm, sunshine_min, humidity_percent,
               source
        FROM silver.weather_cleaned
        LIMIT 5
        """
    ).df()
    print(sample_df)

    con.close()


if __name__ == "__main__":
    main()


