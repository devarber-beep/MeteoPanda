from datetime import date
from pathlib import Path
from src.utils.db import get_connection
from src.extract.meteo_api import load_config  # tu función YAML

def load_parquets_to_duckdb(bronze_path: Path, config_path: Path):
    con = get_connection()
    today = date.today().isoformat()
    region, cities, _, _ = load_config(config_path)
    source = "meteostat"

    
    cities_meta = {city.name.lower(): city for city in cities}


    for file in bronze_path.glob("*.parquet"):
        table_name = file.stem.lower()
        city_key = table_name.replace("_daily", "") 
        city_meta = cities_meta.get(city_key)

        if not city_meta:
            print(f"⚠️  Ciudad '{table_name}' no encontrada en config.yaml. Saltando...")
            continue

        lat = city_meta.latitude
        lon = city_meta.longitude
        alt = city_meta.elevation
        table_name = city_meta.name.lower()

        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.{table_name} AS
            SELECT 
                *,
                CAST('{date.today().isoformat()}' AS DATE) AS ingestion_date,
                '{table_name}' AS city,
                '{region}' AS region,
                '{source}' AS source,
                {lat} AS lat,
                {lon} AS lon,
                {alt} AS alt
            FROM read_parquet('{file.as_posix()}');
        """)
        print(f"✅ Cargado {file.name} a tabla bronze.{table_name}")
