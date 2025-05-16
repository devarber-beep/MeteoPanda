from pathlib import Path
from src.load.load_parquet_to_duckdb import load_parquets_to_duckdb
from src.transform.transform_to_silver import run_sql_transformations

def run_pipeline():
    print("Iniciando pipeline ETL MeteoPanda...\n")

    bronze_path = Path("data/raw")
    sql_transform_path = Path("src/transform/sql")

    # 1. Cargar archivos Parquet en DuckDB (bronze)
    print("Cargando Parquet → DuckDB (bronze)...")
    load_parquets_to_duckdb(bronze_path)

    # 2. Ejecutar transformaciones SQL (silver)
    print("Ejecutando transformaciones SQL → silver...")
    run_sql_transformations(sql_transform_path)

    print("\n✅ Pipeline completo.")

if __name__ == "__main__":
    run_pipeline()
