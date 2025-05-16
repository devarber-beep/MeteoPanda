import duckdb
from pathlib import Path
from src.utils.db import get_connection

def load_parquets_to_duckdb(bronze_path: Path):
    con = get_connection()
    parquet_files = list(bronze_path.glob("*.parquet"))

    for file in parquet_files:
        table_name = file.stem.lower()
        
        con.execute(f"""
            CREATE SCHEMA IF NOT EXISTS bronze
        """)
        con.execute(f"""
            CREATE OR REPLACE TABLE bronze.{table_name} AS
            SELECT * FROM read_parquet('{file.as_posix()}');
        """)
        print(f"Cargado {file.name} a tabla bronze.{table_name}")

if __name__ == "__main__":
    load_parquets_to_duckdb(Path("data/raw"))
