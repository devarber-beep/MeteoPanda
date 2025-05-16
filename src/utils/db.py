import duckdb
from pathlib import Path

DB_PATH = Path("data/meteopanda.duckdb")

def get_connection():
    return duckdb.connect(database=DB_PATH, read_only=False)
