from src.utils.db import get_connection
from pathlib import Path


def run_sql_transformations(sql_dir: Path):
    con = get_connection()

    for sql_file in sorted(sql_dir.glob("*.sql")):
        with open(sql_file, "r", encoding="utf-8") as f:
            sql = f.read()

            con.execute(sql)
            print(f"Ejecutado: {sql_file.name}")

if __name__ == "__main__":
    run_sql_transformations(Path("src/transform/sql/datamarts"))
