from src.utils.db import get_connection
from pathlib import Path

def generate_union_all_from_bronze(con) -> str:
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze'
    """).fetchall()

    union_sqls = [f"SELECT * FROM bronze.{row[0]}" for row in tables]
    return "\nUNION ALL\n".join(union_sqls)

def run_sql_transformations(sql_dir: Path):
    con = get_connection()

    for sql_file in sorted(sql_dir.glob("*.sql")):
        with open(sql_file, "r", encoding="utf-8") as f:
            sql = f.read()

            if "__UNION_ALL_BRONZE_TABLES__" in sql:
                union_sql = generate_union_all_from_bronze(con)
                sql = sql.replace("__UNION_ALL_BRONZE_TABLES__", f"(\n{union_sql}\n)")

            con.execute(sql)
            print(f"Ejecutado: {sql_file.name}")

if __name__ == "__main__":
    run_sql_transformations(Path("src/transform/sql"))
