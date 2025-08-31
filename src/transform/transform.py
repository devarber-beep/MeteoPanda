from src.utils.db import get_connection
from pathlib import Path

def generate_union_all_from_bronze(con) -> str:
    # Con DLT, los datos se cargan en el esquema weather_raw
    tables = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'weather_raw'
    """).fetchall()

    if not tables:
        # Si no hay tablas en weather_raw, buscar en bronze como fallback
        tables = con.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'bronze'
        """).fetchall()
        
        if tables:
            union_sqls = [f"SELECT * FROM bronze.{row[0]}" for row in tables]
            return "\nUNION ALL\n".join(union_sqls)
        else:
            return "SELECT 1 as dummy WHERE FALSE"  # Query vacío si no hay datos
    
    # Buscar específicamente la tabla weather_data
    weather_data_tables = [row[0] for row in tables if 'weather_data' in row[0].lower()]
    if weather_data_tables:
        union_sqls = [f"SELECT * FROM weather_raw.{table}" for table in weather_data_tables]
        return "\nUNION ALL\n".join(union_sqls)
    else:
        return "SELECT 1 as dummy WHERE FALSE"  # Query vacío si no hay datos

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
