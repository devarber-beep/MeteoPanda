from src.utils.db import get_connection
from src.utils.logging_config import get_logger, log_operation_start, log_operation_success, log_operation_error, log_database_operation
from pathlib import Path

# Configurar logger
logger = get_logger("transform")

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
    log_operation_start(logger, "transformaciones SQL", sql_directory=str(sql_dir))
    
    con = get_connection()
    if not con:
        log_operation_error(logger, "transformaciones SQL", Exception("No se pudo conectar a la base de datos"))
        return
    
    sql_files = list(sql_dir.glob("*.sql"))
    logger.info(f"Encontrados {len(sql_files)} archivos SQL para procesar")
    
    successful_transformations = 0
    failed_transformations = 0

    for sql_file in sorted(sql_files):
        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql = f.read()

                if "__UNION_ALL_BRONZE_TABLES__" in sql:
                    union_sql = generate_union_all_from_bronze(con)
                    sql = sql.replace("__UNION_ALL_BRONZE_TABLES__", f"(\n{union_sql}\n)")

                con.execute(sql)
                log_database_operation(logger, "ejecutar", sql_file.stem, sql_file=str(sql_file))
                successful_transformations += 1
                
        except Exception as e:
            log_operation_error(logger, f"transformación SQL {sql_file.name}", e, sql_file=str(sql_file))
            failed_transformations += 1
    
    log_operation_success(logger, "transformaciones SQL", 
                         successful_transformations=successful_transformations,
                         failed_transformations=failed_transformations,
                         total_files=len(sql_files))

if __name__ == "__main__":
    run_sql_transformations(Path("src/transform/sql"))
