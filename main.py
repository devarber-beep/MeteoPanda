import argparse
import os
import time
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar PYTHONPATH si no está configurado
if 'PYTHONPATH' not in os.environ:
    os.environ['PYTHONPATH'] = '.'

# Configurar logging antes de importar otros módulos
from src.utils.logging_config import setup_logging, get_logger, log_operation_start, log_operation_success, log_operation_error

# Configurar logging
setup_logging(level="INFO", log_file="logs/meteopanda.log", console_output=True, structured=True)
logger = get_logger("meteo_cli")

from src.extract.extract import extract_and_load
from src.transform.transform import run_sql_transformations

# Rutas base
CONFIG_PATH = Path("config/config.yaml")
BRONZE_PATH = Path("data/raw")
SQL_PATH = Path("src/transform/sql")

def run_download():
    start_time = time.time()
    log_operation_start(logger, "extracción de datos meteorológicos", config_path=str(CONFIG_PATH))
    
    try:
        extract_and_load(CONFIG_PATH)
        duration = time.time() - start_time
        log_operation_success(logger, "extracción de datos meteorológicos", duration=duration)
    except Exception as e:
        log_operation_error(logger, "extracción de datos meteorológicos", e)

def run_pipeline_silver():
    start_time = time.time()
    log_operation_start(logger, "pipeline ELT Silver", sql_path=str(SQL_PATH))
    
    try:
        run_sql_transformations(SQL_PATH)
        duration = time.time() - start_time
        log_operation_success(logger, "pipeline ELT Silver", duration=duration)
    except Exception as e:
        log_operation_error(logger, "pipeline ELT Silver", e)

def run_pipeline_gold():
    start_time = time.time()
    log_operation_start(logger, "pipeline ELT Gold", sql_path=str(SQL_PATH / "datamarts"))
    
    try:
        run_sql_transformations(SQL_PATH / "datamarts")
        duration = time.time() - start_time
        log_operation_success(logger, "pipeline ELT Gold", duration=duration)
    except Exception as e:
        log_operation_error(logger, "pipeline ELT Gold", e)

def clean_database():
    start_time = time.time()
    log_operation_start(logger, "limpieza de base de datos")
    
    try:
        from src.utils.db import get_connection
        
        conn = get_connection()
        
        # Obtener todos los esquemas excepto los del sistema
        schemas_query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'main', 'pg_catalog', 'pg_toast')
        """
        
        schemas = conn.execute(schemas_query).fetchall()
        logger.info(f"Encontrados {len(schemas)} esquemas para eliminar")
        
        for schema in schemas:
            schema_name = schema[0]
            logger.info(f"Eliminando esquema: {schema_name}")
            conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        
        conn.close()
        duration = time.time() - start_time
        log_operation_success(logger, "limpieza de base de datos", duration=duration, schemas_removed=len(schemas))
        
    except Exception as e:
        log_operation_error(logger, "limpieza de base de datos", e)

def run_download_and_pipelines():
    start_time = time.time()
    log_operation_start(logger, "descarga y pipelines completos")
    
    try:
        # Descarga
        run_download()
        
        # Pipeline Silver
        run_pipeline_silver()
        
        # Pipeline Gold
        run_pipeline_gold()
        
        duration = time.time() - start_time
        log_operation_success(logger, "descarga y pipelines completos", duration=duration)
        
    except Exception as e:
        log_operation_error(logger, "descarga y pipelines completos", e)

def run_full_pipeline():
    start_time = time.time()
    log_operation_start(logger, "pipeline completo (limpieza + descarga + pipelines)")
    
    try:
        # Limpieza
        clean_database()
        
        # Descarga
        run_download()
        
        # Pipeline Silver
        run_pipeline_silver()
        
        # Pipeline Gold
        run_pipeline_gold()
        
        duration = time.time() - start_time
        log_operation_success(logger, "pipeline completo", duration=duration)
        
    except Exception as e:
        log_operation_error(logger, "pipeline completo", e)

def main():
    parser = argparse.ArgumentParser(description="MeteoPanda CLI")
    parser.add_argument("--download", action="store_true", help="Descargar datos desde la API")
    parser.add_argument("--pipeline-silver", action="store_true", help="Ejecutar ETL a Silver")
    parser.add_argument("--pipeline-gold", action="store_true", help="Transformar a capa Gold")
    parser.add_argument("--clean", action="store_true", help="Limpiar base de datos")
    parser.add_argument("--download-and-pipelines", action="store_true", help="Ejecutar descarga y pipelines")
    parser.add_argument("--full-pipeline", action="store_true", help="Ejecutar pipeline completo (limpieza + descarga + pipelines)")

    args = parser.parse_args()

    if args.clean:
        clean_database()

    if args.download:
        run_download()

    if args.pipeline_silver:
        run_pipeline_silver()

    if args.pipeline_gold:
        run_pipeline_gold()

    if args.download_and_pipelines:
        run_download_and_pipelines()

    if args.full_pipeline:
        run_full_pipeline()

    if not any([args.clean, args.download, args.pipeline_silver, args.pipeline_gold, 
                args.download_and_pipelines, args.full_pipeline]):
        parser.print_help()

if __name__ == "__main__":
    main()
