import argparse
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configurar PYTHONPATH si no está configurado
if 'PYTHONPATH' not in os.environ:
    os.environ['PYTHONPATH'] = '.'
    print("PYTHONPATH configurado manualmente")


from src.extract.extract import extract_and_load
from src.transform.transform import run_sql_transformations

# Rutas base
CONFIG_PATH = Path("config/config.yaml")
BRONZE_PATH = Path("data/raw")
SQL_PATH = Path("src/transform/sql")

# Configurar logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("meteo_cli")

def run_download():
    try:
        logger.info("Iniciando extracción de datos meteorológicos...")
        extract_and_load(CONFIG_PATH)
        logger.info("Extracción completada correctamente.")
    except Exception as e:
        logger.exception(f"Error durante la extracción: {e}")

def run_pipeline_silver():
    try:
        logger.info("Iniciando pipeline ELT MeteoPanda → Silver...")
        run_sql_transformations(SQL_PATH)
        logger.info("✅ Pipeline Silver completo.")
    except Exception as e:
        logger.exception(f"❌ Error en el pipeline Silver: {e}")

def run_pipeline_gold():
    try:
        logger.info("Iniciando pipeline ELT MeteoPanda → Gold...")
        run_sql_transformations(SQL_PATH / "datamarts")
        logger.info("✅ Pipeline Gold completo.")
    except Exception as e:
        logger.exception(f"❌ Error en el pipeline Gold: {e}")

def clean_database():
    try:
        logger.info("Limpiando base de datos...")
        from src.utils.db import get_connection
        
        conn = get_connection()
        
        # Obtener todos los esquemas excepto los del sistema
        schemas_query = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name NOT IN ('information_schema', 'main', 'pg_catalog', 'pg_toast')
        """
        
        schemas = conn.execute(schemas_query).fetchall()
        
        for schema in schemas:
            schema_name = schema[0]
            logger.info(f"Eliminando esquema: {schema_name}")
            conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        
        conn.close()
        logger.info("✅ Base de datos limpiada correctamente.")
        
    except Exception as e:
        logger.exception(f"❌ Error limpiando la base de datos: {e}")

def run_download_and_pipelines():
    try:
        logger.info("Ejecutando descarga y pipelines...")
        
        # Descarga
        run_download()
        
        # Pipeline Silver
        run_pipeline_silver()
        
        # Pipeline Gold
        run_pipeline_gold()
        
        logger.info("✅ Descarga y pipelines completados correctamente.")
        
    except Exception as e:
        logger.exception(f"❌ Error en descarga y pipelines: {e}")

def run_full_pipeline():
    try:
        logger.info("Ejecutando pipeline completo (limpieza + descarga + pipelines)...")
        
        # Limpieza
        clean_database()
        
        # Descarga
        run_download()
        
        # Pipeline Silver
        run_pipeline_silver()
        
        # Pipeline Gold
        run_pipeline_gold()
        
        logger.info("✅ Pipeline completo ejecutado correctamente.")
        
    except Exception as e:
        logger.exception(f"❌ Error en pipeline completo: {e}")

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
