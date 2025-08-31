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

def run_pipeline():
    try:
        logger.info("Iniciando pipeline ELT MeteoPanda...")

        logger.info("Ejecutando transformaciones SQL → silver...")
        run_sql_transformations(SQL_PATH)

        logger.info("Pipeline completo.")

    except Exception as e:
        logger.exception(f"Error en el pipeline: {e}")

def run_pipeline_gold():
    try:
        logger.info("Iniciando pipeline ELT MeteoPanda → Gold...")
        run_sql_transformations(SQL_PATH / "datamarts")
        logger.info("✅ Pipeline Gold completo.")
    except Exception as e:
        logger.exception(f"❌ Error en el pipeline Gold: {e}")

def main():
    parser = argparse.ArgumentParser(description="MeteoPanda CLI")
    parser.add_argument("--download", action="store_true", help="Descargar datos desde la API")
    parser.add_argument("--run-pipeline", action="store_true", help="Ejecutar ETL a Silver")
    parser.add_argument("--pipeline-gold", action="store_true", help="Transformar a capa Gold")
    

    args = parser.parse_args()

    if args.download:
        run_download()

    if args.run_pipeline:
        run_pipeline()

    if args.pipeline_gold:
        run_pipeline_gold()

    if not args.download and not args.run_pipeline and not args.pipeline_gold:
        parser.print_help()

if __name__ == "__main__":
    main()
