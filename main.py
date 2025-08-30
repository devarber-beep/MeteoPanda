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


from src.extract.meteo_api import load_config, get_station_id, fetch_daily_data, save_to_parquet
from src.load.load_parquet_to_duckdb import load_parquets_to_duckdb
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
<<<<<<< HEAD
        cities, start_date, end_date = load_config(CONFIG_PATH)

        for city in cities:
            logger.info(f"📍 Procesando ciudad: {city.name}")
            station_id = get_station_id(city.latitude, city.longitude)
            if not station_id:
                logger.warning(f"No se encontró estación para {city.name}")
                continue
            df = fetch_daily_data(station_id, start_date, end_date)
            save_to_parquet(df, city.name)

        logger.info("✅ Descarga completada correctamente.")

    except Exception as e:
        logger.exception(f"❌ Error durante la descarga: {e}")
=======
        logger.info("Iniciando extracción de datos meteorológicos...")
        extract_and_load(CONFIG_PATH)
        logger.info("Extracción completada correctamente.")
    except Exception as e:
        logger.exception(f"Error durante la extracción: {e}")
>>>>>>> 0c17efa (Solucion definitiva al modulo de descarga masiva)

def run_pipeline():
    try:
        logger.info("Iniciando pipeline ELT MeteoPanda...")

        logger.info("Cargando Parquet → DuckDB (bronze)...")
        load_parquets_to_duckdb(BRONZE_PATH, CONFIG_PATH)

        logger.info("Ejecutando transformaciones SQL → silver...")
        run_sql_transformations(SQL_PATH)

        logger.info("Pipeline completo.")

    except Exception as e:
        logger.exception(f"Error en el pipeline: {e}")

def run_pipeline_gold():
    try:
        logger.info("Iniciando pipeline ELT MeteoPanda → Gold...")
        run_sql_transformations(SQL_PATH / "datamarts")
        logger.info("✅ Pipeline completo.")
    except Exception as e:
        logger.exception(f"❌ Error en el pipeline: {e}")

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
