import os
from typing import Dict, List
import pandas as pd
from pathlib import Path
import dlt
import duckdb
from datetime import datetime
from dotenv import load_dotenv

from .dto import CityConfigDTO
from .meteo_api import fetch_daily_data as fetch_meteostat_data, get_station_id as get_meteostat_station_id
from .aemet_api import fetch_daily_data as fetch_aemet_data, get_station_id as get_aemet_station_id
from ..utils.logging_config import get_logger, log_operation_start, log_operation_success, log_operation_error, log_data_loaded, log_api_request, log_validation_warning

# Rutas base
CONFIG_PATH = Path("config/config.yaml")
BRONZE_PATH = Path("data/raw")
SQL_PATH = Path("src/transform/sql")


# Cargar variables de entorno
load_dotenv()

# Configurar logger
logger = get_logger("extract")

# Configurar pipeline dlt
pipeline = dlt.pipeline(
    pipeline_name="meteopanda",
    destination="duckdb",
    dataset_name="weather_raw",
    dev_mode=True
)

def load_city_config(config_path: str) -> tuple[List[CityConfigDTO], str, str, str]:
    """
    Carga la configuración de ciudades desde un archivo YAML.
    """
    import yaml
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    region = cities[0].region if cities else "Unknown"
    return cities, cfg["start_date"], cfg["end_date"], region

def extract_meteostat_data(city: CityConfigDTO, start_date: str, end_date: str, region: str) -> pd.DataFrame:
    """
    Extrae datos de Meteostat para una ciudad.
    """
    try:
        station_id = get_meteostat_station_id(city.latitude, city.longitude)
        if station_id:
            logger.info(f"Extrayendo datos Meteostat para {city.name} (estación: {station_id})")
            df = fetch_meteostat_data(station_id, start_date, end_date)
            df["source"] = "meteostat"
            df["city"] = city.name
            df["region"] = region
            df["station"] = station_id
            
            if not df.empty:
                log_data_loaded(logger, "Meteostat", len(df), city=city.name, station=station_id, date_range=f"{start_date} to {end_date}")
            else:
                log_validation_warning(logger, "Meteostat", f"No se obtuvieron datos para {city.name}", city=city.name, station=station_id)
            
            return df
        else:
            log_validation_warning(logger, "Meteostat", f"No se encontró estación para {city.name}", city=city.name, coordinates=f"{city.latitude}, {city.longitude}")
    except Exception as e:
        log_operation_error(logger, f"extracción Meteostat para {city.name}", e, city=city.name, source="meteostat")
    return pd.DataFrame()

def extract_aemet_data(city: CityConfigDTO, start_date: str, end_date: str, region: str) -> pd.DataFrame:
    """
    Extrae datos de AEMET para una ciudad.
    """
    try:
        station_id = get_aemet_station_id(city.latitude, city.longitude)
        if station_id:
            logger.info(f"Extrayendo datos AEMET para {city.name} (estación: {station_id})")
            df = fetch_aemet_data(station_id, start_date, end_date)
            df["source"] = "aemet"
            df["city"] = city.name
            df["region"] = region
            df["station"] = station_id
            
            if not df.empty:
                log_data_loaded(logger, "AEMET", len(df), city=city.name, station=station_id, date_range=f"{start_date} to {end_date}")
            else:
                log_validation_warning(logger, "AEMET", f"No se obtuvieron datos para {city.name}", city=city.name, station=station_id)
            
            return df
        else:
            log_validation_warning(logger, "AEMET", f"No se encontró estación para {city.name}", city=city.name, coordinates=f"{city.latitude}, {city.longitude}")
    except Exception as e:
        log_operation_error(logger, f"extracción AEMET para {city.name}", e, city=city.name, source="aemet")
    return pd.DataFrame()


def create_weather_raw_schema():
    """
    Crea el esquema weather_raw y copia los datos del esquema con timestamp más reciente.
    """
    try:
        # Conectar a DuckDB
        con = duckdb.connect('meteopanda.duckdb')
        
        # Buscar el esquema más reciente con timestamp (excluyendo _staging)
        schemas = con.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name LIKE 'weather_raw_%' 
            AND schema_name NOT LIKE '%_staging'
            ORDER BY schema_name DESC
        """).fetchall()
        
        if not schemas:
            logger.warning("No se encontraron esquemas DLT con datos")
            con.close()
            return
        
        latest_schema = schemas[0][0]
        logger.info(f"Copiando datos desde {latest_schema} a weather_raw")
        
        # Crear esquema weather_raw si no existe
        con.execute("CREATE SCHEMA IF NOT EXISTS weather_raw")
        
        # Verificar si la tabla weather_data existe en el esquema más reciente
        tables = con.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{latest_schema}' AND table_name = 'weather_data'
        """).fetchall()
        
        if tables:
            # Leer coordenadas del config.yaml
            import yaml
            with open('config/config.yaml', 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            
            # Crear diccionario de coordenadas por ciudad
            city_coords = {}
            for city in config['cities']:
                city_coords[city['name']] = {
                    'lat': city['latitude'],
                    'lon': city['longitude']
                }
            
            # Generar dinámicamente las cláusulas CASE para lat y lon
            lat_cases = []
            lon_cases = []
            for city_name, coords in city_coords.items():
                lat_cases.append(f"WHEN city = '{city_name}' THEN {coords['lat']}")
                lon_cases.append(f"WHEN city = '{city_name}' THEN {coords['lon']}")
            
            lat_case_sql = "CASE " + " ".join(lat_cases) + " ELSE NULL END"
            lon_case_sql = "CASE " + " ".join(lon_cases) + " ELSE NULL END"
                        
            # Construir lista de columnas de la tabla origen excluyendo lat/lon para evitar colisiones
            cols_df = con.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{latest_schema}'
                  AND table_name = 'weather_data'
                ORDER BY ordinal_position
            """).fetchdf()

            source_columns = [row[0] for _, row in cols_df.iterrows()] if not cols_df.empty else []
            non_coord_columns = [c for c in source_columns if c.lower() not in ('lat', 'lon')]
            select_non_coords = ",\n                    ".join(non_coord_columns) if non_coord_columns else "*"

            # Asignar coordenadas directamente desde config.yaml (las APIs diarias no las traen)
            select_sql = f"""
                CREATE TABLE IF NOT EXISTS weather_raw.weather_data AS 
                SELECT 
                    {select_non_coords},
                    {lat_case_sql} AS lat,
                    {lon_case_sql} AS lon
                FROM {latest_schema}.weather_data
            """

            con.execute(select_sql)
            
            # Verificar que se copiaron los datos
            count = con.execute("SELECT COUNT(*) FROM weather_raw.weather_data").fetchone()[0]
            log_data_loaded(logger, "weather_raw.weather_data", count, schema=latest_schema)
            
            # Verificar coordenadas
            coords_count = con.execute("""
                SELECT COUNT(*) 
                FROM weather_raw.weather_data 
                WHERE lat IS NOT NULL AND lon IS NOT NULL
            """).fetchone()[0]
            
            if coords_count < count:
                log_validation_warning(logger, "coordenadas", f"Solo {coords_count}/{count} registros tienen coordenadas", total_records=count, records_with_coords=coords_count)
                        
        else:
            logger.error(f"No se encontró la tabla weather_data en {latest_schema}")
        
        con.close()
        
    except Exception as e:
        log_operation_error(logger, "creación de esquema weather_raw", e)
        if 'con' in locals():
            con.close()

def extract_and_load(config_path: str):
    """
    Función principal que extrae datos de todas las fuentes y los carga en dlt.
    """
    log_operation_start(logger, "extracción y carga de datos", config_path=config_path)
    
    # Cargar configuración
    cities, start_date, end_date, _ = load_city_config(config_path)
    logger.info(f"Configuración cargada: {len(cities)} ciudades, período {start_date} a {end_date}")
    
    all_data = []
    successful_extractions = 0
    failed_extractions = 0
    
    for city in cities:
        logger.info(f"Procesando ciudad: {city.name}")
        
        # Usar la región específica de cada ciudad
        city_region = city.region
        
        # Extraer datos de Meteostat
        meteostat_df = extract_meteostat_data(city, start_date, end_date, city_region)
        if not meteostat_df.empty:
            all_data.append(meteostat_df)
            successful_extractions += 1
        else:
            failed_extractions += 1
        
        # Extraer datos de AEMET
        aemet_df = extract_aemet_data(city, start_date, end_date, city_region)
        if not aemet_df.empty:
            all_data.append(aemet_df)
            successful_extractions += 1
        else:
            failed_extractions += 1
        
    
    if all_data:
        # Combinar todos los datos
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Convertir tipos solo para las columnas que existen
        type_mapping = {}
        
        # Columnas que siempre deberían existir
        if 'date' in combined_df.columns:
            type_mapping['date'] = 'datetime64[ns]'
        if 'tavg' in combined_df.columns:
            type_mapping['tavg'] = 'float64'
        if 'tmin' in combined_df.columns:
            type_mapping['tmin'] = 'float64'
        if 'tmax' in combined_df.columns:
            type_mapping['tmax'] = 'float64'
        if 'prcp' in combined_df.columns:
            type_mapping['prcp'] = 'float64'
        if 'wdir' in combined_df.columns:
            type_mapping['wdir'] = 'float64'
        if 'wspd' in combined_df.columns:
            type_mapping['wspd'] = 'float64'
        if 'wpgt' in combined_df.columns:
            type_mapping['wpgt'] = 'float64'
        if 'pres' in combined_df.columns:
            type_mapping['pres'] = 'float64'
        if 'snow' in combined_df.columns:
            type_mapping['snow'] = 'float64'
        if 'tsun' in combined_df.columns:
            type_mapping['tsun'] = 'float64'
        if 'rhum' in combined_df.columns:
            type_mapping['rhum'] = 'float64'
        if 'source' in combined_df.columns:
            type_mapping['source'] = 'str'
        if 'city' in combined_df.columns:
            type_mapping['city'] = 'str'
        if 'region' in combined_df.columns:
            type_mapping['region'] = 'str'
        if 'station' in combined_df.columns:
            type_mapping['station'] = 'str'
        if 'lat' in combined_df.columns:
            type_mapping['lat'] = 'float'
        if 'lon' in combined_df.columns:
            type_mapping['lon'] = 'float'
        
        # Aplicar conversiones solo si hay columnas para convertir
        if type_mapping:
            combined_df = combined_df.astype(type_mapping)
        
        # Cargar datos en dlt
        load_info = pipeline.run(
            combined_df,
            table_name="weather_data",
            write_disposition="merge",
            primary_key=["date", "city", "source"]
        )
        logger.info(f"Datos cargados exitosamente: {load_info}")
        
        # Crear esquema weather_raw y copiar datos
        create_weather_raw_schema()
        
        # Log resumen final
        log_operation_success(logger, "extracción y carga de datos", 
                            successful_extractions=successful_extractions,
                            failed_extractions=failed_extractions,
                            total_records=len(combined_df),
                            cities_processed=len(cities))
    else:
        logger.warning("No se encontraron datos para cargar", 
                      successful_extractions=successful_extractions,
                      failed_extractions=failed_extractions)