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

# Rutas base
CONFIG_PATH = Path("config/config.yaml")
BRONZE_PATH = Path("data/raw")
SQL_PATH = Path("src/transform/sql")


# Cargar variables de entorno
load_dotenv()

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
    return cities, cfg["start_date"], cfg["end_date"], cfg["region"]

def extract_meteostat_data(city: CityConfigDTO, start_date: str, end_date: str, region: str) -> pd.DataFrame:
    """
    Extrae datos de Meteostat para una ciudad.
    """
    try:
        station_id = get_meteostat_station_id(city.latitude, city.longitude)
        if station_id:
            df = fetch_meteostat_data(station_id, start_date, end_date)
            df["source"] = "meteostat"
            df["city"] = city.name
            df["region"] = region
            df["station"] = station_id  # Agregar ID de estación
            return df
    except Exception as e:
        print(f"Error extrayendo datos de Meteostat para {city.name}: {e}")
    return pd.DataFrame()

def extract_aemet_data(city: CityConfigDTO, start_date: str, end_date: str, region: str) -> pd.DataFrame:
    """
    Extrae datos de AEMET para una ciudad.
    """
    try:
        station_id = get_aemet_station_id(city.latitude, city.longitude)
        if station_id:
            df = fetch_aemet_data(station_id, start_date, end_date)
            df["source"] = "aemet"
            df["city"] = city.name
            df["region"] = region
            df["station"] = station_id  # Agregar ID de estación
            return df
    except Exception as e:
        print(f"Error extrayendo datos de AEMET para {city.name}: {e}")
    return pd.DataFrame()

def standardize_weather_data(df: pd.DataFrame, source: str) -> pd.DataFrame: #ToDo: Implementar
    """ 
    Estandariza los datos meteorológicos al formato común.
    """
    if source == "meteostat":
        return df
    elif source == "aemet":
        return df  
    return df

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
            print("No se encontraron esquemas DLT con datos")
            con.close()
            return
        
        latest_schema = schemas[0][0]
        print(f"Copiando datos desde {latest_schema} a weather_raw")
        
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
                        
            # Copiar datos al esquema weather_raw con coordenadas del config
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS weather_raw.weather_data AS 
                SELECT 
                    *,
                    CASE 
                        WHEN city = 'Sevilla' THEN {city_coords['Sevilla']['lat']}
                        WHEN city = 'Malaga' THEN {city_coords['Malaga']['lat']}
                        WHEN city = 'Cordoba' THEN {city_coords['Cordoba']['lat']}
                        WHEN city = 'Granada' THEN {city_coords['Granada']['lat']}
                        WHEN city = 'Almeria' THEN {city_coords['Almeria']['lat']}
                        WHEN city = 'Cadiz' THEN {city_coords['Cadiz']['lat']}
                        WHEN city = 'Huelva' THEN {city_coords['Huelva']['lat']}
                        WHEN city = 'Jaen' THEN {city_coords['Jaen']['lat']}
                        ELSE NULL
                    END AS lat,
                    CASE 
                        WHEN city = 'Sevilla' THEN {city_coords['Sevilla']['lon']}
                        WHEN city = 'Malaga' THEN {city_coords['Malaga']['lon']}
                        WHEN city = 'Cordoba' THEN {city_coords['Cordoba']['lon']}
                        WHEN city = 'Granada' THEN {city_coords['Granada']['lon']}
                        WHEN city = 'Almeria' THEN {city_coords['Almeria']['lon']}
                        WHEN city = 'Cadiz' THEN {city_coords['Cadiz']['lon']}
                        WHEN city = 'Huelva' THEN {city_coords['Huelva']['lon']}
                        WHEN city = 'Jaen' THEN {city_coords['Jaen']['lon']}
                        ELSE NULL
                    END AS lon
                FROM {latest_schema}.weather_data
            """)
            
            # Verificar que se copiaron los datos
            count = con.execute("SELECT COUNT(*) FROM weather_raw.weather_data").fetchone()[0]
            print(f"✅ Datos copiados exitosamente: {count} registros en weather_raw.weather_data")
            
            # Verificar coordenadas
            coords_count = con.execute("""
                SELECT COUNT(*) 
                FROM weather_raw.weather_data 
                WHERE lat IS NOT NULL AND lon IS NOT NULL
            """).fetchone()[0]
                        
        else:
            print(f"❌ No se encontró la tabla weather_data en {latest_schema}")
        
        con.close()
        
    except Exception as e:
        print(f"Error creando esquema weather_raw: {e}")
        if 'con' in locals():
            con.close()

def extract_and_load(config_path: str):
    """
    Función principal que extrae datos de todas las fuentes y los carga en dlt.
    """
    # Cargar configuración
    cities, start_date, end_date, region = load_city_config(config_path)
    
    all_data = []
    
    for city in cities:
        print(f"\nProcesando ciudad: {city.name}")
        
        # Extraer datos de Meteostat
        print(f"Extrayendo datos de Meteostat {city.name}")
        meteostat_df = extract_meteostat_data(city, start_date, end_date, region)
        if not meteostat_df.empty:
            #meteostat_df = standardize_weather_data(meteostat_df, "meteostat")
            all_data.append(meteostat_df)
        
        # Extraer datos de AEMET
        print(f"Extrayendo datos de AEMET {city.name}")
        aemet_df = extract_aemet_data(city, start_date, end_date, region)
        if not aemet_df.empty:
            #aemet_df = standardize_weather_data(aemet_df, "aemet")
            all_data.append(aemet_df)
        
    
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
        print(f"\nDatos cargados exitosamente: {load_info}")
        
        # Crear esquema weather_raw y copiar datos
        create_weather_raw_schema()
    else:
        print("No se encontraron datos para cargar")