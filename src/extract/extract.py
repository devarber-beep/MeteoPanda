import os
from typing import Dict, List
import pandas as pd
from pathlib import Path
import dlt
from datetime import datetime
from dotenv import load_dotenv

from extract.dto import CityConfigDTO
from extract.meteo_api import fetch_daily_data as fetch_meteostat_data, get_station_id as get_meteostat_station_id
from extract.aemet_api import fetch_daily_data as fetch_aemet_data, get_station_id as get_aemet_station_id

# Cargar variables de entorno
load_dotenv()

# Configurar pipeline dlt
pipeline = dlt.pipeline(
    pipeline_name="weather_data",
    destination="duckdb",
    dataset_name="weather_data"
)

def load_city_config(config_path: str) -> tuple[List[CityConfigDTO], str, str]:
    """
    Carga la configuración de ciudades desde un archivo YAML.
    """
    import yaml
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    return cities, cfg["start_date"], cfg["end_date"]

def extract_meteostat_data(city: CityConfigDTO, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extrae datos de Meteostat para una ciudad.
    """
    try:
        station_id = get_meteostat_station_id(city.latitude, city.longitude)
        if station_id:
            df = fetch_meteostat_data(station_id, start_date, end_date)
            df["source"] = "meteostat"
            df["city"] = city.name
            return df
    except Exception as e:
        print(f"Error extrayendo datos de Meteostat para {city.name}: {e}")
    return pd.DataFrame()

def extract_aemet_data(city: CityConfigDTO, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extrae datos de AEMET para una ciudad.
    """
    try:
        station_id = get_aemet_station_id(city.latitude, city.longitude)
        if station_id:
            df = fetch_aemet_data(station_id, start_date, end_date)
            df["source"] = "aemet"
            df["city"] = city.name
            return df
    except Exception as e:
        print(f"Error extrayendo datos de AEMET para {city.name}: {e}")
    return pd.DataFrame()

def standardize_weather_data(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """
    Estandariza los datos meteorológicos al formato común.
    """
    if source == "meteostat":
        return df.rename(columns={
            "date": "fecha",
            "tavg": "temperatura_media",
            "tmin": "temperatura_minima",
            "tmax": "temperatura_maxima",
            "prcp": "precipitacion",
            "wdir": "direccion_viento",
            "wspd": "velocidad_viento",
            "wpgt": "racha_maxima",
            "pres": "presion_atmosferica",
            "snow": "nieve",
            "tsun": "horas_sol"
        })
    elif source == "aemet":
        return df  # Ya está en el formato estándar
    return df

def extract_and_load(config_path: str):
    """
    Función principal que extrae datos de todas las fuentes y los carga en dlt.
    """
    # Cargar configuración
    cities, start_date, end_date = load_city_config(config_path)
    
    # Extraer datos de todas las fuentes
    all_data = []
    
    for city in cities:
        print(f"\nProcesando ciudad: {city.name}")
        
        # Extraer datos de Meteostat
        meteostat_df = extract_meteostat_data(city, start_date, end_date)
        if not meteostat_df.empty:
            meteostat_df = standardize_weather_data(meteostat_df, "meteostat")
            all_data.append(meteostat_df)
        
        # Extraer datos de AEMET
        aemet_df = extract_aemet_data(city, start_date, end_date)
        if not aemet_df.empty:
            aemet_df = standardize_weather_data(aemet_df, "aemet")
            all_data.append(aemet_df)
    
    if all_data:
        # Combinar todos los datos
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Cargar datos en dlt
        load_info = pipeline.run(
            combined_df,
            table_name="weather_data",
            write_disposition="merge",
            primary_key=["date", "city", "source"]
        )
        print(f"\nDatos cargados exitosamente: {load_info}")
    else:
        print("No se encontraron datos para cargar")

if __name__ == "__main__":
    config_path = "config/cities.yaml"
    extract_and_load(config_path) 