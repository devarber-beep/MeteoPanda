import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict
from extract.dto import DailyWeatherDTO, CityConfigDTO
#from extract.aemet_api import fetch_daily_data as fetch_aemet_daily_data, get_station_id as get_aemet_station_id, save_aemet_to_parquet
import yaml

# Cargar API key
load_dotenv()
API_KEY = os.getenv("METEOSTAT_API_KEY")
METEOSTAT_BASE_URL = "https://meteostat.p.rapidapi.com"
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": "meteostat.p.rapidapi.com"}


def load_config(path: str) -> tuple[List[CityConfigDTO], str, str]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    return cfg["region"], cities, cfg["start_date"], cfg["end_date"]


def get_station_id(lat: float, lon: float) -> Optional[str]:
    """
    Obtiene el ID de la estación Meteostat más cercana a las coordenadas dadas.
    """
    url = f"{METEOSTAT_BASE_URL}/stations/nearby?lat={lat}&lon={lon}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        stations = response.json()["data"]
        return stations[0]["id"] if stations else None
    except Exception as e:
        print(f"Error al obtener estación Meteostat: {e}")
        return None


def fetch_daily_data(station_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Obtiene datos diarios de una estación Meteostat para un rango de fechas.
    """
    url = f"{METEOSTAT_BASE_URL}/stations/daily?station={station_id}&start={start}&end={end}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        raw_data = response.json()["data"]
        
        # Transformar datos al formato común
        validated_data = []
        for record in raw_data:
            try:
                weather_data = {
                    "date": datetime.strptime(record["date"].split()[0], "%Y-%m-%d").date(),
                    "tmax": record.get("tmax"),
                    "tmin": record.get("tmin"),
                    "tavg": record.get("tavg"),
                    "prcp": record.get("prcp"),
                    "wdir": record.get("wdir"),
                    "wspd": record.get("wspd"),
                    "wpgt": record.get("wpgt"),
                    "pres": record.get("pres"),
                    "snow": record.get("snow"),
                    "tsun": record.get("tsun"),
                    "rhum": record.get("rhum"),
                    "station": station_id
                }
                validated_data.append(DailyWeatherDTO(**weather_data).dict())
            except Exception as e:
                print(f"Error al procesar registro: {e}")
                continue
        
        return pd.DataFrame(validated_data)
    except Exception as e:
        print(f"Error al obtener datos de Meteostat: {e}")
        return pd.DataFrame()