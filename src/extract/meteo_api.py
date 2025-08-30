import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
<<<<<<< HEAD
from typing import List
from extract.dto import DailyWeatherDTO, CityConfigDTO
=======
from typing import List, Dict
from .dto import DailyWeatherDTO, CityConfigDTO
#from extract.aemet_api import fetch_daily_data as fetch_aemet_daily_data, get_station_id as get_aemet_station_id, save_aemet_to_parquet
>>>>>>> 0c17efa (Solucion definitiva al modulo de descarga masiva)
import yaml

# Cargar API key
load_dotenv()
<<<<<<< HEAD
API_KEY = os.getenv("METEOSTAT_API_KEY")
=======
API_KEY = "b5e36938d0msh2b370cd85bb5d48p15ee49jsn8987e37d36ff"
METEOSTAT_BASE_URL = "https://meteostat.p.rapidapi.com"
>>>>>>> 0c17efa (Solucion definitiva al modulo de descarga masiva)
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": "meteostat.p.rapidapi.com"}


def load_config(path: str) -> tuple[List[CityConfigDTO], str, str]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    return cfg["region"], cities, cfg["start_date"], cfg["end_date"]


def get_station_id(lat: float, lon: float) -> str:
    url = f"https://meteostat.p.rapidapi.com/stations/nearby?lat={lat}&lon={lon}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    stations = r.json()["data"]
    return stations[0]["id"] if stations else None


def fetch_daily_data(station_id: str, start: str, end: str) -> pd.DataFrame:
    url = f"https://meteostat.p.rapidapi.com/stations/daily?station={station_id}&start={start}&end={end}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    raw_data = r.json()["data"]
    validated = [DailyWeatherDTO(**d).dict() for d in raw_data]
    return pd.DataFrame(validated)


def save_to_parquet(df: pd.DataFrame, city_name: str):
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{city_name.lower().replace(' ', '_')}_daily .parquet"
    df.to_parquet(file_path, index=False)
    print(f"[✓] Guardado: {file_path}")
