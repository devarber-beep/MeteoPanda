import os
import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List
from dotenv import load_dotenv
from extract.dto import DailyWeatherDTO, CityConfigDTO

# Cargar API key
load_dotenv()
AEMET_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYW5pZWwuYmFyYmVyb2pAZ21haWwuY29tIiwianRpIjoiYjcwNGI1ZTEtMTY2My00MmQ1LWIyOTUtZmVhYTExNGVlM2I3IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3NDk3MTYyNTQsInVzZXJJZCI6ImI3MDRiNWUxLTE2NjMtNDJkNS1iMjk1LWZlYWExMTRlZTNiNyIsInJvbGUiOiIifQ.Mf_o4q6TWGaT8pwQCZW-sOeuOi0wNyBUxyOM24jgaRg"
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
HEADERS = {"api_key": AEMET_API_KEY}

def load_config(path: str) -> tuple[List[CityConfigDTO], str, str]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    return cfg["region"], cities, cfg["start_date"], cfg["end_date"]



def get_station_id(lat: float, lon: float) -> Optional[str]:
    """
    Obtiene el ID de la estación AEMET más cercana a las coordenadas dadas.
    """
    url = f"{AEMET_BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        stations = response.json()
        
        # Encontrar la estación más cercana
        # TODO: Implementar lógica de distancia más cercana
        return stations[0]["id"] if stations else None
    except Exception as e:
        print(f"Error al obtener estación AEMET: {e}")
        return None


def fetch_daily_data(station_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Obtiene datos diarios de una estación AEMET para un rango de fechas.
    """
    url = f"{AEMET_BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/{station_id}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        raw_data = response.json()
        
        # Transformar datos al formato común
        validated_data = []
        for record in raw_data:
            try:
                weather_data = {
                    "date": datetime.strptime(record["fecha"], "%Y-%m-%d").date(),
                    "tmax": float(record.get("tmax", "null").replace(",", ".")),
                    "tmin": float(record.get("tmin", "null").replace(",", ".")),
                    "tavg": float(record.get("tmed", "null").replace(",", ".")),
                    "prcp": float(record.get("prec", "null").replace(",", ".")),
                    "wdir": float(record.get("dir", "null").replace(",", ".")),
                    "wspd": float(record.get("velmedia", "null").replace(",", ".")),
                    "wpgt": float(record.get("racha", "null").replace(",", ".")),
                    "pres": float(record.get("presMax", "null").replace(",", ".")),
                    "snow": float(record.get("nieve", "null").replace(",", ".")),
                    "tsun": float(record.get("sol", "null").replace(",", ".")),
                    "rhum": float(record.get("hrMedia", "null").replace(",", ".")),
                    "station": station_id
                }
                validated_data.append(DailyWeatherDTO(**weather_data).dict())
            except Exception as e:
                print(f"Error al procesar registro: {e}")
                continue
        
        return pd.DataFrame(validated_data)
    except Exception as e:
        print(f"Error al obtener datos de AEMET: {e}")
        return pd.DataFrame() 