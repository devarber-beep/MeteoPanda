import os
import requests
import pandas as pd
import json
from datetime import datetime
from typing import Optional, List, Dict
from dotenv import load_dotenv
from pathlib import Path
from extract.dto import DailyWeatherDTO, CityConfigDTO
import math

# Cargar API key
load_dotenv()
AEMET_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYW5pZWwuYmFyYmVyb2pAZ21haWwuY29tIiwianRpIjoiYjcwNGI1ZTEtMTY2My00MmQ1LWIyOTUtZmVhYTExNGVlM2I3IiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3NDk3MTYyNTQsInVzZXJJZCI6ImI3MDRiNWUxLTE2NjMtNDJkNS1iMjk1LWZlYWExMTRlZTNiNyIsInJvbGUiOiIifQ.Mf_o4q6TWGaT8pwQCZW-sOeuOi0wNyBUxyOM24jgaRg"
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
HEADERS = {"api_key": AEMET_API_KEY}

# Paths
UTILS_PATH = Path("data/utils")
STATIONS_FILE = UTILS_PATH / "aemet_stations.json"

def download_stations_data() -> Dict:
    """
    Descarga los datos de las estaciones de AEMET y los guarda localmente.
    Devuelve los datos de las estaciones como un diccionario.
    """
    url = f"{AEMET_BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
    
    try:
        # Primera petición para obtener el endpoint de los datos
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        metadata = response.json()
        
        if metadata.get("estado") != 200:
            print(f"Error en la respuesta de AEMET: {metadata.get('descripcion')}")
            return {}
            
        # Segunda petición para obtener los datos reales
        data_url = metadata.get("datos")
        if not data_url:
            print("No se encontró la URL de los datos en la respuesta")
            return {}
            
        data_response = requests.get(data_url, headers=HEADERS)
        data_response.raise_for_status()
        stations = data_response.json()
        
        # Create utils directory if it doesn't exist
        UTILS_PATH.mkdir(parents=True, exist_ok=True)
        
        # Save stations data
        with open(STATIONS_FILE, 'w', encoding='utf-8') as f:
            json.dump(stations, f, ensure_ascii=False, indent=2)
            
        return stations
    except Exception as e:
        print(f"Error descargando AEMET datos de estaciones: {e}")
        return {}

def load_stations_data() -> Dict:
    """
    Carga los datos de las estaciones de AEMET desde un archivo local o los descarga si no están disponibles.
    Devuelve los datos de las estaciones como un diccionario.
    """
    if not STATIONS_FILE.exists():
        return download_stations_data()
    
    try:
        with open(STATIONS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error cargando datos de estaciones: {e}")
        return download_stations_data()

def dms_to_decimal(dms_str: str) -> float:
    """
    Convierte coordenadas de formato DMS (Grados, Minutos, Segundos) a decimal.
    """
    try:
        # Extraer la dirección (último carácter)
        direction = dms_str[-1].upper()
        # Extraer los números
        dms = dms_str[:-1]
        
        # Convertir a grados, minutos y segundos
        degrees = int(dms[:2])
        minutes = int(dms[2:4])
        seconds = int(dms[4:])
        
        # Calcular decimal
        decimal = degrees + minutes/60 + seconds/3600
        
        # Ajustar según la dirección
        if direction in ['S', 'W']:
            decimal = -decimal
            
        return decimal
    except Exception as e:
        print(f"Error convirtiendo coordenadas DMS a decimal: {e}")
        return 0.0

def get_nearest_station(lat: float, lon: float, stations: List[Dict]) -> Optional[str]:
    """
    Encuentra la estación más cercana usando la distancia euclidea.
    """
    if not stations:
        return None
    
    min_distance = float('inf')
    nearest_station = None
    
    for station in stations:
        try:
            # Convertir coordenadas de DMS a decimal
            station_lat = dms_to_decimal(station.get('latitud', '0'))
            station_lon = dms_to_decimal(station.get('longitud', '0'))
            
            # Calcular distancia euclidiana
            distance = math.sqrt((lat - station_lat)**2 + (lon - station_lon)**2)
            
            if distance < min_distance:
                min_distance = distance
                nearest_station = station['indicativo']
        except (ValueError, KeyError) as e:
            print(f"Error procesando coordenadas de estación: {e}")
            continue
    
    return nearest_station

def get_station_id(lat: float, lon: float) -> Optional[str]:
    """
    Obtiene el ID de la estación AEMET más cercana a las coordenadas dadas.
    """
    stations = load_stations_data()
    return get_nearest_station(lat, lon, stations)

def load_config(path: str) -> tuple[List[CityConfigDTO], str, str]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    return cfg["region"], cities, cfg["start_date"], cfg["end_date"]

def fetch_daily_data(station_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Obtiene datos diarios de una estación AEMET para un rango de fechas.
    Las fechas deben estar en formato ISO 8601 (YYYY-MM-DD).
    """
    # Asegurar que las fechas estén en formato ISO 8601
    try:
        # Convertir a datetime y añadir hora 00:00:00 UTC
        start_date = datetime.strptime(start, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00UTC")
        end_date = datetime.strptime(end, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00UTC")
    except ValueError as e:
        print(f"Error en el formato de fechas: {e}")
        return pd.DataFrame()
    
    url = f"{AEMET_BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_id}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        raw_data = response.json()

        print(f"Respuesta de AEMET para {station_id, start_date, end_date}: {raw_data}")
        
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