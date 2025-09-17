import os
import requests
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from typing import List, Dict, Optional
from .dto import DailyWeatherDTO, CityConfigDTO
from ..utils.logging_config import get_logger, log_api_request, log_performance_warning, log_validation_warning
import yaml
import time
import random
from functools import wraps
from collections import deque
import threading

# Cargar API key
load_dotenv()
API_KEY = "b5e36938d0msh2b370cd85bb5d48p15ee49jsn8987e37d36ff"
METEOSTAT_BASE_URL = "https://meteostat.p.rapidapi.com"
HEADERS = {"x-rapidapi-key": API_KEY, "x-rapidapi-host": "meteostat.p.rapidapi.com"}

# Configurar logger
logger = get_logger("meteostat_api")

# Rate Limiter para Meteostat (más conservador que AEMET)
class MeteostatRateLimiter:
    """
    Rate limiter específico para Meteostat API.
    Meteostat tiene límites más estrictos que AEMET.
    """
    
    def __init__(self, max_requests: int = 30, time_window: float = 60.0):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()
    
    def acquire(self, wait: bool = True) -> bool:
        with self.lock:
            now = time.time()
            
            # Limpiar peticiones antiguas
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            
            if not wait:
                return False
            
            # Calcular tiempo de espera
            oldest_request = self.requests[0]
            wait_time = self.time_window - (now - oldest_request)
            
            if wait_time > 0:
                print(f"Meteostat rate limit alcanzado. Esperando {wait_time:.2f} segundos...")
                time.sleep(wait_time)
                
                self.requests.popleft()
                self.requests.append(time.time())
                return True
            
            return False
    
    def get_stats(self) -> Dict:
        with self.lock:
            now = time.time()
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            
            return {
                "current_requests": len(self.requests),
                "max_requests": self.max_requests,
                "time_window": self.time_window,
                "available_slots": max(0, self.max_requests - len(self.requests))
            }

# Configuración de rate limiting para Meteostat
METEOSTAT_RATE_LIMITER = MeteostatRateLimiter(max_requests=25, time_window=60.0)  # 25 peticiones por minuto

def with_meteostat_rate_limiting():
    """Decorador para aplicar rate limiting a peticiones de Meteostat"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not METEOSTAT_RATE_LIMITER.acquire(wait=True):
                raise Exception("No se pudo adquirir slot del rate limiter de Meteostat")
            
            result = func(*args, **kwargs)
            
            # Mostrar estadísticas si quedan pocos slots
            stats = METEOSTAT_RATE_LIMITER.get_stats()
            if stats["available_slots"] < 5:
                print(f"Meteostat rate limiter: {stats['current_requests']}/{stats['max_requests']} peticiones en uso")
            
            return result
        return wrapper
    return decorator

def retry_with_exponential_backoff_meteostat(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    jitter: bool = True
):
    """Decorador para retry logic específico de Meteostat"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.RequestException, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        print(f"Error después de {max_retries + 1} intentos en Meteostat: {e}")
                        raise
                    
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)
                    
                    print(f"Intento {attempt + 1} falló en Meteostat: {e}. Reintentando en {delay:.2f} segundos...")
                    time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator

@with_meteostat_rate_limiting()
@retry_with_exponential_backoff_meteostat(max_retries=3, base_delay=2.0, max_delay=30.0)
def make_meteostat_request(url: str, description: str = "petición Meteostat") -> requests.Response:
    """
    Función helper para hacer peticiones a Meteostat con manejo de errores específicos.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=(5, 30))
        
        # Log de la petición API
        log_api_request(logger, "Meteostat", url, response.status_code, description=description)
        
        if response.status_code >= 400:
            response.raise_for_status()
            
        return response
        
    except requests.exceptions.Timeout:
        print(f"Timeout en {description}")
        raise
    except requests.exceptions.ConnectionError:
        print(f"Error de conexión en {description}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Error en {description}: {e}")
        raise

def load_config(path: str) -> tuple[List[CityConfigDTO], str, str]:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    cities = [CityConfigDTO(**c) for c in cfg["cities"]]
    return cfg["region"], cities, cfg["start_date"], cfg["end_date"]


@with_meteostat_rate_limiting()
@retry_with_exponential_backoff_meteostat(max_retries=3, base_delay=2.0, max_delay=30.0)
def get_station_id(lat: float, lon: float) -> Optional[str]:
    """
    Obtiene el ID de la estación Meteostat más cercana a las coordenadas dadas.
    """
    url = f"{METEOSTAT_BASE_URL}/stations/nearby?lat={lat}&lon={lon}"
    
    try:
        response = make_meteostat_request(url, f"obtener estación para lat={lat}, lon={lon}")
        stations = response.json()["data"]
        station_id = stations[0]["id"] if stations else None
        
        if station_id:
            print(f"Estación Meteostat encontrada: {station_id}")
        else:
            print(f"No se encontraron estaciones Meteostat para lat={lat}, lon={lon}")
        
        return station_id
        
    except Exception as e:
        print(f"Error al obtener estación Meteostat: {e}")
        return None


@with_meteostat_rate_limiting()
@retry_with_exponential_backoff_meteostat(max_retries=3, base_delay=2.0, max_delay=30.0)
def fetch_daily_data(station_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Obtiene datos diarios de una estación Meteostat para un rango de fechas.
    """
    url = f"{METEOSTAT_BASE_URL}/stations/daily?station={station_id}&start={start}&end={end}"
    
    try:
        response = make_meteostat_request(url, f"obtener datos diarios para estación {station_id}")
        raw_data = response.json()["data"]
        
        if not raw_data:
            print(f"No se encontraron datos para la estación {station_id} en el período {start} - {end}")
            return pd.DataFrame()
        
        validated = [DailyWeatherDTO(**d).dict() for d in raw_data]
        df = pd.DataFrame(validated)
        
        print(f"Datos Meteostat obtenidos: {len(df)} registros para estación {station_id}")
        return df
        
    except Exception as e:
        print(f"Error al obtener datos de Meteostat: {e}")
        return pd.DataFrame()


def save_to_parquet(df: pd.DataFrame, city_name: str):
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{city_name.lower().replace(' ', '_')}_meteostat_daily.parquet"
    df.to_parquet(file_path, index=False)
    print(f"[✓] Guardado Meteostat: {file_path}")


def get_meteostat_rate_limiter_stats() -> Dict:
    """Obtiene estadísticas del rate limiter de Meteostat"""
    return METEOSTAT_RATE_LIMITER.get_stats()


def log_meteostat_rate_limiter_status():
    """Función para mostrar el estado actual del rate limiter de Meteostat"""
    stats = get_meteostat_rate_limiter_stats()
    print(f"🌤️  Meteostat Rate Limiter Status: {stats['current_requests']}/{stats['max_requests']} peticiones activas")
    print(f"   Slots disponibles: {stats['available_slots']}")
    print(f"   Ventana de tiempo: {stats['time_window']} segundos")
    
    usage_percentage = (stats['current_requests'] / stats['max_requests']) * 100
    if usage_percentage > 80:
        print(f"   ⚠️  Uso alto: {usage_percentage:.1f}%")
    elif usage_percentage > 60:
        print(f"   ⚡ Uso moderado: {usage_percentage:.1f}%")
    else:
        print(f"   ✅ Uso normal: {usage_percentage:.1f}%")


def reset_meteostat_rate_limiter():
    """Resetea el rate limiter de Meteostat"""
    with METEOSTAT_RATE_LIMITER.lock:
        METEOSTAT_RATE_LIMITER.requests.clear()
    print("Rate limiter de Meteostat reseteado")
