import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.poolmanager import PoolManager
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Callable, Any
from dotenv import load_dotenv
from pathlib import Path
import math
import time
import random
from functools import wraps
from collections import deque
import threading
import ssl

from .dto import DailyWeatherDTO, CityConfigDTO
from ..utils.logging_config import get_logger, log_api_request, log_performance_warning, log_validation_warning
import yaml

# Cargar API key
load_dotenv()
AEMET_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJkYW5pZWwuYmFyYmVyb2pAZ21haWwuY29tIiwianRpIjoiNzZjM2ZkODMtYjVlZi00YTQ3LWI4M2QtMTc5MjJjMzZlMjMxIiwiaXNzIjoiQUVNRVQiLCJpYXQiOjE3NTYwMzE4MTcsInVzZXJJZCI6Ijc2YzNmZDgzLWI1ZWYtNGE0Ny1iODNkLTE3OTIyYzM2ZTIzMSIsInJvbGUiOiIifQ.rVEOBWGmTc3utFTnYY27SB_tVfJmIPXGIIn1Rp80MKI"

AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"
HEADERS = {"api_key": AEMET_API_KEY}

# Configurar logger
logger = get_logger("aemet_api")

# Paths
UTILS_PATH = Path("data/utils")
STATIONS_FILE = UTILS_PATH / "aemet_stations.json"

class RateLimiter:
    """
    Rate limiter thread-safe para controlar peticiones a APIs.
    Implementa sliding window rate limiting.
    """
    
    def __init__(self, max_requests: int, time_window: float):
        """
        Args:
            max_requests: Número máximo de peticiones permitidas
            time_window: Ventana de tiempo en segundos
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = threading.Lock()
    
    def acquire(self, wait: bool = True) -> bool:
        """
        Intenta adquirir un slot para una petición.
        
        Args:
            wait: Si esperar hasta que haya un slot disponible
            
        Returns:
            True si se adquirió el slot, False si no se pudo
        """
        with self.lock:
            now = time.time()
            
            # Limpiar peticiones antiguas fuera de la ventana de tiempo
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            
            # Verificar si hay espacio disponible
            if len(self.requests) < self.max_requests:
                self.requests.append(now)
                return True
            
            if not wait:
                return False
            
            # Calcular cuánto esperar hasta que haya un slot disponible
            oldest_request = self.requests[0]
            wait_time = self.time_window - (now - oldest_request)
            
            if wait_time > 0:
                print(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos...")
                time.sleep(wait_time)
                
                # Limpiar y añadir la nueva petición
                self.requests.popleft()
                self.requests.append(time.time())
                return True
            
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del rate limiter"""
        with self.lock:
            now = time.time()
            # Limpiar peticiones antiguas
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            
            return {
                "current_requests": len(self.requests),
                "max_requests": self.max_requests,
                "time_window": self.time_window,
                "available_slots": max(0, self.max_requests - len(self.requests))
            }

# Configuración de rate limiting para AEMET
# Basado en la documentación de AEMET: ~100 peticiones por minuto
AEMET_RATE_LIMITER = RateLimiter(max_requests=90, time_window=60.0)  # 90 peticiones por minuto (conservador)

class ConnectionPoolManager:
    """
    Gestor de connection pools para optimizar las conexiones HTTP.
    Proporciona configuración optimizada para diferentes tipos de peticiones.
    """
    
    def __init__(self, 
                 pool_connections: int = 10,
                 pool_maxsize: int = 20,
                 max_retries: int = 3,
                 backoff_factor: float = 0.3,
                 status_forcelist: List[int] = None,
                 timeout: tuple = (5, 30)):
        """
        Args:
            pool_connections: Número de pools de conexión
            pool_maxsize: Tamaño máximo del pool por host
            max_retries: Número máximo de reintentos
            backoff_factor: Factor de backoff para reintentos
            status_forcelist: Lista de códigos de estado para reintentar
            timeout: Tupla (connect_timeout, read_timeout)
        """
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.status_forcelist = status_forcelist or [429, 500, 502, 503, 504]
        self.timeout = timeout
        
        # Crear la sesión con configuración optimizada
        self.session = self._create_optimized_session()
    
    def _create_optimized_session(self) -> requests.Session:
        """Crea una sesión HTTP optimizada con connection pooling"""
        
        # Crear sesión
        session = requests.Session()
        
        # Configurar retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=self.status_forcelist,
            backoff_factor=self.backoff_factor,
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        
        # Crear adapter personalizado
        adapter = HTTPAdapter(
            pool_connections=self.pool_connections,
            pool_maxsize=self.pool_maxsize,
            max_retries=retry_strategy,
            pool_block=False
        )
        
        # Montar el adapter en la sesión
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Configurar headers por defecto
        session.headers.update({
            'User-Agent': 'MeteoPanda/1.0 (Weather Data Extractor)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        
        return session
    
    def get_session(self) -> requests.Session:
        """Retorna la sesión configurada"""
        return self.session
    
    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Realiza una petición HTTP usando la sesión optimizada.
        
        Args:
            method: Método HTTP (GET, POST, etc.)
            url: URL de la petición
            **kwargs: Argumentos adicionales para requests
            
        Returns:
            Response object
        """
        # Configurar timeout si no está especificado
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
        
        return self.session.request(method, url, **kwargs)
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """Realiza una petición GET"""
        return self.request('GET', url, **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """Realiza una petición POST"""
        return self.request('POST', url, **kwargs)
    
    def close(self):
        """Cierra la sesión y libera recursos"""
        if self.session:
            self.session.close()
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """Obtiene estadísticas del connection pool"""
        try:
            # Obtener estadísticas del pool manager
            pool_manager = self.session.adapters['https://'].poolmanager
            
            return {
                "pool_connections": self.pool_connections,
                "pool_maxsize": self.pool_maxsize,
                "active_connections": len(pool_manager.pools),
                "timeout": self.timeout,
                "retry_config": {
                    "max_retries": self.max_retries,
                    "backoff_factor": self.backoff_factor,
                    "status_forcelist": self.status_forcelist
                }
            }
        except Exception as e:
            return {
                "error": f"No se pudieron obtener estadísticas: {e}",
                "pool_connections": self.pool_connections,
                "pool_maxsize": self.pool_maxsize
            }

# Configuración del connection pool para AEMET
AEMET_CONNECTION_POOL = ConnectionPoolManager(
    pool_connections=10,      # 10 pools de conexión
    pool_maxsize=20,          # 20 conexiones máximas por host
    max_retries=3,            # 3 reintentos
    backoff_factor=0.3,       # Backoff exponencial
    timeout=(5, 30)           # 5s connect, 30s read
)

def with_rate_limiting(rate_limiter: RateLimiter = AEMET_RATE_LIMITER):
    """
    Decorador para aplicar rate limiting a funciones que hacen peticiones HTTP.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Adquirir slot del rate limiter
            if not rate_limiter.acquire(wait=True):
                raise Exception("No se pudo adquirir slot del rate limiter")
            
            # Ejecutar la función
            result = func(*args, **kwargs)
            
            # Opcional: mostrar estadísticas
            stats = rate_limiter.get_stats()
            if stats["available_slots"] < 10:  # Solo mostrar si quedan pocos slots
                print(f"Rate limiter stats: {stats['current_requests']}/{stats['max_requests']} peticiones en uso")
            
            return result
        return wrapper
    return decorator


def log_rate_limiter_status():
    """
    Función para mostrar el estado actual del rate limiter.
    Útil para debugging y monitoreo.
    """
    stats = get_rate_limiter_stats()
    print(f"   Rate Limiter Status: {stats['current_requests']}/{stats['max_requests']} peticiones activas")
    print(f"   Slots disponibles: {stats['available_slots']}")
    print(f"   Ventana de tiempo: {stats['time_window']} segundos")
    
    # Calcular porcentaje de uso
    usage_percentage = (stats['current_requests'] / stats['max_requests']) * 100
    if usage_percentage > 80:
        log_performance_warning(logger, "Rate Limiter AEMET", usage_percentage, threshold=80)
        logger.warning(f"Uso alto del rate limiter: {usage_percentage:.1f}%", extra_data=stats)
    elif usage_percentage > 60:
        logger.info(f"Uso moderado del rate limiter: {usage_percentage:.1f}%", extra_data=stats)
    else:
        logger.info(f"Uso normal del rate limiter: {usage_percentage:.1f}%", extra_data=stats)


def get_optimal_rate_limiter_config() -> Dict[str, Any]:
    """
    Retorna la configuración óptima del rate limiter basada en la documentación de AEMET.
    """
    return {
        "max_requests": 90,
        "time_window": 60.0,
        "description": "90 peticiones por minuto (conservador para evitar 429)",
        "recommendation": "Ajustar según el comportamiento observado de la API"
    }


def get_connection_pool_stats() -> Dict[str, Any]:
    """
    Obtiene estadísticas del connection pool de AEMET.
    """
    return AEMET_CONNECTION_POOL.get_pool_stats()


def log_connection_pool_status():
    """
    Función para mostrar el estado actual del connection pool.
    Útil para debugging y monitoreo.
    """
    stats = get_connection_pool_stats()
    
    logger.info(f"Connection Pool Status: {stats['pool_connections']}/{stats['pool_maxsize']} conexiones", extra_data=stats)
    
    if 'retry_config' in stats:
        retry = stats['retry_config']
        logger.debug(f"Retry config: {retry['max_retries']} max, {retry['backoff_factor']} backoff", extra_data=retry)


def configure_connection_pool(
    pool_connections: int = None,
    pool_maxsize: int = None,
    max_retries: int = None,
    backoff_factor: float = None,
    timeout: tuple = None
):
    """
    Configura el connection pool con nuevos parámetros.
    
    Args:
        pool_connections: Número de pools de conexión
        pool_maxsize: Tamaño máximo del pool por host
        max_retries: Número máximo de reintentos
        backoff_factor: Factor de backoff para reintentos
        timeout: Tupla (connect_timeout, read_timeout)
    """
    global AEMET_CONNECTION_POOL
    
    # Cerrar la sesión actual
    AEMET_CONNECTION_POOL.close()
    
    # Obtener valores actuales si no se especifican
    current_stats = AEMET_CONNECTION_POOL.get_pool_stats()
    
    pool_connections = pool_connections or current_stats['pool_connections']
    pool_maxsize = pool_maxsize or current_stats['pool_maxsize']
    max_retries = max_retries or current_stats['retry_config']['max_retries']
    backoff_factor = backoff_factor or current_stats['retry_config']['backoff_factor']
    timeout = timeout or current_stats['timeout']
    
    # Crear nueva configuración
    AEMET_CONNECTION_POOL = ConnectionPoolManager(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=max_retries,
        backoff_factor=backoff_factor,
        timeout=timeout
    )
    
    print(f"Connection pool configurado: {pool_connections} pools, {pool_maxsize} maxsize, {timeout} timeout")


def get_optimal_connection_pool_config() -> Dict[str, Any]:
    """
    Retorna la configuración óptima del connection pool para AEMET.
    """
    return {
        "pool_connections": 10,
        "pool_maxsize": 20,
        "max_retries": 3,
        "backoff_factor": 0.3,
        "timeout": (5, 30),
        "description": "Configuración optimizada para extracción de datos meteorológicos",
        "recommendation": "Ajustar según el volumen de peticiones y latencia de red"
    }


def cleanup_connection_pool():
    """
    Limpia y cierra el connection pool para liberar recursos.
    Útil al finalizar la aplicación o para reiniciar conexiones.
    """
    AEMET_CONNECTION_POOL.close()
    print("Connection pool cerrado y recursos liberados")


def get_rate_limiter_stats() -> Dict[str, Any]:
    """
    Obtiene estadísticas del rate limiter de AEMET.
    """
    return AEMET_RATE_LIMITER.get_stats()


def reset_rate_limiter():
    """
    Resetea el rate limiter (útil para testing o reinicio de sesión).
    """
    with AEMET_RATE_LIMITER.lock:
        AEMET_RATE_LIMITER.requests.clear()
    print("Rate limiter reseteado")


def configure_rate_limiter(max_requests: int, time_window: float):
    """
    Configura el rate limiter con nuevos parámetros.
    
    Args:
        max_requests: Número máximo de peticiones permitidas
        time_window: Ventana de tiempo en segundos
    """
    global AEMET_RATE_LIMITER
    AEMET_RATE_LIMITER = RateLimiter(max_requests, time_window)
    print(f"Rate limiter configurado: {max_requests} peticiones por {time_window} segundos")

def retry_with_exponential_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retry_exceptions: tuple = (requests.RequestException, ConnectionError, TimeoutError)
):
    """
    Decorador para implementar retry logic con backoff exponencial.
    
    Args:
        max_retries: Número máximo de reintentos
        base_delay: Delay inicial en segundos
        max_delay: Delay máximo en segundos
        exponential_base: Base para el cálculo exponencial
        jitter: Si añadir jitter aleatorio para evitar thundering herd
        retry_exceptions: Tupla de excepciones que deben provocar reintento
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Último intento falló, re-raise la excepción
                        print(f"Error después de {max_retries + 1} intentos: {e}")
                        raise
                    
                    # Calcular delay con backoff exponencial
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    # Añadir jitter si está habilitado
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)
                    
                    print(f"Intento {attempt + 1} falló: {e}. Reintentando en {delay:.2f} segundos...")
                    time.sleep(delay)
            
            # Nunca debería llegar aquí, pero por si acaso
            raise last_exception
            
        return wrapper
    return decorator


def retry_with_http_status_handling(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
):
    """
    Decorador específico para manejar errores HTTP con diferentes estrategias según el código de estado.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.HTTPError as e:
                    last_exception = e
                    response = e.response
                    
                    if attempt == max_retries:
                        print(f"Error HTTP después de {max_retries + 1} intentos: {e}")
                        raise
                    
                    # Manejar diferentes códigos de estado
                    if response.status_code == 429:  # Too Many Requests
                        # Para rate limiting, usar un delay más largo
                        delay = min(30.0 * (exponential_base ** attempt), max_delay)
                        log_api_request(logger, "AEMET", url, response.status_code, 
                                      rate_limited=True, retry_attempt=attempt + 1, delay=delay)
                        logger.warning(f"Rate limit alcanzado (429). Esperando {delay:.2f} segundos antes del reintento {attempt + 1}...")
                    elif response.status_code in [500, 502, 503, 504]:  # Server errors
                        # Para errores del servidor, usar delay normal
                        delay = min(base_delay * (exponential_base ** attempt), max_delay)
                        log_api_request(logger, "AEMET", url, response.status_code, 
                                      server_error=True, retry_attempt=attempt + 1, delay=delay)
                        logger.warning(f"Error del servidor ({response.status_code}). Reintentando en {delay:.2f} segundos...")
                    else:
                        # Para otros errores HTTP, no reintentar
                        log_api_request(logger, "AEMET", url, response.status_code, 
                                      error=True, error_message=str(e))
                        logger.error(f"Error HTTP {response.status_code}: {e}")
                        raise
                    
                    # Añadir jitter si está habilitado
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)
                    
                    time.sleep(delay)
                    
                except (requests.RequestException, ConnectionError, TimeoutError) as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        print(f"Error de conexión después de {max_retries + 1} intentos: {e}")
                        raise
                    
                    # Para errores de conexión, usar delay normal
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    if jitter:
                        delay = delay * (0.5 + random.random() * 0.5)
                    
                    print(f"Error de conexión en intento {attempt + 1}: {e}. Reintentando en {delay:.2f} segundos...")
                    time.sleep(delay)
            
            raise last_exception
            
        return wrapper
    return decorator


@with_rate_limiting()
def make_aemet_request(url: str, description: str = "petición AEMET") -> requests.Response:
    """
    Función helper para hacer peticiones a AEMET con manejo de errores específicos.
    Usa connection pooling para optimizar las conexiones HTTP.
    """
    try:
        # Usar el connection pool en lugar de requests directo
        response = AEMET_CONNECTION_POOL.get(url, headers=HEADERS)
        
        # Log de la petición API
        log_api_request(logger, "AEMET", url, response.status_code, description=description)
        
        # Si es un error HTTP, raise la excepción para que el decorador la maneje
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

@retry_with_http_status_handling(max_retries=3, base_delay=2.0, max_delay=60.0)
def download_stations_data() -> Dict:
    """
    Descarga los datos de las estaciones de AEMET y los guarda localmente.
    Devuelve los datos de las estaciones como un diccionario.
    """
    url = f"{AEMET_BASE_URL}/valores/climatologicos/inventarioestaciones/todasestaciones"
    
    # Primera petición para obtener el endpoint de los datos
    response = make_aemet_request(url, "descarga de estaciones - primera petición")
    metadata = response.json()
    
    if metadata.get("estado") != 200:
        print(f"Error en la respuesta de AEMET: {metadata.get('descripcion')}")
        return {}
        
    # Segunda petición para obtener los datos reales
    data_url = metadata.get("datos")
    if not data_url:
        print("No se encontró la URL de los datos en la respuesta")
        return {}
        
    data_response = make_aemet_request(data_url, "descarga de estaciones - segunda petición")
    stations = data_response.json()
    
    # Create utils directory if it doesn't exist
    UTILS_PATH.mkdir(parents=True, exist_ok=True)
    
    # Save stations data
    with open(STATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(stations, f, ensure_ascii=False, indent=2)
        
    return stations

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

@retry_with_http_status_handling(max_retries=3, base_delay=2.0, max_delay=60.0)
def _fetch_data_batch(station_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Función interna para obtener datos de AEMET para un rango de fechas específico.
    """
    url = f"{AEMET_BASE_URL}/valores/climatologicos/diarios/datos/fechaini/{start_date}/fechafin/{end_date}/estacion/{station_id}"
    
    # Primera petición para obtener la URL de los datos
    response = make_aemet_request(url, f"primera petición para {station_id} ({start_date} - {end_date})")
    metadata = response.json()
    print(f"Respuesta de AEMET 1 para {station_id, start_date, end_date}: {metadata}")
    
    if metadata.get('estado') != 200:
        print(f"Error en la primera respuesta de AEMET: {metadata.get('descripcion')}")
        return pd.DataFrame()
        
    # Segunda petición para obtener los datos reales
    data_url = metadata.get('datos')
    if not data_url:
        print("Error antes de la segunda respuesta de AEMET: No se encontró URL de datos en la primera respuesta")
        return pd.DataFrame()
        
    # El rate limiter ya maneja los delays automáticamente
    data_response = make_aemet_request(data_url, f"segunda petición para {station_id} ({start_date} - {end_date})")
    raw_data = data_response.json()
    print(f"Respuesta de AEMET 2 para {station_id, start_date, end_date}: {data_response}")
    
    # Obtener coordenadas de la estación
    stations = load_stations_data()
    station_coords = None
    for station in stations:
        if station.get('indicativo') == station_id:
            station_coords = station
            break
    
    # Transformar datos al formato común
    validated_data = []
    for record in raw_data:
        try:
            def safe_float(value):
                if value is None or value == "" or value == "null" or value == "Ip":
                    return None
                return float(str(value).replace(",", "."))
            
            weather_data = {
                "date": datetime.strptime(record["fecha"], "%Y-%m-%d").date(),
                "tmax": safe_float(record.get("tmax")),
                "tmin": safe_float(record.get("tmin")),
                "tavg": safe_float(record.get("tmed")),
                "prcp": safe_float(record.get("prec")),
                "wdir": safe_float(record.get("dir")),
                "wspd": safe_float(record.get("velmedia")),
                "wpgt": safe_float(record.get("racha")),
                "pres": None,  # No existe en AEMET
                "snow": None,  # No existe en AEMET
                "tsun": None,  # No existe en AEMET
                "rhum": safe_float(record.get("hrMedia")),  # Humedad relativa media
                "station": station_id,
                "lat": dms_to_decimal(station_coords.get('latitud', '0')) if station_coords else None,
                "lon": dms_to_decimal(station_coords.get('longitud', '0')) if station_coords else None
            }
            validated_data.append(DailyWeatherDTO(**weather_data).dict())
        except Exception as e:
            print(f"Error al procesar registro: {e}")
            continue
    
    return pd.DataFrame(validated_data)

def fetch_daily_data(station_id: str, start: str, end: str) -> pd.DataFrame:
    """
    Obtiene datos diarios de una estación AEMET para un rango de fechas.
    Si el rango es mayor a 6 meses, descarga los datos en batches de 6 meses.
    """
    try:
        # Convertir fechas a datetime para cálculos
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        
        # Calcular diferencia en meses
        months_diff = (end_dt.year - start_dt.year) * 12 + end_dt.month - start_dt.month
        
        if months_diff <= 6:
            # Si es menos de 6 meses, descargar directamente
            start_date = start_dt.strftime("%Y-%m-%dT00:00:00UTC")
            end_date = end_dt.strftime("%Y-%m-%dT00:00:00UTC")
            return _fetch_data_batch(station_id, start_date, end_date)
        else:
            # Si es más de 6 meses, descargar en batches
            all_data = []
            current_start = start_dt
            
            while current_start < end_dt:
                # Calcular fecha final del batch (6 meses después o end_dt)
                current_end = min(
                    datetime(current_start.year + (current_start.month + 6) // 12,
                            (current_start.month + 6) % 12 or 12,
                            1) - timedelta(days=1),
                    end_dt
                )
                
                # Formatear fechas para la API
                batch_start = current_start.strftime("%Y-%m-%dT00:00:00UTC")
                batch_end = current_end.strftime("%Y-%m-%dT00:00:00UTC")
                
                # Descargar batch
                batch_data = _fetch_data_batch(station_id, batch_start, batch_end)
                if not batch_data.empty:
                    all_data.append(batch_data)
                
                # Mover al siguiente batch
                current_start = current_end + timedelta(days=1)
                
                # El rate limiter ya maneja los delays automáticamente
            
            # Combinar todos los batches
            return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
            
    except ValueError as e:
        print(f"Error en el formato de fechas: {e}")
        return pd.DataFrame() 