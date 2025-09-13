from typing import Optional, Dict, Any, List
from datetime import date, datetime
from pydantic import BaseModel
from enum import Enum

# =============================================================================
# DTOs EXISTENTES (MANTENIDOS)
# =============================================================================

class DailyWeatherDTO(BaseModel):
    date: date
    tavg: Optional[float] = None
    tmin: Optional[float] = None
    tmax: Optional[float] = None
    prcp: Optional[float] = None
    wdir: Optional[float] = None
    wspd: Optional[float] = None
    wpgt: Optional[float] = None
    pres: Optional[float] = None
    snow: Optional[float] = None
    tsun: Optional[float] = None
    rhum: Optional[float] = None
    station: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None

class CityConfigDTO(BaseModel):
    name: str
    latitude: float
    longitude: float
    elevation: float
    region: str

# =============================================================================
# NUEVOS DTOs PARA DESACOPLAMIENTO
# =============================================================================

class DataSourceType(str, Enum):
    """Tipos de fuentes de datos"""
    AEMET = "aemet"
    METEOSTAT = "meteostat"
    UNKNOWN = "unknown"

class WeatherDataDTO(BaseModel):
    """DTO para datos meteorológicos estandarizados"""
    date: date
    city: str
    region: str
    source: DataSourceType
    station: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    
    # Temperaturas
    avg_temp: Optional[float] = None
    max_temp: Optional[float] = None
    min_temp: Optional[float] = None
    
    # Precipitación
    precip_mm: Optional[float] = None
    total_precip: Optional[float] = None
    
    # Viento
    wind_speed: Optional[float] = None
    wind_direction: Optional[float] = None
    wind_gust: Optional[float] = None
    
    # Otros
    humidity_percent: Optional[float] = None
    pressure_avg: Optional[float] = None
    pressure_max: Optional[float] = None
    pressure_min: Optional[float] = None
    sunshine_hours: Optional[float] = None
    snow_mm: Optional[float] = None

class FilterCriteriaDTO(BaseModel):
    """DTO para criterios de filtrado"""
    year: Optional[int] = None
    month: Optional[int] = None
    region: Optional[str] = None
    cities: Optional[List[str]] = None
    season: Optional[str] = None
    alert_level: Optional[str] = None
    min_temp: Optional[float] = None
    max_temp: Optional[float] = None
    max_precip: Optional[float] = None
    source: Optional[DataSourceType] = None
    date_range: Optional[tuple[datetime, datetime]] = None

class MapConfigDTO(BaseModel):
    """DTO para configuración de mapas"""
    center_lat: float = 40.4168
    center_lon: float = -3.7038
    zoom: int = 6
    height: int = 600
    map_type: str = "temperature"
    metric: str = "avg_temp"

class ChartConfigDTO(BaseModel):
    """DTO para configuración de gráficos"""
    template: str = "plotly_white"
    color_palette: str = "Set3"
    height: int = 400
    show_legend: bool = True
    show_grid: bool = True

class TableConfigDTO(BaseModel):
    """DTO para configuración de tablas"""
    items_per_page: int = 10
    show_pagination: bool = True
    show_export: bool = True
    show_search: bool = True
    show_stats: bool = True

class DatabaseConfigDTO(BaseModel):
    """DTO para configuración de base de datos"""
    db_path: str = "meteopanda.duckdb"
    connection_timeout: int = 30
    query_timeout: int = 60
    max_connections: int = 10

class CacheConfigDTO(BaseModel):
    """DTO para configuración de caché"""
    ttl: int = 7200  # 2 horas
    max_size: int = 1000
    enabled: bool = True

class UIConfigDTO(BaseModel):
    """DTO para configuración de interfaz"""
    page_title: str = "MeteoPanda Dashboard"
    page_icon: str = "🌦️"
    layout: str = "wide"
    sidebar_state: str = "expanded"
    theme: str = "light"

class SystemConfigDTO(BaseModel):
    """DTO para configuración general del sistema"""
    database: DatabaseConfigDTO
    cache: CacheConfigDTO
    ui: UIConfigDTO
    map: MapConfigDTO
    chart: ChartConfigDTO
    table: TableConfigDTO
