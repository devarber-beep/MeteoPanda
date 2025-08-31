from typing import Optional
from datetime import date
from pydantic import BaseModel


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
