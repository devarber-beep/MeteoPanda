from typing import Optional
from datetime import date
from pydantic import BaseModel


class DailyWeatherDTO(BaseModel):
    date: date
    tavg: Optional[float]
    tmin: Optional[float]
    tmax: Optional[float]
    prcp: Optional[float]
    wdir: Optional[float]
    wspd: Optional[float]
    wpgt: Optional[float]
    pres: Optional[float]
    snow: Optional[float]
    tsun: Optional[float]


class CityConfigDTO(BaseModel):
    name: str
    latitude: float
    longitude: float
    elevation: float
