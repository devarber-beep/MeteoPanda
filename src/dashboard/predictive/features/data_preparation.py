"""
Preparación de datos para análisis predictivo meteorológico
Sistema robusto y escalable para preparar datos para Prophet y otros modelos ML
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class PredictionHorizon(Enum):
    """Horizontes de predicción disponibles"""
    SHORT_TERM = 7      # 7 días
    MEDIUM_TERM = 14    # 14 días  
    LONG_TERM = 30      # 30 días
    SEASONAL = 90       # 3 meses

@dataclass
class DataQualityMetrics:
    """Métricas de calidad de datos"""
    total_records: int
    missing_values: int
    missing_percentage: float
    date_range: Tuple[datetime, datetime]
    cities_count: int
    completeness_score: float

class WeatherDataPreparator:
    """
    Preparador de datos meteorológicos para análisis predictivo
    Maneja limpieza, validación y preparación de datos para Prophet
    """
    
    def __init__(self, min_data_points: int = 365, max_missing_percentage: float = 0.1):
        """
        Inicializar preparador de datos
        
        Args:
            min_data_points: Mínimo de puntos de datos requeridos
            max_missing_percentage: Máximo porcentaje de valores faltantes permitido
        """
        self.min_data_points = min_data_points
        self.max_missing_percentage = max_missing_percentage
        self.required_columns = [
            'date', 'city', 'temp_avg_c', 'temp_max_c', 'temp_min_c', 
            'precip_mm', 'humidity_percent', 'wind_avg_kmh', 'pressure_hpa'
        ]
        
    def prepare_data_for_prophet(self, 
                                data: pd.DataFrame, 
                                target_variable: str,
                                city: Optional[str] = None,
                                add_regressors: bool = True) -> pd.DataFrame:
        """
        Preparar datos para Prophet con formato específico
        
        Args:
            data: DataFrame con datos meteorológicos
            target_variable: Variable objetivo (temp_avg_c, precip_mm, etc.)
            city: Ciudad específica (None para todas)
            add_regressors: Si agregar variables adicionales como regresores
            
        Returns:
            DataFrame preparado para Prophet
        """
        try:
            logger.info(f"Preparando datos para Prophet - Variable: {target_variable}, Ciudad: {city}")
            
            # Filtrar por ciudad si se especifica
            if city:
                data = data[data['city'] == city].copy()
                if data.empty:
                    raise ValueError(f"No hay datos para la ciudad: {city}")
            
            # Validar datos
            self._validate_data(data, target_variable)
            
            # Preparar datos base
            prophet_data = self._prepare_base_data(data, target_variable)
            
            # Agregar regresores si se solicita
            if add_regressors:
                prophet_data = self._add_regressors(prophet_data, data)
            
            # Validar calidad final
            quality_metrics = self._calculate_quality_metrics(prophet_data, data)
            logger.info(f"Métricas de calidad: {quality_metrics}")
            
            if quality_metrics.completeness_score < 0.7:
                logger.warning(f"Calidad de datos baja: {quality_metrics.completeness_score:.2%}")
            
            return prophet_data
            
        except Exception as e:
            logger.error(f"Error preparando datos para Prophet: {str(e)}")
            raise
    
    def prepare_multi_city_data(self, 
                               data: pd.DataFrame, 
                               target_variable: str,
                               cities: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Preparar datos para múltiples ciudades
        
        Args:
            data: DataFrame con datos meteorológicos
            target_variable: Variable objetivo
            cities: Lista de ciudades
            
        Returns:
            Diccionario con datos preparados por ciudad
        """
        multi_city_data = {}
        
        for city in cities:
            try:
                city_data = self.prepare_data_for_prophet(data, target_variable, city)
                multi_city_data[city] = city_data
                logger.info(f"Datos preparados para {city}: {len(city_data)} registros")
            except Exception as e:
                logger.error(f"Error preparando datos para {city}: {str(e)}")
                continue
                
        return multi_city_data
    
    def create_forecast_dates(self, 
                            last_date: datetime, 
                            horizon: PredictionHorizon) -> pd.DataFrame:
        """
        Crear fechas para predicciones futuras
        
        Args:
            last_date: Última fecha de datos disponibles
            horizon: Horizonte de predicción
            
        Returns:
            DataFrame con fechas futuras
        """
        future_dates = pd.date_range(
            start=last_date + timedelta(days=1),
            periods=horizon.value,
            freq='D'
        )
        
        return pd.DataFrame({'ds': future_dates})
    
    def _validate_data(self, data: pd.DataFrame, target_variable: str) -> None:
        """Validar datos de entrada"""
        if data.empty:
            raise ValueError("DataFrame vacío")
        
        if target_variable not in data.columns:
            raise ValueError(f"Variable objetivo '{target_variable}' no encontrada")
        
        if 'date' not in data.columns:
            raise ValueError("Columna 'date' no encontrada")
        
        # Verificar suficientes datos (más flexible para datos mensuales)
        min_required = min(self.min_data_points, 24)  # Al menos 24 meses (2 años)
        if len(data) < min_required:
            raise ValueError(f"Datos insuficientes: {len(data)} < {min_required}")
        
        # Verificar valores faltantes en variable objetivo
        missing_target = data[target_variable].isna().sum()
        missing_percentage = missing_target / len(data)
        
        if missing_percentage > self.max_missing_percentage:
            raise ValueError(f"Demasiados valores faltantes en {target_variable}: {missing_percentage:.2%}")
    
    def _prepare_base_data(self, data: pd.DataFrame, target_variable: str) -> pd.DataFrame:
        """Preparar datos base para Prophet"""
        # Ordenar por fecha
        data_sorted = data.sort_values('date').copy()
        
        # Crear DataFrame para Prophet
        prophet_data = pd.DataFrame({
            'ds': pd.to_datetime(data_sorted['date']),
            'y': data_sorted[target_variable]
        })
        
        # Eliminar valores faltantes
        prophet_data = prophet_data.dropna()
        
        # Verificar que tenemos suficientes datos después de limpiar (más flexible para datos mensuales)
        min_required = min(self.min_data_points, 24)  # Al menos 24 meses (2 años)
        if len(prophet_data) < min_required:
            raise ValueError(f"Datos insuficientes después de limpieza: {len(prophet_data)}")
        
        return prophet_data
    
    def _add_regressors(self, prophet_data: pd.DataFrame, original_data: pd.DataFrame) -> pd.DataFrame:
        """Agregar regresores adicionales"""
        # Por ahora, no agregar regresores adicionales para evitar problemas de merge
        # Los datos de Gold ya tienen las variables principales
        return prophet_data
    
    def _calculate_quality_metrics(self, prophet_data: pd.DataFrame, original_data: pd.DataFrame) -> DataQualityMetrics:
        """Calcular métricas de calidad de datos"""
        total_records = len(original_data)
        missing_values = prophet_data['y'].isna().sum()
        missing_percentage = missing_values / len(prophet_data) if len(prophet_data) > 0 else 1.0
        
        date_range = (
            prophet_data['ds'].min(),
            prophet_data['ds'].max()
        )
        
        cities_count = original_data['city'].nunique()
        completeness_score = 1.0 - missing_percentage
        
        return DataQualityMetrics(
            total_records=total_records,
            missing_values=missing_values,
            missing_percentage=missing_percentage,
            date_range=date_range,
            cities_count=cities_count,
            completeness_score=completeness_score
        )
    
    def get_data_summary(self, data: pd.DataFrame) -> Dict:
        """Obtener resumen de datos disponibles"""
        summary = {
            'total_records': len(data),
            'date_range': (data['date'].min(), data['date'].max()),
            'cities': data['city'].unique().tolist(),
            'cities_count': data['city'].nunique(),
            'variables': data.columns.tolist(),
            'missing_values': data.isnull().sum().to_dict()
        }
        
        return summary
