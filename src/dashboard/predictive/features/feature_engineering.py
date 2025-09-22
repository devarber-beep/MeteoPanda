"""
Ingeniería de características avanzada para análisis predictivo meteorológico
Sistema robusto para crear features que mejoren la precisión de Prophet
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class FeatureType(Enum):
    """Tipos de características disponibles"""
    TEMPORAL = "temporal"
    CYCLICAL = "cyclical"
    LAGGED = "lagged"
    ROLLING = "rolling"
    INTERACTION = "interaction"
    WEATHER_SPECIFIC = "weather_specific"

@dataclass
class FeatureConfig:
    """Configuración para generación de características"""
    enable_temporal: bool = True
    enable_cyclical: bool = True
    enable_lagged: bool = True
    enable_rolling: bool = True
    enable_interactions: bool = True
    enable_weather_specific: bool = True
    max_lag_days: int = 7
    rolling_windows: List[int] = None
    
    def __post_init__(self):
        if self.rolling_windows is None:
            self.rolling_windows = [3, 7, 14, 30]

class WeatherFeatureEngineer:
    """
    Ingeniero de características especializado en datos meteorológicos
    Crea features avanzadas para mejorar predicciones de Prophet
    """
    
    def __init__(self, config: Optional[FeatureConfig] = None):
        """
        Inicializar ingeniero de características
        
        Args:
            config: Configuración de características a generar
        """
        self.config = config or FeatureConfig()
        self.feature_history = []
        
    def engineer_features(self, data: pd.DataFrame, target_variable: str) -> pd.DataFrame:
        """
        Aplicar ingeniería de características completa
        
        Args:
            data: DataFrame con datos meteorológicos
            target_variable: Variable objetivo
            
        Returns:
            DataFrame con características adicionales
        """
        try:
            logger.info(f"Aplicando ingeniería de características para {target_variable}")
            
            # Crear copia para no modificar original
            enhanced_data = data.copy()
            
            # Asegurar que date es datetime
            enhanced_data['date'] = pd.to_datetime(enhanced_data['date'])
            
            # Aplicar características por tipo
            if self.config.enable_temporal:
                enhanced_data = self._add_temporal_features(enhanced_data)
                
            if self.config.enable_cyclical:
                enhanced_data = self._add_cyclical_features(enhanced_data)
                
            if self.config.enable_lagged:
                enhanced_data = self._add_lagged_features(enhanced_data, target_variable)
                
            if self.config.enable_rolling:
                enhanced_data = self._add_rolling_features(enhanced_data, target_variable)
                
            if self.config.enable_interactions:
                enhanced_data = self._add_interaction_features(enhanced_data)
                
            if self.config.enable_weather_specific:
                enhanced_data = self._add_weather_specific_features(enhanced_data)
            
            # Registrar características creadas
            new_features = [col for col in enhanced_data.columns if col not in data.columns]
            self.feature_history.extend(new_features)
            logger.info(f"Características creadas: {len(new_features)}")
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"Error en ingeniería de características: {str(e)}")
            raise
    
    def _add_temporal_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Agregar características temporales"""
        data = data.copy()
        
        # Año, mes, día, día de la semana
        data['year'] = data['date'].dt.year
        data['month'] = data['date'].dt.month
        data['day'] = data['date'].dt.day
        data['dayofweek'] = data['date'].dt.dayofweek
        data['dayofyear'] = data['date'].dt.dayofyear
        data['week'] = data['date'].dt.isocalendar().week
        data['quarter'] = data['date'].dt.quarter
        
        # Días desde inicio de año
        data['days_from_year_start'] = data['dayofyear']
        
        # Días hasta fin de año
        data['days_to_year_end'] = 365 - data['dayofyear']
        
        return data
    
    def _add_cyclical_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Agregar características cíclicas para estacionalidad"""
        data = data.copy()
        
        # Características cíclicas para mes (estacionalidad anual)
        data['month_sin'] = np.sin(2 * np.pi * data['month'] / 12)
        data['month_cos'] = np.cos(2 * np.pi * data['month'] / 12)
        
        # Características cíclicas para día del año
        data['dayofyear_sin'] = np.sin(2 * np.pi * data['dayofyear'] / 365)
        data['dayofyear_cos'] = np.cos(2 * np.pi * data['dayofyear'] / 365)
        
        # Características cíclicas para día de la semana
        data['dayofweek_sin'] = np.sin(2 * np.pi * data['dayofweek'] / 7)
        data['dayofweek_cos'] = np.cos(2 * np.pi * data['dayofweek'] / 7)
        
        # Características cíclicas para hora (si estuviera disponible)
        # data['hour_sin'] = np.sin(2 * np.pi * data['hour'] / 24)
        # data['hour_cos'] = np.cos(2 * np.pi * data['hour'] / 24)
        
        return data
    
    def _add_lagged_features(self, data: pd.DataFrame, target_variable: str) -> pd.DataFrame:
        """Agregar características de retraso (lag features)"""
        data = data.copy()
        
        # Ordenar por fecha y ciudad
        data = data.sort_values(['city', 'date'])
        
        # Crear lags para la variable objetivo
        for lag in range(1, self.config.max_lag_days + 1):
            data[f'{target_variable}_lag_{lag}'] = data.groupby('city')[target_variable].shift(lag)
        
        # Crear lags para otras variables meteorológicas importantes
        weather_vars = ['temp_avg_c', 'precip_mm', 'humidity_percent', 'pressure_hpa']
        
        for var in weather_vars:
            if var in data.columns and var != target_variable:
                for lag in [1, 2, 3, 7]:  # Lags específicos
                    data[f'{var}_lag_{lag}'] = data.groupby('city')[var].shift(lag)
        
        return data
    
    def _add_rolling_features(self, data: pd.DataFrame, target_variable: str) -> pd.DataFrame:
        """Agregar características de ventana deslizante"""
        data = data.copy()
        
        # Ordenar por fecha y ciudad
        data = data.sort_values(['city', 'date'])
        
        # Estadísticas de ventana deslizante para variable objetivo
        for window in self.config.rolling_windows:
            data[f'{target_variable}_rolling_mean_{window}'] = (
                data.groupby('city')[target_variable]
                .rolling(window=window, min_periods=1)
                .mean()
                .reset_index(0, drop=True)
            )
            
            data[f'{target_variable}_rolling_std_{window}'] = (
                data.groupby('city')[target_variable]
                .rolling(window=window, min_periods=1)
                .std()
                .reset_index(0, drop=True)
            )
            
            data[f'{target_variable}_rolling_max_{window}'] = (
                data.groupby('city')[target_variable]
                .rolling(window=window, min_periods=1)
                .max()
                .reset_index(0, drop=True)
            )
            
            data[f'{target_variable}_rolling_min_{window}'] = (
                data.groupby('city')[target_variable]
                .rolling(window=window, min_periods=1)
                .min()
                .reset_index(0, drop=True)
            )
        
        return data
    
    def _add_interaction_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Agregar características de interacción"""
        data = data.copy()
        
        # Interacciones entre variables meteorológicas
        if 'temp_avg_c' in data.columns and 'humidity_percent' in data.columns:
            data['temp_humidity_interaction'] = data['temp_avg_c'] * data['humidity_percent']
        
        if 'temp_avg_c' in data.columns and 'pressure_hpa' in data.columns:
            data['temp_pressure_interaction'] = data['temp_avg_c'] * data['pressure_hpa']
        
        if 'precip_mm' in data.columns and 'wind_avg_kmh' in data.columns:
            data['precip_wind_interaction'] = data['precip_mm'] * data['wind_avg_kmh']
        
        # Interacciones con características temporales
        if 'temp_avg_c' in data.columns:
            data['temp_month_interaction'] = data['temp_avg_c'] * data['month']
            data['temp_dayofyear_interaction'] = data['temp_avg_c'] * data['dayofyear']
        
        return data
    
    def _add_weather_specific_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Agregar características específicas del clima"""
        data = data.copy()
        
        # Características de temperatura
        if 'temp_avg_c' in data.columns:
            data['temp_above_30'] = (data['temp_avg_c'] > 30).astype(int)
            data['temp_below_0'] = (data['temp_avg_c'] < 0).astype(int)
            data['temp_comfortable'] = ((data['temp_avg_c'] >= 18) & (data['temp_avg_c'] <= 25)).astype(int)
        
        # Características de precipitación
        if 'precip_mm' in data.columns:
            data['is_rainy'] = (data['precip_mm'] > 0).astype(int)
            data['heavy_rain'] = (data['precip_mm'] > 10).astype(int)
            data['precip_squared'] = data['precip_mm'] ** 2
        
        # Características de humedad
        if 'humidity_percent' in data.columns:
            data['high_humidity'] = (data['humidity_percent'] > 80).astype(int)
            data['low_humidity'] = (data['humidity_percent'] < 30).astype(int)
        
        # Características de viento
        if 'wind_avg_kmh' in data.columns:
            data['windy'] = (data['wind_avg_kmh'] > 20).astype(int)
            data['very_windy'] = (data['wind_avg_kmh'] > 40).astype(int)
        
        # Características de presión
        if 'pressure_hpa' in data.columns:
            data['pressure_anomaly'] = data['pressure_hpa'] - data['pressure_hpa'].rolling(30).mean()
            data['low_pressure'] = (data['pressure_hpa'] < 1013).astype(int)
            data['high_pressure'] = (data['pressure_hpa'] > 1020).astype(int)
        
        # Características estacionales específicas
        data['is_summer'] = data['month'].isin([6, 7, 8]).astype(int)
        data['is_winter'] = data['month'].isin([12, 1, 2]).astype(int)
        data['is_spring'] = data['month'].isin([3, 4, 5]).astype(int)
        data['is_autumn'] = data['month'].isin([9, 10, 11]).astype(int)
        
        return data
    
    def get_feature_importance(self, data: pd.DataFrame, target_variable: str) -> Dict[str, float]:
        """
        Calcular importancia de características usando correlación
        
        Args:
            data: DataFrame con características
            target_variable: Variable objetivo
            
        Returns:
            Diccionario con importancia de características
        """
        if target_variable not in data.columns:
            return {}
        
        # Calcular correlaciones
        correlations = data.corr()[target_variable].abs().sort_values(ascending=False)
        
        # Excluir la variable objetivo y características no numéricas
        feature_importance = {}
        for feature, corr in correlations.items():
            if feature != target_variable and pd.api.types.is_numeric_dtype(data[feature]):
                feature_importance[feature] = corr
        
        return feature_importance
    
    def select_top_features(self, data: pd.DataFrame, target_variable: str, top_n: int = 20) -> List[str]:
        """
        Seleccionar las características más importantes
        
        Args:
            data: DataFrame con características
            target_variable: Variable objetivo
            top_n: Número de características a seleccionar
            
        Returns:
            Lista de características seleccionadas
        """
        importance = self.get_feature_importance(data, target_variable)
        top_features = list(importance.keys())[:top_n]
        
        logger.info(f"Características seleccionadas: {len(top_features)}")
        return top_features
    
    def get_feature_summary(self) -> Dict:
        """Obtener resumen de características creadas"""
        return {
            'total_features_created': len(self.feature_history),
            'feature_types': {
                'temporal': len([f for f in self.feature_history if 'year' in f or 'month' in f or 'day' in f]),
                'cyclical': len([f for f in self.feature_history if 'sin' in f or 'cos' in f]),
                'lagged': len([f for f in self.feature_history if 'lag' in f]),
                'rolling': len([f for f in self.feature_history if 'rolling' in f]),
                'interaction': len([f for f in self.feature_history if 'interaction' in f]),
                'weather_specific': len([f for f in self.feature_history if any(x in f for x in ['temp_', 'precip_', 'humidity_', 'wind_', 'pressure_'])])
            },
            'features_list': self.feature_history
        }
