"""
Modelo Prophet especializado para predicciones meteorológicas
Implementación robusta y optimizada para datos climáticos
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

# Aplicar parche de compatibilidad con NumPy 2.0 antes de importar Prophet
from ..numpy_patch import apply_numpy_patch
apply_numpy_patch()

try:
    from prophet import Prophet
    from prophet.plot import plot_plotly, plot_components_plotly
    from prophet.diagnostics import cross_validation, performance_metrics
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    Prophet = None
    plot_plotly = None
    plot_components_plotly = None
    go = None
    make_subplots = None
    # Crear clases dummy para las anotaciones de tipo
    class DummyFigure:
        pass
    go = type('PlotlyModule', (), {'Figure': DummyFigure})()

logger = logging.getLogger(__name__)

class SeasonalityMode(Enum):
    """Modos de estacionalidad para Prophet"""
    ADDITIVE = "additive"
    MULTIPLICATIVE = "multiplicative"

@dataclass
class ProphetConfig:
    """Configuración avanzada para Prophet"""
    # Estacionalidad
    yearly_seasonality: bool = True
    weekly_seasonality: bool = True
    daily_seasonality: bool = False
    seasonality_mode: SeasonalityMode = SeasonalityMode.ADDITIVE
    
    # Componentes
    growth: str = "linear"
    changepoint_prior_scale: float = 0.05
    seasonality_prior_scale: float = 10.0
    holidays_prior_scale: float = 10.0
    
    # Intervalos de confianza
    interval_width: float = 0.95
    
    # Regresores
    add_regressors: bool = True
    
    # Optimización
    mcmc_samples: int = 0  # 0 para MAP, >0 para MCMC
    uncertainty_samples: int = 1000

class WeatherProphetModel:
    """
    Modelo Prophet especializado para predicciones meteorológicas
    Implementación robusta con características específicas para clima
    """
    
    def __init__(self, config: Optional[ProphetConfig] = None):
        """
        Inicializar modelo Prophet
        
        Args:
            config: Configuración del modelo Prophet
        """
        if Prophet is None:
            raise ImportError("Prophet no está instalado. Instala con: pip install prophet")
        
        self.config = config or ProphetConfig()
        self.model = None
        self.is_trained = False
        self.training_data = None
        self.feature_columns = []
        self.model_metrics = {}
        
    def train(self, 
              data: pd.DataFrame, 
              target_variable: str = 'y',
              regressors: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Entrenar modelo Prophet con datos meteorológicos
        
        Args:
            data: DataFrame con datos preparados para Prophet
            target_variable: Nombre de la variable objetivo
            regressors: Lista de regresores adicionales
            
        Returns:
            Diccionario con métricas de entrenamiento
        """
        try:
            logger.info(f"Entrenando modelo Prophet para {target_variable}")
            
            # Validar datos
            self._validate_training_data(data, target_variable)
            
            # Configurar modelo Prophet
            self.model = self._configure_prophet_model(regressors)
            
            # Preparar datos de entrenamiento
            training_data = self._prepare_training_data(data, target_variable, regressors)
            self.training_data = training_data.copy()
            
            # Entrenar modelo
            logger.info("Iniciando entrenamiento del modelo...")
            self.model.fit(training_data)
            
            # Marcar como entrenado
            self.is_trained = True
            
            # Calcular métricas de entrenamiento
            metrics = self._calculate_training_metrics(training_data)
            self.model_metrics = metrics
            
            logger.info(f"Modelo entrenado exitosamente. RMSE: {metrics.get('rmse', 'N/A'):.4f}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error entrenando modelo Prophet: {str(e)}")
            raise
    
    def predict(self, 
                periods: int = 30,
                future_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Realizar predicciones con el modelo entrenado
        
        Args:
            periods: Número de períodos a predecir
            future_data: DataFrame con datos futuros (regresores)
            
        Returns:
            DataFrame con predicciones
        """
        if not self.is_trained:
            raise ValueError("Modelo no entrenado. Llama a train() primero.")
        
        try:
            logger.info(f"Generando predicciones para {periods} períodos")
            
            # Crear DataFrame futuro
            if future_data is not None:
                future_df = future_data.copy()
            else:
                future_df = self.model.make_future_dataframe(periods=periods)
            
            # Realizar predicción
            forecast = self.model.predict(future_df)
            
            # Agregar información adicional
            forecast = self._enhance_forecast(forecast)
            
            logger.info("Predicciones generadas exitosamente")
            return forecast
            
        except Exception as e:
            logger.error(f"Error generando predicciones: {str(e)}")
            raise
    
    def cross_validate(self, 
                      initial: str = '365 days',
                      period: str = '30 days',
                      horizon: str = '30 days') -> pd.DataFrame:
        """
        Realizar validación cruzada del modelo
        
        Args:
            initial: Período inicial de entrenamiento
            period: Período entre validaciones
            horizon: Horizonte de predicción
            
        Returns:
            DataFrame con métricas de validación cruzada
        """
        if not self.is_trained:
            raise ValueError("Modelo no entrenado. Llama a train() primero.")
        
        try:
            logger.info("Realizando validación cruzada...")
            
            # Validación cruzada
            df_cv = cross_validation(
                self.model,
                initial=initial,
                period=period,
                horizon=horizon
            )
            
            # Métricas de rendimiento
            metrics = performance_metrics(df_cv)
            
            logger.info("Validación cruzada completada")
            return metrics
            
        except Exception as e:
            logger.error(f"Error en validación cruzada: {str(e)}")
            raise
    
    def get_forecast_components(self, forecast: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Extraer componentes de la predicción
        
        Args:
            forecast: DataFrame con predicciones
            
        Returns:
            Diccionario con componentes separados
        """
        components = {
            'trend': forecast[['ds', 'trend']].copy(),
            'yearly': forecast[['ds', 'yearly']].copy() if 'yearly' in forecast.columns else None,
            'weekly': forecast[['ds', 'weekly']].copy() if 'weekly' in forecast.columns else None,
            'daily': forecast[['ds', 'daily']].copy() if 'daily' in forecast.columns else None,
            'holidays': forecast[['ds', 'holidays']].copy() if 'holidays' in forecast.columns else None
        }
        
        # Filtrar componentes nulos
        components = {k: v for k, v in components.items() if v is not None}
        
        return components
    
    def plot_forecast(self, 
                     forecast: pd.DataFrame, 
                     title: str = "Predicción Meteorológica",
                     show_components: bool = True) -> go.Figure:
        """
        Crear visualización de la predicción
        
        Args:
            forecast: DataFrame con predicciones
            title: Título del gráfico
            show_components: Si mostrar componentes
            
        Returns:
            Figura de Plotly
        """
        if go is None or plot_plotly is None:
            raise ImportError("Plotly no está disponible para visualización")
        
        try:
            # Gráfico principal
            fig = plot_plotly(self.model, forecast)
            fig.update_layout(title=title)
            
            # Agregar componentes si se solicita
            if show_components and plot_components_plotly is not None:
                components_fig = plot_components_plotly(self.model, forecast)
                # Aquí podrías combinar ambos gráficos si es necesario
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creando visualización: {str(e)}")
            raise
    
    def get_model_summary(self) -> Dict[str, Any]:
        """Obtener resumen del modelo"""
        if not self.is_trained:
            return {"status": "No entrenado"}
        
        summary = {
            "status": "Entrenado",
            "training_records": len(self.training_data) if self.training_data is not None else 0,
            "feature_columns": self.feature_columns,
            "config": {
                "yearly_seasonality": self.config.yearly_seasonality,
                "weekly_seasonality": self.config.weekly_seasonality,
                "seasonality_mode": self.config.seasonality_mode.value,
                "growth": self.config.growth
            },
            "metrics": self.model_metrics
        }
        
        return summary
    
    def _validate_training_data(self, data: pd.DataFrame, target_variable: str) -> None:
        """Validar datos de entrenamiento"""
        if data.empty:
            raise ValueError("Datos de entrenamiento vacíos")
        
        if target_variable not in data.columns:
            raise ValueError(f"Variable objetivo '{target_variable}' no encontrada")
        
        if 'ds' not in data.columns:
            raise ValueError("Columna 'ds' (fecha) no encontrada")
        
        if len(data) < 30:
            raise ValueError("Datos insuficientes para entrenamiento (mínimo 30 registros)")
    
    def _configure_prophet_model(self, regressors: Optional[List[str]] = None) -> Prophet:
        """Configurar modelo Prophet"""
        model_params = {
            'yearly_seasonality': self.config.yearly_seasonality,
            'weekly_seasonality': self.config.weekly_seasonality,
            'daily_seasonality': self.config.daily_seasonality,
            'seasonality_mode': self.config.seasonality_mode.value,
            'growth': self.config.growth,
            'changepoint_prior_scale': self.config.changepoint_prior_scale,
            'seasonality_prior_scale': self.config.seasonality_prior_scale,
            'holidays_prior_scale': self.config.holidays_prior_scale,
            'interval_width': self.config.interval_width,
            'mcmc_samples': self.config.mcmc_samples,
            'uncertainty_samples': self.config.uncertainty_samples
        }
        
        model = Prophet(**model_params)
        
        # Agregar regresores si se especifican
        if regressors and self.config.add_regressors:
            for regressor in regressors:
                model.add_regressor(regressor)
                self.feature_columns.append(regressor)
        
        return model
    
    def _prepare_training_data(self, 
                              data: pd.DataFrame, 
                              target_variable: str,
                              regressors: Optional[List[str]] = None) -> pd.DataFrame:
        """Preparar datos para entrenamiento"""
        training_data = data[['ds', target_variable]].copy()
        
        # Agregar regresores si existen
        if regressors:
            for regressor in regressors:
                if regressor in data.columns:
                    training_data[regressor] = data[regressor]
        
        # Eliminar valores faltantes
        training_data = training_data.dropna()
        
        return training_data
    
    def _calculate_training_metrics(self, training_data: pd.DataFrame) -> Dict[str, float]:
        """Calcular métricas de entrenamiento"""
        try:
            # Predicción en datos de entrenamiento
            forecast = self.model.predict(training_data)
            
            # Calcular métricas
            actual = training_data['y'].values
            predicted = forecast['yhat'].values
            
            # RMSE
            rmse = np.sqrt(np.mean((actual - predicted) ** 2))
            
            # MAE
            mae = np.mean(np.abs(actual - predicted))
            
            # MAPE
            mape = np.mean(np.abs((actual - predicted) / actual)) * 100
            
            # R²
            ss_res = np.sum((actual - predicted) ** 2)
            ss_tot = np.sum((actual - np.mean(actual)) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
            
            return {
                'rmse': rmse,
                'mae': mae,
                'mape': mape,
                'r2': r2
            }
            
        except Exception as e:
            logger.warning(f"Error calculando métricas de entrenamiento: {str(e)}")
            return {}
    
    def _enhance_forecast(self, forecast: pd.DataFrame) -> pd.DataFrame:
        """Mejorar DataFrame de predicción con información adicional"""
        forecast = forecast.copy()
        
        # Agregar información de fecha
        forecast['date'] = forecast['ds'].dt.date
        forecast['year'] = forecast['ds'].dt.year
        forecast['month'] = forecast['ds'].dt.month
        forecast['day'] = forecast['ds'].dt.day
        forecast['dayofweek'] = forecast['ds'].dt.dayofweek
        
        # Agregar información de confianza
        forecast['confidence_interval_width'] = forecast['yhat_upper'] - forecast['yhat_lower']
        forecast['prediction_uncertainty'] = forecast['confidence_interval_width'] / forecast['yhat']
        
        return forecast
