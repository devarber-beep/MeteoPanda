"""
Gestor principal del sistema de análisis predictivo meteorológico
Orquesta todos los componentes para proporcionar predicciones robustas
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

from .features import WeatherFeatureEngineer, WeatherDataPreparator, FeatureConfig, PredictionHorizon
from .models import WeatherProphetModel, ModelEvaluator, ProphetConfig, EvaluationConfig
from .visualizations import WeatherPredictionCharts, ChartTheme

logger = logging.getLogger(__name__)

class PredictionType(Enum):
    """Tipos de predicción disponibles"""
    TEMPERATURE = "temperature"
    PRECIPITATION = "precipitation"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"
    WIND = "wind"

@dataclass
class PredictionRequest:
    """Solicitud de predicción"""
    prediction_type: PredictionType
    city: Optional[str] = None
    horizon: PredictionHorizon = PredictionHorizon.MEDIUM_TERM
    include_confidence: bool = True
    add_regressors: bool = True
    custom_features: Optional[List[str]] = None

@dataclass
class PredictionResult:
    """Resultado de predicción"""
    predictions: pd.DataFrame
    model_metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    confidence_intervals: Optional[pd.DataFrame] = None
    components: Optional[Dict[str, pd.DataFrame]] = None
    alerts: Optional[List[Dict[str, Any]]] = None

class WeatherPredictiveManager:
    """
    Gestor principal del sistema de análisis predictivo meteorológico
    Proporciona interfaz unificada para todas las funcionalidades predictivas
    """
    
    def __init__(self, 
                 data_manager=None,
                 feature_config: Optional[FeatureConfig] = None,
                 prophet_config: Optional[ProphetConfig] = None,
                 evaluation_config: Optional[EvaluationConfig] = None):
        """
        Inicializar gestor predictivo
        
        Args:
            data_manager: Gestor de datos del dashboard
            feature_config: Configuración de características
            prophet_config: Configuración de Prophet
            evaluation_config: Configuración de evaluación
        """
        self.data_manager = data_manager
        
        # Inicializar componentes
        self.feature_engineer = WeatherFeatureEngineer(feature_config)
        self.data_preparator = WeatherDataPreparator()
        self.prophet_model = WeatherProphetModel(prophet_config)
        self.model_evaluator = ModelEvaluator(evaluation_config)
        self.chart_generator = WeatherPredictionCharts(ChartTheme.WEATHER)
        
        # Cache de modelos entrenados
        self.trained_models = {}
        self.prediction_cache = {}
        
        # Configuraciones
        self.feature_config = feature_config or FeatureConfig()
        self.prophet_config = prophet_config or ProphetConfig()
        
        # Umbrales de alerta por tipo de predicción
        self.alert_thresholds = {
            PredictionType.TEMPERATURE: {
                'heat_wave': 35.0,
                'cold_wave': 0.0,
                'extreme_heat': 40.0,
                'extreme_cold': -5.0
            },
            PredictionType.PRECIPITATION: {
                'heavy_rain': 20.0,
                'extreme_rain': 50.0,
                'flood_risk': 100.0
            },
            PredictionType.HUMIDITY: {
                'high_humidity': 90.0,
                'low_humidity': 20.0
            }
        }
    
    def predict(self, request: PredictionRequest) -> PredictionResult:
        """
        Realizar predicción meteorológica
        
        Args:
            request: Solicitud de predicción
            
        Returns:
            Resultado de predicción
        """
        try:
            logger.info(f"Iniciando predicción: {request.prediction_type.value} para {request.city or 'todas las ciudades'}")
            
            # Obtener datos
            data = self._get_data_for_prediction(request)
            if data.empty:
                raise ValueError("No hay datos disponibles para la predicción")
            
            # Preparar datos
            prepared_data = self._prepare_data_for_prediction(data, request)
            
            # Entrenar modelo si es necesario
            model_key = self._get_model_key(request)
            if model_key not in self.trained_models:
                self._train_model(prepared_data, request, model_key)
            
            # Generar predicción
            predictions = self._generate_prediction(request, model_key)
            
            # Calcular métricas
            metrics = self._calculate_prediction_metrics(prepared_data, predictions)
            
            # Obtener importancia de características
            feature_importance = self._get_feature_importance(prepared_data, request)
            
            # Generar alertas si es necesario
            alerts = self._generate_alerts(predictions, request)
            
            # Crear resultado
            result = PredictionResult(
                predictions=predictions,
                model_metrics=metrics,
                feature_importance=feature_importance,
                confidence_intervals=self._extract_confidence_intervals(predictions),
                components=self._extract_components(predictions),
                alerts=alerts
            )
            
            # Guardar en caché
            self.prediction_cache[model_key] = result
            
            logger.info(f"Predicción completada exitosamente: {len(predictions)} días")
            return result
            
        except Exception as e:
            logger.error(f"Error en predicción: {str(e)}")
            raise
    
    def predict_multiple_cities(self, 
                              prediction_type: PredictionType,
                              cities: List[str],
                              horizon: PredictionHorizon = PredictionHorizon.MEDIUM_TERM) -> Dict[str, PredictionResult]:
        """
        Realizar predicciones para múltiples ciudades
        
        Args:
            prediction_type: Tipo de predicción
            cities: Lista de ciudades
            horizon: Horizonte de predicción
            
        Returns:
            Diccionario con resultados por ciudad
        """
        results = {}
        
        for city in cities:
            try:
                request = PredictionRequest(
                    prediction_type=prediction_type,
                    city=city,
                    horizon=horizon
                )
                
                result = self.predict(request)
                results[city] = result
                
                logger.info(f"Predicción completada para {city}")
                
            except Exception as e:
                logger.error(f"Error prediciendo para {city}: {str(e)}")
                continue
        
        return results
    
    def get_prediction_chart(self, 
                           result: PredictionResult,
                           historical_data: Optional[pd.DataFrame] = None,
                           chart_type: str = "forecast") -> Any:
        """
        Obtener gráfico de predicción
        
        Args:
            result: Resultado de predicción
            historical_data: Datos históricos (opcional)
            chart_type: Tipo de gráfico
            
        Returns:
            Figura de Plotly
        """
        try:
            if chart_type == "forecast":
                # Verificar si historical_data es válido
                if historical_data is None or historical_data.empty:
                    historical_data = pd.DataFrame()
                
                return self.chart_generator.create_forecast_chart(
                    historical_data,
                    result.predictions,
                    show_confidence=True,
                    show_components=False
                )
            elif chart_type == "components":
                return self.chart_generator.create_components_chart(result.predictions)
            elif chart_type == "metrics":
                metrics_df = pd.DataFrame([result.model_metrics])
                return self.chart_generator.create_accuracy_metrics_chart(metrics_df)
            elif chart_type == "alerts" and result.alerts:
                return self.chart_generator.create_weather_alert_chart(
                    result.predictions,
                    self.alert_thresholds.get(PredictionType.TEMPERATURE, {})
                )
            else:
                raise ValueError(f"Tipo de gráfico no soportado: {chart_type}")
                
        except Exception as e:
            logger.error(f"Error generando gráfico: {str(e)}")
            raise
    
    def get_model_performance_summary(self) -> Dict[str, Any]:
        """Obtener resumen del rendimiento de modelos"""
        summary = {
            "trained_models": len(self.trained_models),
            "cached_predictions": len(self.prediction_cache),
            "model_types": list(set(key.split('_')[0] for key in self.trained_models.keys())),
            "cities_covered": list(set(key.split('_')[1] for key in self.trained_models.keys() if '_' in key)),
            "evaluation_summary": self.model_evaluator.get_evaluation_summary()
        }
        
        return summary
    
    def clear_cache(self) -> None:
        """Limpiar caché de modelos y predicciones"""
        self.trained_models.clear()
        self.prediction_cache.clear()
        logger.info("Caché de predicciones limpiado")
    
    def _get_data_for_prediction(self, request: PredictionRequest) -> pd.DataFrame:
        """Obtener datos para predicción desde tablas Gold"""
        if self.data_manager is None:
            raise ValueError("DataManager no disponible")
        
        # Usar datos de Gold (gold.city_yearly_summary)
        try:
            # Obtener datos desde Gold
            data = self.data_manager.get_gold_weather_data()
            
            if data is None or data.empty:
                # Fallback a datos de summary si no hay datos Gold
                data = self.data_manager.get_data_on_demand('summary')
                if data is not None and not data.empty:
                    # Crear fechas sintéticas para datos agregados
                    data = self._create_synthetic_dates(data)
            
            if data is None or data.empty:
                raise ValueError("No se pudieron cargar los datos necesarios")
            
            # Filtrar por ciudad si se especifica
            if request.city:
                data = data[data['city'] == request.city].copy()
                if data.empty:
                    raise ValueError(f"No hay datos para la ciudad: {request.city}")
            
            return data
            
        except Exception as e:
            logger.error(f"Error obteniendo datos para predicción: {str(e)}")
            raise
    
    def _create_synthetic_dates(self, data: pd.DataFrame) -> pd.DataFrame:
        """Crear fechas sintéticas para datos agregados"""
        if 'year' in data.columns and 'month' in data.columns:
            # Crear fechas del primer día de cada mes
            data['date'] = pd.to_datetime(data[['year', 'month']].assign(day=1))
        else:
            # Si no hay year/month, crear fechas secuenciales
            data['date'] = pd.date_range(start='2020-01-01', periods=len(data), freq='D')
        
        return data
    
    def _prepare_data_for_prediction(self, data: pd.DataFrame, request: PredictionRequest) -> pd.DataFrame:
        """Preparar datos para predicción"""
        # Mapear tipo de predicción a variable objetivo
        target_mapping = {
            PredictionType.TEMPERATURE: 'temp_avg_c',
            PredictionType.PRECIPITATION: 'precip_mm',
            PredictionType.HUMIDITY: 'humidity_percent',
            PredictionType.PRESSURE: 'pressure_hpa',
            PredictionType.WIND: 'wind_avg_kmh'
        }
        
        target_variable = target_mapping[request.prediction_type]
        
        # Aplicar ingeniería de características
        enhanced_data = self.feature_engineer.engineer_features(data, target_variable)
        
        # Preparar datos para Prophet
        prophet_data = self.data_preparator.prepare_data_for_prophet(
            enhanced_data,
            target_variable,
            request.city,
            request.add_regressors
        )
        
        return prophet_data
    
    def _train_model(self, data: pd.DataFrame, request: PredictionRequest, model_key: str) -> None:
        """Entrenar modelo para predicción"""
        target_mapping = {
            PredictionType.TEMPERATURE: 'y',
            PredictionType.PRECIPITATION: 'y',
            PredictionType.HUMIDITY: 'y',
            PredictionType.PRESSURE: 'y',
            PredictionType.WIND: 'y'
        }
        
        target_variable = target_mapping[request.prediction_type]
        
        # Identificar regresores
        regressors = [col for col in data.columns if col not in ['ds', 'y']]
        
        # Entrenar modelo
        self.prophet_model.train(data, target_variable, regressors)
        
        # Guardar modelo entrenado
        self.trained_models[model_key] = self.prophet_model
        
        logger.info(f"Modelo entrenado y guardado: {model_key}")
    
    def _generate_prediction(self, request: PredictionRequest, model_key: str) -> pd.DataFrame:
        """Generar predicción con modelo entrenado"""
        model = self.trained_models[model_key]
        
        # Crear fechas futuras
        last_date = model.training_data['ds'].max()
        future_dates = self.data_preparator.create_forecast_dates(last_date, request.horizon)
        
        # Generar predicción
        predictions = model.predict(periods=request.horizon.value, future_data=future_dates)
        
        return predictions
    
    def _calculate_prediction_metrics(self, data: pd.DataFrame, predictions: pd.DataFrame) -> Dict[str, float]:
        """Calcular métricas de predicción"""
        # Métricas básicas del modelo
        if hasattr(self.prophet_model, 'model_metrics'):
            return self.prophet_model.model_metrics
        else:
            return {}
    
    def _get_feature_importance(self, data: pd.DataFrame, request: PredictionRequest) -> Dict[str, float]:
        """Obtener importancia de características"""
        target_mapping = {
            PredictionType.TEMPERATURE: 'y',
            PredictionType.PRECIPITATION: 'y',
            PredictionType.HUMIDITY: 'y',
            PredictionType.PRESSURE: 'y',
            PredictionType.WIND: 'y'
        }
        
        target_variable = target_mapping[request.prediction_type]
        return self.feature_engineer.get_feature_importance(data, target_variable)
    
    def _generate_alerts(self, predictions: pd.DataFrame, request: PredictionRequest) -> List[Dict[str, Any]]:
        """Generar alertas meteorológicas"""
        alerts = []
        
        if request.prediction_type not in self.alert_thresholds:
            return alerts
        
        thresholds = self.alert_thresholds[request.prediction_type]
        
        for _, row in predictions.iterrows():
            date = row['ds']
            value = row['yhat']
            
            for alert_type, threshold in thresholds.items():
                if self._check_alert_condition(value, threshold, alert_type):
                    alerts.append({
                        'date': date,
                        'alert_type': alert_type,
                        'threshold': threshold,
                        'predicted_value': value,
                        'severity': self._get_alert_severity(alert_type, value, threshold)
                    })
        
        return alerts
    
    def _check_alert_condition(self, value: float, threshold: float, alert_type: str) -> bool:
        """Verificar condición de alerta"""
        if 'heat' in alert_type or 'high' in alert_type:
            return value > threshold
        elif 'cold' in alert_type or 'low' in alert_type:
            return value < threshold
        else:
            return abs(value) > threshold
    
    def _get_alert_severity(self, alert_type: str, value: float, threshold: float) -> str:
        """Obtener severidad de alerta"""
        if 'extreme' in alert_type:
            return 'HIGH'
        elif abs(value - threshold) > threshold * 0.5:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _extract_confidence_intervals(self, predictions: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Extraer intervalos de confianza"""
        if 'yhat_lower' in predictions.columns and 'yhat_upper' in predictions.columns:
            return predictions[['ds', 'yhat_lower', 'yhat_upper']]
        return None
    
    def _extract_components(self, predictions: pd.DataFrame) -> Optional[Dict[str, pd.DataFrame]]:
        """Extraer componentes de Prophet"""
        if hasattr(self.prophet_model, 'get_forecast_components'):
            return self.prophet_model.get_forecast_components(predictions)
        return None
    
    def _get_model_key(self, request: PredictionRequest) -> str:
        """Generar clave única para modelo"""
        city = request.city or 'all'
        return f"{request.prediction_type.value}_{city}_{request.horizon.value}"
