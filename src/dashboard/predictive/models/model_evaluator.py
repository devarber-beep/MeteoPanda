"""
Evaluador de modelos de Machine Learning para análisis predictivo meteorológico
Sistema robusto para evaluar y comparar modelos de predicción
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

logger = logging.getLogger(__name__)

class EvaluationMetric(Enum):
    """Métricas de evaluación disponibles"""
    RMSE = "rmse"
    MAE = "mae"
    MAPE = "mape"
    SMAPE = "smape"
    R2 = "r2"
    ADJUSTED_R2 = "adjusted_r2"
    MEAN_BIAS = "mean_bias"
    MEAN_ABSOLUTE_BIAS = "mean_absolute_bias"

@dataclass
class EvaluationConfig:
    """Configuración para evaluación de modelos"""
    test_size: float = 0.2
    validation_size: float = 0.2
    time_series_split: bool = True
    cross_validation_folds: int = 5
    metrics: List[EvaluationMetric] = None
    
    def __post_init__(self):
        if self.metrics is None:
            self.metrics = [
                EvaluationMetric.RMSE,
                EvaluationMetric.MAE,
                EvaluationMetric.MAPE,
                EvaluationMetric.R2
            ]

@dataclass
class ModelPerformance:
    """Rendimiento de un modelo"""
    model_name: str
    metrics: Dict[str, float]
    predictions: pd.DataFrame
    actual_values: pd.Series
    evaluation_date: datetime
    data_info: Dict[str, Any]

class ModelEvaluator:
    """
    Evaluador de modelos de Machine Learning especializado en predicciones meteorológicas
    Proporciona métricas robustas y comparaciones detalladas
    """
    
    def __init__(self, config: Optional[EvaluationConfig] = None):
        """
        Inicializar evaluador de modelos
        
        Args:
            config: Configuración de evaluación
        """
        self.config = config or EvaluationConfig()
        self.evaluation_history = []
        
    def evaluate_model(self, 
                      model: Any,
                      test_data: pd.DataFrame,
                      target_variable: str = 'y',
                      model_name: str = "Model") -> ModelPerformance:
        """
        Evaluar un modelo con datos de prueba
        
        Args:
            model: Modelo entrenado a evaluar
            test_data: DataFrame con datos de prueba
            target_variable: Variable objetivo
            model_name: Nombre del modelo
            
        Returns:
            Objeto ModelPerformance con resultados
        """
        try:
            logger.info(f"Evaluando modelo: {model_name}")
            
            # Validar datos de prueba
            self._validate_test_data(test_data, target_variable)
            
            # Generar predicciones
            predictions = self._generate_predictions(model, test_data, target_variable)
            
            # Obtener valores reales
            actual_values = test_data[target_variable]
            
            # Calcular métricas
            metrics = self._calculate_metrics(actual_values, predictions, target_variable)
            
            # Crear objeto de rendimiento
            performance = ModelPerformance(
                model_name=model_name,
                metrics=metrics,
                predictions=predictions,
                actual_values=actual_values,
                evaluation_date=datetime.now(),
                data_info=self._get_data_info(test_data)
            )
            
            # Guardar en historial
            self.evaluation_history.append(performance)
            
            logger.info(f"Evaluación completada para {model_name}")
            return performance
            
        except Exception as e:
            logger.error(f"Error evaluando modelo {model_name}: {str(e)}")
            raise
    
    def compare_models(self, performances: List[ModelPerformance]) -> pd.DataFrame:
        """
        Comparar múltiples modelos
        
        Args:
            performances: Lista de rendimientos de modelos
            
        Returns:
            DataFrame con comparación de modelos
        """
        if not performances:
            return pd.DataFrame()
        
        try:
            logger.info(f"Comparando {len(performances)} modelos")
            
            # Crear DataFrame de comparación
            comparison_data = []
            
            for perf in performances:
                row = {
                    'Model': perf.model_name,
                    'Evaluation_Date': perf.evaluation_date,
                    **perf.metrics
                }
                comparison_data.append(row)
            
            comparison_df = pd.DataFrame(comparison_data)
            
            # Ordenar por RMSE (menor es mejor)
            if 'rmse' in comparison_df.columns:
                comparison_df = comparison_df.sort_values('rmse')
            
            logger.info("Comparación de modelos completada")
            return comparison_df
            
        except Exception as e:
            logger.error(f"Error comparando modelos: {str(e)}")
            raise
    
    def time_series_split_evaluation(self, 
                                   model: Any,
                                   data: pd.DataFrame,
                                   target_variable: str = 'y',
                                   n_splits: int = 5) -> Dict[str, float]:
        """
        Evaluación con división temporal para series de tiempo
        
        Args:
            model: Modelo a evaluar
            data: DataFrame con datos
            target_variable: Variable objetivo
            n_splits: Número de divisiones
            
        Returns:
            Diccionario con métricas promedio
        """
        try:
            logger.info(f"Evaluación temporal con {n_splits} divisiones")
            
            # Ordenar por fecha
            data_sorted = data.sort_values('ds' if 'ds' in data.columns else 'date')
            
            # Dividir datos temporalmente
            total_length = len(data_sorted)
            split_size = total_length // n_splits
            
            all_metrics = []
            
            for i in range(n_splits - 1):
                # Datos de entrenamiento (hasta el split actual)
                train_end = (i + 1) * split_size
                train_data = data_sorted.iloc[:train_end]
                
                # Datos de prueba (siguiente split)
                test_start = train_end
                test_end = min((i + 2) * split_size, total_length)
                test_data = data_sorted.iloc[test_start:test_end]
                
                if len(test_data) == 0:
                    continue
                
                # Entrenar modelo en datos de entrenamiento
                model_copy = self._clone_model(model)
                model_copy.train(train_data, target_variable)
                
                # Evaluar en datos de prueba
                performance = self.evaluate_model(
                    model_copy, test_data, target_variable, 
                    f"Split_{i+1}"
                )
                
                all_metrics.append(performance.metrics)
            
            # Calcular métricas promedio
            avg_metrics = self._calculate_average_metrics(all_metrics)
            
            logger.info("Evaluación temporal completada")
            return avg_metrics
            
        except Exception as e:
            logger.error(f"Error en evaluación temporal: {str(e)}")
            raise
    
    def get_model_ranking(self, metric: str = 'rmse') -> pd.DataFrame:
        """
        Obtener ranking de modelos por métrica
        
        Args:
            metric: Métrica para ranking
            
        Returns:
            DataFrame con ranking de modelos
        """
        if not self.evaluation_history:
            return pd.DataFrame()
        
        try:
            # Crear DataFrame con todos los modelos
            ranking_data = []
            for perf in self.evaluation_history:
                row = {
                    'Model': perf.model_name,
                    'Metric_Value': perf.metrics.get(metric, np.nan),
                    'Evaluation_Date': perf.evaluation_date
                }
                ranking_data.append(row)
            
            ranking_df = pd.DataFrame(ranking_data)
            
            # Ordenar por métrica (menor es mejor para RMSE, MAE, MAPE)
            ascending = metric.lower() in ['rmse', 'mae', 'mape', 'smape']
            ranking_df = ranking_df.sort_values('Metric_Value', ascending=ascending)
            
            # Agregar ranking
            ranking_df['Rank'] = range(1, len(ranking_df) + 1)
            
            return ranking_df
            
        except Exception as e:
            logger.error(f"Error creando ranking: {str(e)}")
            return pd.DataFrame()
    
    def _validate_test_data(self, data: pd.DataFrame, target_variable: str) -> None:
        """Validar datos de prueba"""
        if data.empty:
            raise ValueError("Datos de prueba vacíos")
        
        if target_variable not in data.columns:
            raise ValueError(f"Variable objetivo '{target_variable}' no encontrada")
        
        if data[target_variable].isna().all():
            raise ValueError("Variable objetivo tiene todos los valores faltantes")
    
    def _generate_predictions(self, model: Any, test_data: pd.DataFrame, target_variable: str) -> pd.Series:
        """Generar predicciones del modelo"""
        try:
            # Para modelos Prophet
            if hasattr(model, 'predict'):
                if 'ds' in test_data.columns:
                    # Crear DataFrame futuro
                    future_df = test_data[['ds']].copy()
                    
                    # Agregar regresores si existen
                    regressor_cols = [col for col in test_data.columns 
                                    if col not in ['ds', target_variable]]
                    for col in regressor_cols:
                        future_df[col] = test_data[col]
                    
                    # Generar predicción
                    forecast = model.predict(future_df)
                    return forecast['yhat']
                else:
                    raise ValueError("Columna 'ds' no encontrada para Prophet")
            
            # Para otros modelos (scikit-learn, etc.)
            elif hasattr(model, 'predict'):
                # Preparar features
                feature_cols = [col for col in test_data.columns if col != target_variable]
                X_test = test_data[feature_cols]
                return pd.Series(model.predict(X_test), index=test_data.index)
            
            else:
                raise ValueError("Modelo no soportado para evaluación")
                
        except Exception as e:
            logger.error(f"Error generando predicciones: {str(e)}")
            raise
    
    def _calculate_metrics(self, 
                          actual: pd.Series, 
                          predicted: pd.Series,
                          target_variable: str) -> Dict[str, float]:
        """Calcular métricas de evaluación"""
        metrics = {}
        
        # Asegurar que las series tengan el mismo índice
        common_index = actual.index.intersection(predicted.index)
        actual = actual.loc[common_index]
        predicted = predicted.loc[common_index]
        
        # Eliminar valores NaN
        mask = ~(actual.isna() | predicted.isna())
        actual = actual[mask]
        predicted = predicted[mask]
        
        if len(actual) == 0:
            logger.warning("No hay datos válidos para calcular métricas")
            return {}
        
        # Calcular métricas según configuración
        for metric in self.config.metrics:
            try:
                if metric == EvaluationMetric.RMSE:
                    metrics['rmse'] = np.sqrt(np.mean((actual - predicted) ** 2))
                
                elif metric == EvaluationMetric.MAE:
                    metrics['mae'] = np.mean(np.abs(actual - predicted))
                
                elif metric == EvaluationMetric.MAPE:
                    # Evitar división por cero
                    mask = actual != 0
                    if mask.any():
                        mape = np.mean(np.abs((actual[mask] - predicted[mask]) / actual[mask])) * 100
                        metrics['mape'] = mape
                    else:
                        metrics['mape'] = np.nan
                
                elif metric == EvaluationMetric.SMAPE:
                    smape = np.mean(2 * np.abs(actual - predicted) / (np.abs(actual) + np.abs(predicted))) * 100
                    metrics['smape'] = smape
                
                elif metric == EvaluationMetric.R2:
                    ss_res = np.sum((actual - predicted) ** 2)
                    ss_tot = np.sum((actual - np.mean(actual)) ** 2)
                    r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
                    metrics['r2'] = r2
                
                elif metric == EvaluationMetric.ADJUSTED_R2:
                    ss_res = np.sum((actual - predicted) ** 2)
                    ss_tot = np.sum((actual - np.mean(actual)) ** 2)
                    r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
                    n = len(actual)
                    p = 1  # Número de predictores (simplificado)
                    adj_r2 = 1 - (1 - r2) * (n - 1) / (n - p - 1) if n > p + 1 else r2
                    metrics['adjusted_r2'] = adj_r2
                
                elif metric == EvaluationMetric.MEAN_BIAS:
                    metrics['mean_bias'] = np.mean(predicted - actual)
                
                elif metric == EvaluationMetric.MEAN_ABSOLUTE_BIAS:
                    metrics['mean_absolute_bias'] = np.mean(np.abs(predicted - actual))
                
            except Exception as e:
                logger.warning(f"Error calculando métrica {metric.value}: {str(e)}")
                metrics[metric.value] = np.nan
        
        return metrics
    
    def _calculate_average_metrics(self, metrics_list: List[Dict[str, float]]) -> Dict[str, float]:
        """Calcular métricas promedio de múltiples evaluaciones"""
        if not metrics_list:
            return {}
        
        avg_metrics = {}
        for metric in metrics_list[0].keys():
            values = [m.get(metric, np.nan) for m in metrics_list if not np.isnan(m.get(metric, np.nan))]
            if values:
                avg_metrics[metric] = np.mean(values)
            else:
                avg_metrics[metric] = np.nan
        
        return avg_metrics
    
    def _clone_model(self, model: Any) -> Any:
        """Clonar modelo para evaluación temporal"""
        # Implementación simplificada - en producción usaría deepcopy o métodos específicos
        return model
    
    def _get_data_info(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Obtener información de los datos"""
        return {
            'total_records': len(data),
            'date_range': (data['ds'].min(), data['ds'].max()) if 'ds' in data.columns else None,
            'columns': list(data.columns),
            'missing_values': data.isnull().sum().to_dict()
        }
    
    def get_evaluation_summary(self) -> Dict[str, Any]:
        """Obtener resumen de todas las evaluaciones"""
        if not self.evaluation_history:
            return {"message": "No hay evaluaciones disponibles"}
        
        summary = {
            "total_evaluations": len(self.evaluation_history),
            "models_evaluated": list(set(perf.model_name for perf in self.evaluation_history)),
            "evaluation_dates": [perf.evaluation_date for perf in self.evaluation_history],
            "best_model": self.get_model_ranking('rmse').iloc[0]['Model'] if self.evaluation_history else None
        }
        
        return summary
