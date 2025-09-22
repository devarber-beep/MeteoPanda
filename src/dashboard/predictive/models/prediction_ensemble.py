"""
Sistema de ensemble para combinar múltiples modelos predictivos
Mejora la precisión combinando Prophet con otros algoritmos
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
import logging
from dataclasses import dataclass
from enum import Enum
import warnings
warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class EnsembleMethod(Enum):
    """Métodos de ensemble disponibles"""
    AVERAGE = "average"
    WEIGHTED_AVERAGE = "weighted_average"
    VOTING = "voting"
    STACKING = "stacking"

@dataclass
class ModelWeight:
    """Peso de un modelo en el ensemble"""
    model_name: str
    weight: float
    performance_score: float

class PredictionEnsemble:
    """
    Sistema de ensemble para combinar predicciones de múltiples modelos
    Mejora la precisión y robustez de las predicciones meteorológicas
    """
    
    def __init__(self, method: EnsembleMethod = EnsembleMethod.WEIGHTED_AVERAGE):
        """
        Inicializar sistema de ensemble
        
        Args:
            method: Método de ensemble a utilizar
        """
        self.method = method
        self.models = {}
        self.model_weights = {}
        self.ensemble_metrics = {}
        
    def add_model(self, 
                  model_name: str, 
                  model: Any, 
                  weight: float = 1.0,
                  performance_score: float = 1.0) -> None:
        """
        Agregar modelo al ensemble
        
        Args:
            model_name: Nombre del modelo
            model: Modelo entrenado
            weight: Peso del modelo
            performance_score: Puntuación de rendimiento
        """
        self.models[model_name] = model
        self.model_weights[model_name] = ModelWeight(
            model_name=model_name,
            weight=weight,
            performance_score=performance_score
        )
        
        logger.info(f"Modelo agregado al ensemble: {model_name} (peso: {weight})")
    
    def predict_ensemble(self, 
                        data: pd.DataFrame,
                        periods: int = 30) -> pd.DataFrame:
        """
        Generar predicción del ensemble
        
        Args:
            data: Datos de entrada
            periods: Períodos a predecir
            
        Returns:
            DataFrame con predicción del ensemble
        """
        try:
            logger.info(f"Generando predicción del ensemble con {len(self.models)} modelos")
            
            # Obtener predicciones de cada modelo
            individual_predictions = {}
            
            for model_name, model in self.models.items():
                try:
                    if hasattr(model, 'predict'):
                        pred = model.predict(periods=periods, future_data=data)
                        individual_predictions[model_name] = pred
                        logger.info(f"Predicción generada para {model_name}")
                    else:
                        logger.warning(f"Modelo {model_name} no tiene método predict")
                except Exception as e:
                    logger.error(f"Error prediciendo con {model_name}: {str(e)}")
                    continue
            
            if not individual_predictions:
                raise ValueError("No se pudieron generar predicciones de ningún modelo")
            
            # Combinar predicciones según el método
            ensemble_prediction = self._combine_predictions(individual_predictions)
            
            # Calcular métricas del ensemble
            self.ensemble_metrics = self._calculate_ensemble_metrics(individual_predictions, ensemble_prediction)
            
            logger.info("Predicción del ensemble generada exitosamente")
            return ensemble_prediction
            
        except Exception as e:
            logger.error(f"Error en predicción del ensemble: {str(e)}")
            raise
    
    def _combine_predictions(self, individual_predictions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combinar predicciones individuales"""
        if self.method == EnsembleMethod.AVERAGE:
            return self._average_combination(individual_predictions)
        elif self.method == EnsembleMethod.WEIGHTED_AVERAGE:
            return self._weighted_average_combination(individual_predictions)
        elif self.method == EnsembleMethod.VOTING:
            return self._voting_combination(individual_predictions)
        else:
            raise ValueError(f"Método de ensemble no soportado: {self.method}")
    
    def _average_combination(self, predictions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combinación por promedio simple"""
        # Usar la primera predicción como base
        base_pred = list(predictions.values())[0].copy()
        
        # Calcular promedio de predicciones
        pred_columns = ['yhat']
        for col in pred_columns:
            if col in base_pred.columns:
                values = [pred[col].values for pred in predictions.values() if col in pred.columns]
                if values:
                    base_pred[col] = np.mean(values, axis=0)
        
        # Calcular intervalos de confianza si están disponibles
        if 'yhat_lower' in base_pred.columns and 'yhat_upper' in base_pred.columns:
            lower_values = [pred['yhat_lower'].values for pred in predictions.values() if 'yhat_lower' in pred.columns]
            upper_values = [pred['yhat_upper'].values for pred in predictions.values() if 'yhat_upper' in pred.columns]
            
            if lower_values and upper_values:
                base_pred['yhat_lower'] = np.mean(lower_values, axis=0)
                base_pred['yhat_upper'] = np.mean(upper_values, axis=0)
        
        return base_pred
    
    def _weighted_average_combination(self, predictions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combinación por promedio ponderado"""
        # Usar la primera predicción como base
        base_pred = list(predictions.values())[0].copy()
        
        # Calcular pesos normalizados
        total_weight = sum(weight.weight for weight in self.model_weights.values())
        normalized_weights = {name: weight.weight / total_weight 
                            for name, weight in self.model_weights.items()}
        
        # Calcular promedio ponderado
        pred_columns = ['yhat']
        for col in pred_columns:
            if col in base_pred.columns:
                weighted_sum = np.zeros(len(base_pred))
                total_weight_sum = 0
                
                for model_name, pred in predictions.items():
                    if col in pred.columns and model_name in normalized_weights:
                        weight = normalized_weights[model_name]
                        weighted_sum += pred[col].values * weight
                        total_weight_sum += weight
                
                if total_weight_sum > 0:
                    base_pred[col] = weighted_sum / total_weight_sum
        
        return base_pred
    
    def _voting_combination(self, predictions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Combinación por votación (mediana)"""
        # Usar la primera predicción como base
        base_pred = list(predictions.values())[0].copy()
        
        # Calcular mediana de predicciones
        pred_columns = ['yhat']
        for col in pred_columns:
            if col in base_pred.columns:
                values = [pred[col].values for pred in predictions.values() if col in pred.columns]
                if values:
                    base_pred[col] = np.median(values, axis=0)
        
        return base_pred
    
    def _calculate_ensemble_metrics(self, 
                                  individual_predictions: Dict[str, pd.DataFrame],
                                  ensemble_prediction: pd.DataFrame) -> Dict[str, float]:
        """Calcular métricas del ensemble"""
        metrics = {
            'num_models': len(individual_predictions),
            'ensemble_method': self.method.value,
            'model_weights': {name: weight.weight for name, weight in self.model_weights.items()}
        }
        
        # Calcular varianza entre modelos (menor es mejor)
        if len(individual_predictions) > 1:
            pred_values = [pred['yhat'].values for pred in individual_predictions.values()]
            variance = np.var(pred_values, axis=0).mean()
            metrics['model_variance'] = variance
        
        return metrics
    
    def get_ensemble_summary(self) -> Dict[str, Any]:
        """Obtener resumen del ensemble"""
        return {
            'method': self.method.value,
            'num_models': len(self.models),
            'model_names': list(self.models.keys()),
            'weights': {name: weight.weight for name, weight in self.model_weights.items()},
            'metrics': self.ensemble_metrics
        }
    
    def update_model_weights(self, performance_scores: Dict[str, float]) -> None:
        """Actualizar pesos basados en rendimiento"""
        for model_name, score in performance_scores.items():
            if model_name in self.model_weights:
                self.model_weights[model_name].performance_score = score
                # Actualizar peso basado en rendimiento (normalizar)
                total_score = sum(score for score in performance_scores.values())
                if total_score > 0:
                    self.model_weights[model_name].weight = score / total_score
        
        logger.info("Pesos de modelos actualizados basados en rendimiento")
    
    def remove_model(self, model_name: str) -> None:
        """Remover modelo del ensemble"""
        if model_name in self.models:
            del self.models[model_name]
            del self.model_weights[model_name]
            logger.info(f"Modelo removido del ensemble: {model_name}")
        else:
            logger.warning(f"Modelo no encontrado en ensemble: {model_name}")
