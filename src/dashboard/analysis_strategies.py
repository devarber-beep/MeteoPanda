"""
Estrategias de análisis para el dashboard usando el patrón Strategy.
Elimina la duplicación de código en los métodos render_*_analysis().
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
import streamlit as st

class AnalysisStrategy(ABC):
    """Interfaz base para estrategias de análisis"""
    
    @abstractmethod
    def get_data_key(self) -> str:
        """Obtener la clave de datos para esta estrategia"""
        pass
    
    @abstractmethod
    def get_title(self) -> str:
        """Obtener el título del análisis"""
        pass
    
    @abstractmethod
    def get_warning_message(self) -> str:
        """Obtener mensaje de advertencia cuando no hay datos"""
        pass
    
    @abstractmethod
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        """Renderizar los gráficos específicos para esta estrategia"""
        pass

class TrendAnalysisStrategy(AnalysisStrategy):
    """Estrategia para análisis de tendencias"""
    
    def get_data_key(self) -> str:
        return 'trends'
    
    def get_title(self) -> str:
        return "Análisis de Tendencias"
    
    def get_warning_message(self) -> str:
        return "No hay datos de tendencias disponibles."
    
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        chart_component.render_temperature_trends(data, title)

class TemperatureAnalysisStrategy(AnalysisStrategy):
    """Estrategia para análisis de temperatura"""
    
    def get_data_key(self) -> str:
        return 'summary'
    
    def get_title(self) -> str:
        return "Análisis de Temperatura"
    
    def get_warning_message(self) -> str:
        return "No hay datos de temperatura disponibles."
    
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        chart_component.render_temperature_trends(data, "Análisis Detallado de Temperatura")

class PrecipitationAnalysisStrategy(AnalysisStrategy):
    """Estrategia para análisis de precipitación"""
    
    def get_data_key(self) -> str:
        return 'summary'
    
    def get_title(self) -> str:
        return "Análisis de Precipitación"
    
    def get_warning_message(self) -> str:
        return "No hay datos de precipitación disponibles."
    
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        chart_component.render_precipitation_analysis(data, "Análisis Detallado de Precipitación")

class SeasonalAnalysisStrategy(AnalysisStrategy):
    """Estrategia para análisis estacional"""
    
    def get_data_key(self) -> str:
        return 'seasonal'
    
    def get_title(self) -> str:
        return "Análisis Estacional"
    
    def get_warning_message(self) -> str:
        return "No hay datos estacionales disponibles."
    
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        chart_component.render_seasonal_analysis(data, "Análisis Estacional Detallado")

class AlertAnalysisStrategy(AnalysisStrategy):
    """Estrategia para análisis de alertas"""
    
    def get_data_key(self) -> str:
        return 'alerts'
    
    def get_title(self) -> str:
        return "Análisis de Alertas Meteorológicas"
    
    def get_warning_message(self) -> str:
        return "No hay datos de alertas disponibles."
    
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        chart_component.render_alert_analysis(data, "Análisis de Alertas Meteorológicas")

class ClimateComparisonStrategy(AnalysisStrategy):
    """Estrategia para comparación climática"""
    
    def get_data_key(self) -> str:
        return 'comparison'
    
    def get_title(self) -> str:
        return "Comparación Climática"
    
    def get_warning_message(self) -> str:
        return "No hay datos de comparación climática disponibles."
    
    def render_charts(self, chart_component, data: pd.DataFrame, title: str):
        chart_component.render_climate_comparison(data, "Comparación Climática Detallada")

class AnalysisContext:
    """Contexto que ejecuta las estrategias de análisis"""
    
    def __init__(self, data: Dict[str, pd.DataFrame], filter_manager, chart_component):
        self.data = data
        self.filter_manager = filter_manager
        self.chart_component = chart_component
    
    def execute_analysis(self, strategy: AnalysisStrategy):
        """Ejecutar análisis con la estrategia dada"""
        # Obtener datos
        data_key = strategy.get_data_key()
        raw_data = self.data.get(data_key, pd.DataFrame())
        
        if raw_data.empty:
            st.warning(strategy.get_warning_message())
            return
        
        # Aplicar filtros (lógica común)
        if self.filter_manager:
            filtered_data = self.filter_manager.apply_filters(raw_data)
        else:
            filtered_data = raw_data
        
        # Renderizar gráficos (lógica específica)
        title = strategy.get_title()
        strategy.render_charts(self.chart_component, filtered_data, title)
