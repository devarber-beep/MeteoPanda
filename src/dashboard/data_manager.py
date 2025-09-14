"""
Gestor de datos para el dashboard con caché y manejo de errores
"""
import streamlit as st
import duckdb
import pandas as pd
from typing import Dict, Optional, List
import logging
from datetime import datetime, timedelta

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataManager:
    """Gestor centralizado de datos con caché y manejo de errores"""
    
    def __init__(self, db_path: str = 'meteopanda.duckdb'):
        self.db_path = db_path
        self.connection = None
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Obtener conexión a la base de datos con manejo de errores"""
        try:
            if self.connection is None:
                self.connection = duckdb.connect(self.db_path)
                logger.info("Conexión a base de datos establecida")
            return self.connection
        except Exception as e:
            logger.error(f"Error conectando a la base de datos: {e}")
            st.error(f"Error de conexión: {str(e)}")
            return None
    
    def close_connection(self):
        """Cerrar conexión a la base de datos"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Conexión a base de datos cerrada")
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Ejecutar consulta con manejo de errores"""
        try:
            con = self.get_connection()
            if con is None:
                return None
            
            result = con.execute(query).df()
            logger.info(f"Consulta ejecutada exitosamente: {query[:50]}...")
            return result
            
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            st.error(f"Error en consulta: {str(e)}")
            return None
    
    @st.cache_data(ttl=7200)
    def load_summary_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de resumen anual"""
        return _self.execute_query("SELECT * FROM gold.city_yearly_summary")
    
    @st.cache_data(ttl=7200)
    def load_extreme_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de días extremos"""
        return _self.execute_query("SELECT * FROM gold.city_extreme_days")
    
    @st.cache_data(ttl=7200)
    def load_trends_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de tendencias"""
        return _self.execute_query("SELECT * FROM gold.weather_trends")
    
    @st.cache_data(ttl=7200)
    def load_climate_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de perfiles climáticos"""
        return _self.execute_query("SELECT * FROM gold.climate_profiles")
    
    @st.cache_data(ttl=7200)
    def load_coordinates_data(_self) -> Optional[pd.DataFrame]:
        """Cargar coordenadas de ciudades"""
        return _self.execute_query(
            """
            SELECT DISTINCT city, lat, lon 
            FROM silver.weather_cleaned
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            """
        )
    
    @st.cache_data(ttl=7200)
    def load_alerts_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de alertas meteorológicas"""
        return _self.execute_query("SELECT * FROM gold.weather_alerts")
    
    @st.cache_data(ttl=7200)
    def load_seasonal_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de análisis estacional"""
        return _self.execute_query("SELECT * FROM gold.seasonal_analysis")
    
    @st.cache_data(ttl=7200)
    def load_comparison_data(_self) -> Optional[pd.DataFrame]:
        """Cargar datos de comparación climática"""
        return _self.execute_query("SELECT * FROM gold.climate_comparison")
    
    @st.cache_data(ttl=7200)
    def get_essential_data(_self) -> Dict[str, pd.DataFrame]:
        """Cargar solo datos esenciales para la inicialización"""
        with st.spinner("Cargando datos esenciales..."):
            data = {
                'coords': _self.load_coordinates_data(),
                'summary': _self.load_summary_data()
            }
            
            # Verificar que los datos esenciales se cargaron correctamente
            failed_loads = [k for k, v in data.items() if v is None]
            if failed_loads:
                st.warning(f"No se pudieron cargar algunos datos esenciales: {', '.join(failed_loads)}")
            
            return data

    @st.cache_data(ttl=7200)
    def get_data_on_demand(_self, data_type: str) -> Optional[pd.DataFrame]:
        """Cargar datos específicos bajo demanda (lazy loading real)"""
        data_loaders = {
            'summary': _self.load_summary_data,
            'extreme': _self.load_extreme_data,
            'trends': _self.load_trends_data,
            'climate': _self.load_climate_data,
            'coords': _self.load_coordinates_data,
            'alerts': _self.load_alerts_data,
            'seasonal': _self.load_seasonal_data,
            'comparison': _self.load_comparison_data
        }
        
        if data_type in data_loaders:
            with st.spinner(f"Cargando datos de {data_type}..."):
                return data_loaders[data_type]()
        else:
            st.error(f"Tipo de datos no válido: {data_type}")
            return None

    @st.cache_data(ttl=7200)
    def get_all_data(_self) -> Dict[str, pd.DataFrame]:
        """Cargar todos los datos principales (método legacy para compatibilidad)"""
        with st.spinner("Cargando datos..."):
            data = {
                'summary': _self.load_summary_data(),
                'extreme': _self.load_extreme_data(),
                'trends': _self.load_trends_data(),
                'climate': _self.load_climate_data(),
                'coords': _self.load_coordinates_data(),
                'alerts': _self.load_alerts_data(),
                'seasonal': _self.load_seasonal_data(),
                'comparison': _self.load_comparison_data()
            }
            
            # Verificar que todos los datos se cargaron correctamente
            failed_loads = [k for k, v in data.items() if v is None]
            if failed_loads:
                st.warning(f"No se pudieron cargar algunos datos: {', '.join(failed_loads)}")
            
            return data
    
    def clear_cache(self):
        """Limpiar todo el caché"""
        st.cache_data.clear()
        logger.info("Caché limpiado")
    
    @st.cache_data(ttl=7200)
    def get_data_info(_self) -> Dict[str, int]:
        """Obtener información sobre los datos cargados"""
        data = _self.get_all_data()
        info = {}
        
        for key, df in data.items():
            if df is not None:
                info[key] = len(df)
            else:
                info[key] = 0
        
        return info
