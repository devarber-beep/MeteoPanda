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
        self._cache = {}
        self._cache_timestamps = {}
        self.cache_ttl = 3600  # 1 hora en segundos
    
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
    
    def _is_cache_valid(self, key: str) -> bool:
        """Verificar si el caché es válido"""
        if key not in self._cache_timestamps:
            return False
        
        cache_time = self._cache_timestamps[key]
        return datetime.now() - cache_time < timedelta(seconds=self.cache_ttl)
    
    def _set_cache(self, key: str, data):
        """Establecer datos en caché"""
        self._cache[key] = data
        self._cache_timestamps[key] = datetime.now()
        logger.info(f"Datos cacheados para: {key}")
    
    def _get_cache(self, key: str):
        """Obtener datos del caché"""
        if self._is_cache_valid(key):
            logger.info(f"Datos obtenidos del caché: {key}")
            return self._cache[key]
        return None
    
    def execute_query(self, query: str, cache_key: Optional[str] = None) -> Optional[pd.DataFrame]:
        """Ejecutar consulta con caché y manejo de errores"""
        try:
            # Verificar caché primero
            if cache_key:
                cached_data = self._get_cache(cache_key)
                if cached_data is not None:
                    return cached_data
            
            # Ejecutar consulta
            con = self.get_connection()
            if con is None:
                return None
            
            result = con.execute(query).df()
            
            # Guardar en caché si se especificó
            if cache_key:
                self._set_cache(cache_key, result)
            
            logger.info(f"Consulta ejecutada exitosamente: {query[:50]}...")
            return result
            
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            st.error(f"Error en consulta: {str(e)}")
            return None
    
    def load_summary_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de resumen anual"""
        return self.execute_query(
            "SELECT * FROM gold.city_yearly_summary",
            cache_key="summary_data"
        )
    
    def load_extreme_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de días extremos"""
        return self.execute_query(
            "SELECT * FROM gold.city_extreme_days",
            cache_key="extreme_data"
        )
    
    def load_trends_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de tendencias"""
        return self.execute_query(
            "SELECT * FROM gold.weather_trends",
            cache_key="trends_data"
        )
    
    def load_climate_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de perfiles climáticos"""
        return self.execute_query(
            "SELECT * FROM gold.climate_profiles",
            cache_key="climate_data"
        )
    
    def load_coordinates_data(self) -> Optional[pd.DataFrame]:
        """Cargar coordenadas de ciudades"""
        return self.execute_query(
            """
            SELECT DISTINCT city, lat, lon 
            FROM silver.weather_cleaned
            WHERE lat IS NOT NULL AND lon IS NOT NULL
            """,
            cache_key="coordinates_data"
        )
    
    def load_alerts_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de alertas meteorológicas"""
        return self.execute_query(
            "SELECT * FROM gold.weather_alerts",
            cache_key="alerts_data"
        )
    
    def load_seasonal_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de análisis estacional"""
        return self.execute_query(
            "SELECT * FROM gold.seasonal_analysis",
            cache_key="seasonal_data"
        )
    
    def load_comparison_data(self) -> Optional[pd.DataFrame]:
        """Cargar datos de comparación climática"""
        return self.execute_query(
            "SELECT * FROM gold.climate_comparison",
            cache_key="comparison_data"
        )
    
    @st.cache_data(ttl=3600)
    def get_all_data(_self) -> Dict[str, pd.DataFrame]:
        """Cargar todos los datos principales"""
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
        self._cache.clear()
        self._cache_timestamps.clear()
        st.cache_data.clear()
        logger.info("Caché limpiado")
    
    @st.cache_data(ttl=3600)
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
