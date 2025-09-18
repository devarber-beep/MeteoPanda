"""
Gestor de datos para el dashboard con caché y manejo de errores
"""
import streamlit as st
import duckdb
import pandas as pd
from typing import Dict, Optional, List, Tuple, Any
from datetime import datetime, timedelta
from ..utils.logging_config import get_logger, log_operation_start, log_operation_success, log_operation_error, log_database_operation, log_cache_operation, log_performance_warning, log_and_show_warning, log_and_show_error

# Configurar logger
logger = get_logger("data_manager")

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
                log_database_operation(logger, "conectar", "meteopanda.duckdb", db_path=self.db_path)
            return self.connection
        except Exception as e:
            log_operation_error(logger, "conexión a base de datos", e, db_path=self.db_path)
            st.error(f"Error de conexión: {str(e)}")
            return None
    
    def close_connection(self):
        """Cerrar conexión a la base de datos"""
        if self.connection:
            self.connection.close()
            self.connection = None
            log_database_operation(logger, "desconectar", "meteopanda.duckdb")
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """Ejecutar consulta con manejo de errores"""
        try:
            con = self.get_connection()
            if con is None:
                return None
            
            result = con.execute(query).df()
            log_database_operation(logger, "consulta", "query", affected_rows=len(result), query_preview=query[:50])
            return result
            
        except Exception as e:
            log_operation_error(logger, "ejecución de consulta", e, query_preview=query[:50])
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
                log_and_show_warning(logger, f"No se pudieron cargar algunos datos esenciales: {', '.join(failed_loads)}", 
                                   failed_loads=failed_loads, total_data_types=len(data))
            
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
            log_and_show_error(logger, f"Tipo de datos no válido: {data_type}", 
                             data_type=data_type, available_types=list(data_loaders.keys()))
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
                log_and_show_warning(logger, f"No se pudieron cargar algunos datos: {', '.join(failed_loads)}", 
                                   failed_loads=failed_loads, total_data_types=len(data))
            
            return data
    
    def clear_cache(self):
        """Limpiar todo el caché"""
        st.cache_data.clear()
        log_cache_operation(logger, "limpiar", "all_cache")
    
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
    
    def get_paginated_data(self, 
                          data_type: str, 
                          page: int = 1, 
                          items_per_page: int = 50,
                          filters: Optional[Dict] = None,
                          sort_by: Optional[str] = None,
                          sort_ascending: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Obtener datos paginados directamente desde la base de datos
        
        Args:
            data_type: Tipo de datos a cargar
            page: Número de página (1-indexed)
            items_per_page: Elementos por página
            filters: Filtros a aplicar
            sort_by: Columna para ordenar
            sort_ascending: Orden ascendente o descendente
            
        Returns:
            Tuple con (datos_paginados, metadatos_paginación)
        """
        try:
            # Construir consulta base
            base_query = self._get_base_query(data_type)
            
            # Añadir filtros
            where_clause = self._build_where_clause(filters)
            if where_clause:
                base_query += f" WHERE {where_clause}"
            
            # Añadir ordenamiento
            if sort_by:
                order_direction = "ASC" if sort_ascending else "DESC"
                base_query += f" ORDER BY {sort_by} {order_direction}"
            
            # Calcular offset y limit
            offset = (page - 1) * items_per_page
            limit = items_per_page
            
            # Consulta de conteo total
            count_query = f"SELECT COUNT(*) as total FROM ({base_query}) as count_query"
            
            # Consulta paginada
            paginated_query = f"{base_query} LIMIT {limit} OFFSET {offset}"
            
            # Ejecutar consultas
            con = self.get_connection()
            if con is None:
                return pd.DataFrame(), self._get_empty_metadata()
            
            # Obtener total de registros
            total_count = con.execute(count_query).fetchone()[0]
            
            # Obtener datos paginados
            paginated_data = con.execute(paginated_query).df()
            
            # Calcular metadatos de paginación
            total_pages = (total_count + items_per_page - 1) // items_per_page
            has_next = page < total_pages
            has_prev = page > 1
            
            metadata = {
                'current_page': page,
                'total_pages': total_pages,
                'total_items': total_count,
                'items_per_page': items_per_page,
                'has_next': has_next,
                'has_prev': has_prev,
                'start_item': offset + 1,
                'end_item': min(offset + items_per_page, total_count)
            }
            
            log_database_operation(logger, "consulta_paginada", data_type, 
                                 affected_rows=len(paginated_data), 
                                 query_preview=f"Página {page} de {total_pages}")
            
            return paginated_data, metadata
            
        except Exception as e:
            log_operation_error(logger, "consulta paginada", e, data_type=data_type, page=page)
            return pd.DataFrame(), self._get_empty_metadata()
    
    def _get_base_query(self, data_type: str) -> str:
        """Obtener consulta base según el tipo de datos"""
        queries = {
            'summary': "SELECT * FROM gold.city_yearly_summary",
            'extreme': "SELECT * FROM gold.city_extreme_days",
            'trends': "SELECT * FROM gold.weather_trends",
            'climate': "SELECT * FROM gold.climate_profiles",
            'alerts': "SELECT * FROM gold.weather_alerts",
            'seasonal': "SELECT * FROM gold.seasonal_analysis",
            'comparison': "SELECT * FROM gold.climate_comparison"
        }
        
        return queries.get(data_type, "SELECT * FROM gold.city_yearly_summary")
    
    def _build_where_clause(self, filters: Optional[Dict]) -> str:
        """Construir cláusula WHERE basada en filtros"""
        if not filters:
            return ""
        
        conditions = []
        
        for key, value in filters.items():
            if value is None or value == []:
                continue
                
            if key == 'year':
                if isinstance(value, list):
                    years_str = ','.join(map(str, value))
                    conditions.append(f"year IN ({years_str})")
                else:
                    conditions.append(f"year = {value}")
            
            elif key == 'month':
                if isinstance(value, list):
                    months_str = ','.join(map(str, value))
                    conditions.append(f"month IN ({months_str})")
                else:
                    conditions.append(f"month = {value}")
            
            elif key == 'cities':
                if isinstance(value, list):
                    cities_str = "','".join(value)
                    conditions.append(f"city IN ('{cities_str}')")
                else:
                    conditions.append(f"city = '{value}'")
            
            elif key == 'region':
                if isinstance(value, list):
                    regions_str = "','".join(value)
                    conditions.append(f"region IN ('{regions_str}')")
                else:
                    conditions.append(f"region = '{value}'")
            
            elif key == 'min_temp':
                conditions.append(f"avg_temp >= {value}")
            
            elif key == 'max_temp':
                conditions.append(f"avg_temp <= {value}")
            
            elif key == 'max_precip':
                conditions.append(f"total_precip <= {value}")
        
        return " AND ".join(conditions) if conditions else ""
    
    def _get_empty_metadata(self) -> Dict[str, Any]:
        """Obtener metadatos vacíos para casos de error"""
        return {
            'current_page': 1,
            'total_pages': 1,
            'total_items': 0,
            'items_per_page': 50,
            'has_next': False,
            'has_prev': False,
            'start_item': 0,
            'end_item': 0
        }
    