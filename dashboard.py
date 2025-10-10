"""
Dashboard pro MeteoPanda
"""
import streamlit as st
import pandas as pd
import yaml
from typing import Dict, Optional
import sys
import os
from datetime import datetime
from streamlit_option_menu import option_menu

# Añadir el directorio src al path para importar módulos
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))


# Configurar logging antes de importar otros módulos
from src.utils.logging_config import setup_logging, get_logger, log_operation_start, log_operation_success, log_operation_error, log_configuration_loaded, log_and_show_warning, log_and_show_error

# Configurar logging
setup_logging(level="INFO", log_file="logs/dashboard.log", console_output=True, structured=True)
logger = get_logger("dashboard")

# Importar componentes del dashboard
from src.dashboard.data_manager import DataManager
from src.dashboard.filter_manager import FilterManager
from src.dashboard.table_component import AdvancedTableComponent
from src.dashboard.map_component import AdvancedMapComponent
from src.dashboard.chart_component import AdvancedChartComponent
from src.dashboard.analysis_strategies import (
    AnalysisContext,
    TrendAnalysisStrategy,
    TemperatureAnalysisStrategy,
    PrecipitationAnalysisStrategy,
    SeasonalAnalysisStrategy,
    AlertAnalysisStrategy,
    ClimateComparisonStrategy
)

# Configuración de la página
st.set_page_config(
    page_title="MeteoPanda Dashboard",
    page_icon="🌦️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuración de caché para configuración
@st.cache_data
def load_config():
    """Cargar configuración de ciudades"""
    try:
        with open('config/config.yaml', 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            log_configuration_loaded(logger, "ciudades", cities_count=len(config.get('cities', [])))
            return config
    except Exception as e:
        log_operation_error(logger, "carga de configuración", e, config_file="config/config.yaml")
        st.error(f"Error cargando configuración: {str(e)}")
        return None

class MeteoPandaDashboard:
    """Dashboard principal de MeteoPanda con arquitectura modular y desacoplada"""
    
    def __init__(self):
        # Componentes concretos
        self.data_manager = DataManager()
        self.config = load_config()
        self.data = None
        self.loaded_data_types = set()  # Track qué tipos de datos están cargados
        
        # Componentes de UI
        self.table_component = AdvancedTableComponent(items_per_page=50)
        self.map_component = None
        self.chart_component = AdvancedChartComponent()
        
        # Componentes directos 
        self.filter_manager = None
        
        # Contexto de análisis
        self.analysis_context = None
        
        
        # Configuración de rendimiento
        self.performance_config = {
            'enable_lazy_loading': True,
            'enable_caching': True,
            'max_cache_size': 1000,
            'items_per_page': 50
        }
    
    def initialize(self):
        """Inicializar el dashboard con lazy loading"""
        # Verificar si ya está inicializado en session_state
        if 'dashboard_initialized' in st.session_state and st.session_state['dashboard_initialized']:
            logger.info("Dashboard ya inicializado, usando datos de session_state")
            # Usar datos ya cargados
            self.data = st.session_state.get('dashboard_data', {})
            self.loaded_data_types = st.session_state.get('loaded_data_types', set())
            self.map_component = st.session_state.get('map_component')
            self.filter_manager = st.session_state.get('filter_manager')
            self.analysis_context = st.session_state.get('analysis_context')
            
            # Asegurar que el data_manager esté asignado al table_component
            self.table_component.set_data_manager(self.data_manager)
            
            return True
        
        # Cargar solo datos esenciales
        log_operation_start(logger, "inicialización del dashboard")
        with st.spinner("Inicializando dashboard..."):
            self.data = self.data_manager.get_essential_data()
            
            if self.data is None:
                log_operation_error(logger, "inicialización del dashboard", Exception("No se pudieron cargar datos esenciales"))
                st.error("Error al cargar los datos esenciales. Verifica la conexión a la base de datos.")
                return False
            
            # Marcar datos esenciales como cargados
            self.loaded_data_types.update(['coords', 'summary'])
            
            # Inicializar componentes que dependen de datos esenciales
            if self.data.get('coords') is not None:
                self.map_component = AdvancedMapComponent(self.data['coords'])
            
            # Inicializar filtros con datos esenciales
            self.filter_manager = FilterManager(self.data)
            
            # Asignar data_manager al table_component para paginación real
            self.table_component.set_data_manager(self.data_manager)
            
            # Inicializar contexto de análisis
            self.analysis_context = AnalysisContext(
                self.data, 
                self.filter_manager, 
                self.chart_component
            )
            
            
            # Guardar en session_state para evitar reinicializaciones
            st.session_state['dashboard_initialized'] = True
            st.session_state['dashboard_data'] = self.data
            st.session_state['loaded_data_types'] = self.loaded_data_types
            st.session_state['map_component'] = self.map_component
            st.session_state['filter_manager'] = self.filter_manager
            st.session_state['analysis_context'] = self.analysis_context
            
            log_operation_success(logger, "inicialización del dashboard", 
                                loaded_data_types=list(self.loaded_data_types),
                                has_map_component=self.map_component is not None,
                                has_filter_manager=self.filter_manager is not None)
            return True

    def get_data_lazy(self, data_type: str) -> pd.DataFrame:
        """Obtener datos con lazy loading real"""
        # Si ya está cargado, devolverlo
        if data_type in self.loaded_data_types and self.data.get(data_type) is not None:
            return self.data[data_type]
        
        # Cargar bajo demanda
        data = self.data_manager.get_data_on_demand(data_type)
        if data is not None:
            # Inicializar data si es None
            if self.data is None:
                self.data = {}
            
            # Guardar en cache local y session_state
            self.data[data_type] = data
            self.loaded_data_types.add(data_type)
            
            # Actualizar session_state
            st.session_state['dashboard_data'] = self.data
            st.session_state['loaded_data_types'] = self.loaded_data_types
            
            return data
        else:
            return pd.DataFrame()  # Devolver DataFrame vacío si falla
    
    def render_header(self):
        """Renderizar cabecera del dashboard"""
        st.title("🐼 MeteoPanda Dashboard Pro")
    
    def render_sidebar(self):
        """Renderizar sidebar solo con filtros e información"""
        with st.sidebar:           
            # Renderizar filtros
            if self.filter_manager:
                rendered_filters = self.filter_manager.render_filters()
            
            st.markdown("---")
            
            # Información del sistema
            st.header("Información")
            st.write("**Versión:** 2.0")
            st.write("**Dev by:** @devarber")
            st.write("**Linkdn:**  https://www.linkedin.com/in/danielbarero/")
            st.write("**Github:**  https://github.com/devarber-beep")


            
            # Botones de control
            st.header("Controles")
            if st.button("Recargar Datos"):
                self.data_manager.clear_cache()
                if self.map_component:
                    self.map_component.clear_cache()
                # Limpiar session_state
                for key in ['dashboard_initialized', 'dashboard_data', 'loaded_data_types', 
                           'map_component', 'filter_manager', 'analysis_context']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
    

    def render_navbar(self):
        """Renderizar navegación superior con option_menu y lazy loading"""
        # Opciones del menú
        menu_options = [
            "Dashboard Principal",
            "Tabla de Datos",
            "Mapas Interactivos", 
            "Análisis de Tendencias",
            "Análisis de Temperatura",
            "Análisis de Precipitación",
            "Análisis Estacional",
            "Alertas Meteorológicas",
            "Comparación Climática",
            "Similitud Climática",
            "Configuración"
        ]
        
        # Crear el navbar horizontal
        selected = option_menu(
            menu_title=None,
            options=menu_options,
            icons=['house', 'table', 'map', 'trending-up', 'thermometer-half', 
                   'cloud-rain', 'calendar', 'exclamation-triangle', 'globe', 'crystal-ball', 'gear'],
            menu_icon="cast",
            default_index=0,
            orientation="horizontal",
            styles={
                "container": {"padding": "0!important", "background-color": "#fafafa"},
                "icon": {"color": "orange", "font-size": "16px"}, 
                "nav-link": {
                    "font-size": "14px",
                    "text-align": "center",
                    "margin": "0px",
                    "--hover-color": "#eee"
                },
                "nav-link-selected": {"background-color": "#02ab21"},
            }
        )
        
        # Solo renderizar la página seleccionada (lazy loading)
        if selected == "Dashboard Principal":
            self.render_main_dashboard()
        elif selected == "Tabla de Datos":
            self.render_data_table()
        elif selected == "Mapas Interactivos":
            self.render_interactive_maps()
        elif selected == "Análisis de Tendencias":
            self.render_trend_analysis()
        elif selected == "Análisis de Temperatura":
            self.render_temperature_analysis()
        elif selected == "Análisis de Precipitación":
            self.render_precipitation_analysis()
        elif selected == "Análisis Estacional":
            self.render_seasonal_analysis()
        elif selected == "Alertas Meteorológicas":
            self.render_alert_analysis()
        elif selected == "Comparación Climática":
            self.render_climate_comparison()
        elif selected == "Similitud Climática":
            self.render_similarity_page()
        elif selected == "Configuración":
            self.render_configuration()
    
    def render_main_dashboard(self):
        """Renderizar dashboard principal"""
        st.header("Dashboard Principal")
        
        if not self.data or self.data.get('summary') is None:
            log_and_show_warning(logger, "No hay datos disponibles para mostrar el dashboard.", 
                               data_keys=list(self.data.keys()) if self.data else None)
            return
        
        # Aplicar filtros a los datos del resumen
        summary_data = self.data['summary']
        filtered_summary_data = self.filter_manager.apply_filters(summary_data)
        
        # KPIs principales
        self.chart_component.render_kpi_dashboard(filtered_summary_data, "KPIs Principales")
        
        # Mapa interactivo
        if self.map_component:
            st.subheader("Vista General del Clima")
            
            col1, col2 = st.columns([1, 3])
            
            with col1:
                map_type = self.map_component.render_map_selector("main")
                metric = self.map_component.render_metric_selector(map_type, "main")
            
            with col2:
                # Renderizar solo el mapa seleccionado con lazy loading real
                if not filtered_summary_data.empty:
                    self.map_component.render_map_with_lazy_loading(filtered_summary_data, metric, map_type, "main")
                else:
                    log_and_show_warning(logger, "No hay datos para mostrar en el mapa.", 
                                       map_type=map_type, filtered_records=len(filtered_summary_data))
        
        # Resumen de datos por ciudad
        st.subheader("Resumen por Ciudad")
        if not filtered_summary_data.empty:
            city_summary = filtered_summary_data.groupby('city').agg({
                'avg_temp': 'mean',
                'total_precip': 'sum',
                'avg_humidity': 'mean',
                'year': 'count'
            }).round(2)
            
            city_summary.columns = ['Temp. Promedio (°C)', 'Precipitación Total (mm)', 
                                  'Humedad Promedio (%)', 'Registros']
            st.dataframe(city_summary, use_container_width=True)
    
    def render_data_table(self):
        """Renderizar tabla de datos avanzada con paginación real"""
        st.header("Tabla de Datos Avanzada")
        
        # Asegurar que el data_manager esté asignado al table_component
        if not hasattr(self.table_component, 'data_manager') or self.table_component.data_manager is None:
            self.table_component.set_data_manager(self.data_manager)
        
        # Selector de tipo de datos con key única para evitar recargas
        data_types = {
            'summary': 'Resumen Anual',
            'extreme': 'Días Extremos',
            'trends': 'Tendencias',
            'climate': 'Perfiles Climáticos',
            'alerts': 'Alertas Meteorológicas',
            'seasonal': 'Análisis Estacional',
            'comparison': 'Comparación Climática'
        }
        
        # Usar key única para evitar re-renderizados
        selected_data_type = st.selectbox(
            "Tipo de Datos",
            options=list(data_types.keys()),
            format_func=lambda x: data_types[x],
            help="Selecciona el tipo de datos a mostrar con paginación real",
            key="data_type_selector_main"
        )
        
        # Obtener filtros activos
        active_filters = self.filter_manager.active_filters if self.filter_manager else {}
        
        # Renderizar tabla con paginación real solo si es necesario
        if selected_data_type:
            self.table_component.render_table_with_real_pagination(
                data_type=selected_data_type,
                filters=active_filters,
                title=f"Tabla de {data_types[selected_data_type]}",
                context="main"
            )
    
    def render_interactive_maps(self):
        """Renderizar mapas interactivos con lazy loading real"""
        st.header("Mapas Interactivos")
        
        if not self.map_component:
            log_and_show_error(logger, "No se pudo inicializar el componente de mapas.", 
                             component="map_component", initialization_status="failed")
            return
        
        # Selector de tipo de mapa con key única
        map_type = self.map_component.render_map_selector("interactive")
        
        # Renderizar métrica solo para el mapa seleccionado
        if map_type in ['temperature', 'precipitation']:
            metric = self.map_component.render_metric_selector(map_type, "interactive")
        else:
            metric = 'default'
        
        # Renderizar mapa con lazy loading real - solo el seleccionado
        st.subheader("Visualización del Mapa")
        
        # Crear clave de caché para los datos del mapa
        map_cache_key = f"map_data_{map_type}_{metric}"
        
        # Verificar si los datos ya están en caché
        if map_cache_key in st.session_state:
            map_data = st.session_state[map_cache_key]
        else:
            # Obtener datos con lazy loading según el tipo de mapa seleccionado
            if map_type == 'temperature':
                map_data = self.get_data_lazy('summary')
            elif map_type == 'precipitation':
                map_data = self.get_data_lazy('summary')
            elif map_type == 'alerts':
                map_data = self.get_data_lazy('alerts')
            elif map_type == 'comparison':
                map_data = self.get_data_lazy('comparison')
            else:
                map_data = self.get_data_lazy('summary')
            
            # Aplicar filtros
            if self.filter_manager and not map_data.empty:
                map_data = self.filter_manager.apply_filters(map_data)
            
            # Guardar en caché
            st.session_state[map_cache_key] = map_data
        
        # Renderizar solo el mapa seleccionado con lazy loading real
        if not map_data.empty:
            # Usar un contenedor para evitar re-renderizados innecesarios
            with st.container():
                self.map_component.render_map_with_lazy_loading(map_data, metric, map_type, "interactive")
        else:
            log_and_show_warning(logger, "No hay datos para mostrar en el mapa con los filtros seleccionados.", 
                               map_type=map_type, metric=metric, filtered_records=len(map_data))
    
    def render_trend_analysis(self):
        """Renderizar análisis de tendencias con lazy loading"""
        # Cargar datos necesarios bajo demanda
        trends_data = self.get_data_lazy('trends')
        if not trends_data.empty:
            # Actualizar contexto de análisis con datos cargados
            self.analysis_context.data['trends'] = trends_data
            strategy = TrendAnalysisStrategy()
            self.analysis_context.execute_analysis(strategy)
        else:
            log_and_show_warning(logger, "No hay datos de tendencias disponibles.", 
                               analysis_type="trends", data_loaded=False)
    
    def render_temperature_analysis(self):
        """Renderizar análisis específico de temperatura con lazy loading"""
        # Los datos de temperatura están en summary, ya cargados
        strategy = TemperatureAnalysisStrategy()
        self.analysis_context.execute_analysis(strategy)
    
    def render_precipitation_analysis(self):
        """Renderizar análisis específico de precipitación con lazy loading"""
        # Los datos de precipitación están en summary, ya cargados
        strategy = PrecipitationAnalysisStrategy()
        self.analysis_context.execute_analysis(strategy)
    
    def render_seasonal_analysis(self):
        """Renderizar análisis estacional con lazy loading"""
        # Cargar datos estacionales bajo demanda
        seasonal_data = self.get_data_lazy('seasonal')
        if not seasonal_data.empty:
            # Actualizar contexto de análisis con datos cargados
            self.analysis_context.data['seasonal'] = seasonal_data
            strategy = SeasonalAnalysisStrategy()
            self.analysis_context.execute_analysis(strategy)
        else:
            log_and_show_warning(logger, "No hay datos estacionales disponibles.", 
                               analysis_type="seasonal", data_loaded=False)
    
    def render_alert_analysis(self):
        """Renderizar análisis de alertas con lazy loading"""
        # Cargar datos de alertas bajo demanda
        alerts_data = self.get_data_lazy('alerts')
        if not alerts_data.empty:
            # Actualizar contexto de análisis con datos cargados
            self.analysis_context.data['alerts'] = alerts_data
            strategy = AlertAnalysisStrategy()
            self.analysis_context.execute_analysis(strategy)
        else:
            log_and_show_warning(logger, "No hay datos de alertas disponibles.", 
                               analysis_type="alerts", data_loaded=False)
    
    def render_climate_comparison(self):
        """Renderizar comparación climática con lazy loading"""
        # Cargar datos de comparación bajo demanda
        comparison_data = self.get_data_lazy('comparison')
        if not comparison_data.empty:
            # Actualizar contexto de análisis con datos cargados
            self.analysis_context.data['comparison'] = comparison_data
            strategy = ClimateComparisonStrategy()
            self.analysis_context.execute_analysis(strategy)
        else:
            log_and_show_warning(logger, "No hay datos de comparación climática disponibles.", 
                               analysis_type="comparison", data_loaded=False)

    def render_similarity_page(self):
        """Página profesional de Similitud Climática"""
        st.header("Similitud Climática entre Ciudades")

        # Cargar datos necesarios bajo demanda
        similarity_df = self.get_data_lazy('similarity')
        outliers_df = self.get_data_lazy('outliers')

        if similarity_df is None or similarity_df.empty:
            log_and_show_warning(logger, "No hay datos de similitud disponibles.")
            return

        # Aplicar filtros si corresponden (por región/ciudades)
        if self.filter_manager:
            # Filtros por ciudad/region aplican sobre 'city'
            if self.filter_manager.active_filters.get('region'):
                region = self.filter_manager.active_filters['region']
                # Mantener filas donde la ciudad base pertenezca a la región
                comparison = self.get_data_lazy('comparison')
                if comparison is not None and not comparison.empty:
                    valid_cities = comparison[comparison['region'] == region]['city'].unique().tolist()
                    similarity_df = similarity_df[similarity_df['city'].isin(valid_cities)]

            if self.filter_manager.active_filters.get('cities'):
                cities = self.filter_manager.active_filters['cities']
                similarity_df = similarity_df[similarity_df['city'].isin(cities)]

        # UI profesional con pestañas
        tabs = st.tabs(["Overview", "Top Similares", "Tabla de Pares", "Outliers"])

        with tabs[0]:
            self.chart_component.render_similarity_heatmap(similarity_df, "Matriz de Similitud (Simétrica)")
        with tabs[1]:
            self.chart_component.render_top_similars(similarity_df, "Top Similares por Ciudad")
        with tabs[2]:
            self.chart_component.render_similarity_pairs_table(similarity_df, "Tabla de Pares Similares")
        with tabs[3]:
            if outliers_df is not None and not outliers_df.empty:
                self.chart_component.render_outliers_overview(outliers_df, "Outliers Climáticos")
            else:
                st.info("No hay datos de outliers para mostrar.")
    

    def render_configuration(self):
        """Renderizar página de configuración"""
        st.header("Configuración del Sistema")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Configuración de Datos")
            
            # Información de la base de datos
            st.write("**Base de datos:** DuckDB")         #ToDoo: No hardcodearlo
            st.write("**Archivo:** meteopanda.duckdb")    #ToDoo: No hardcodearlo
            
            # Botones de control
            if st.button("Limpiar Caché"):
                self.data_manager.clear_cache()
                st.success("Caché limpiado correctamente")
            
        
        with col2:
            st.subheader("Información del Sistema")
            
            # Información de datos
            if self.data:
                data_info = self.data_manager.get_data_info()
                st.write("**Datos disponibles:**")
                for key, count in data_info.items():
                    if count > 0:
                        st.write(f"• {key}: {count}")
            
            # Información de configuración
            if self.config:
                st.write("**Ciudades configuradas:**")
                for city in self.config.get('cities', []):
                    st.write(f"• {city['name']}")
    
    
    def run(self):
        """Ejecutar el dashboard"""
        # Inicializar
        if not self.initialize():
            return
        
        # Renderizar cabecera
        self.render_header()

        # Renderizar sidebar (solo filtros/controles)
        self.render_sidebar()

        # Renderizar navegación superior y contenido
        self.render_navbar()

def main():
    """Función principal"""
    try:
        # Crear y ejecutar dashboard
        dashboard = MeteoPandaDashboard()
        dashboard.run()
        
    except Exception as e:
        st.error(f"Error en el dashboard: {str(e)}")
        st.exception(e)

if __name__ == "__main__":
    main()
