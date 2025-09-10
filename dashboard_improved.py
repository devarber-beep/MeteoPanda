"""
Dashboard pro MeteoPanda
"""
import streamlit as st
import pandas as pd
import yaml
from typing import Dict, Optional
import sys
import os
from streamlit_option_menu import option_menu

# Añadir el directorio src al path para importar módulos
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Importar componentes del dashboard
from src.dashboard.data_manager import DataManager
from src.dashboard.filter_manager import FilterManager
from src.dashboard.table_component import AdvancedTableComponent
from src.dashboard.map_component import AdvancedMapComponent
from src.dashboard.chart_component import AdvancedChartComponent

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
            return yaml.safe_load(file)
    except Exception as e:
        st.error(f"Error cargando configuración: {str(e)}")
        return None

class MeteoPandaDashboard:
    """Dashboard principal de MeteoPanda con arquitectura modular"""
    
    def __init__(self):
        self.data_manager = DataManager()
        self.config = load_config()
        self.data = None
        self.filter_manager = None
        self.table_component = AdvancedTableComponent()
        self.map_component = None
        self.chart_component = AdvancedChartComponent()
    
    def initialize(self):
        """Inicializar el dashboard"""
        # Cargar datos
        with st.spinner("Inicializando dashboard..."):
            self.data = self.data_manager.get_all_data()
            
            if self.data is None:
                st.error("Error al cargar los datos. Verifica la conexión a la base de datos.")
                return False
            
            # Inicializar componentes que dependen de datos
            if self.data.get('coords') is not None:
                self.map_component = AdvancedMapComponent(self.data['coords'])
            
            # Inicializar filtros sin aplicar ninguno
            self.filter_manager = FilterManager(self.data)
            
            return True
    
    def render_header(self):
        """Renderizar cabecera del dashboard"""
        st.title("🐼 MeteoPanda Dashboard Pro")
    
    def render_sidebar(self):
        """Renderizar sidebar solo con filtros e información"""
        with st.sidebar:           
            # Renderizar filtros
            if self.filter_manager:
                _ = self.filter_manager.render_filters()
            
            st.markdown("---")
            
            # Información del sistema
            st.header("Información")
            st.write("**Versión:** 2.0 Pro")
            
            # Botones de control
            st.header("Controles")
            if st.button("Recargar Datos"):
                self.data_manager.clear_cache()
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
            "Configuración"
        ]
        
        # Crear el navbar horizontal
        selected = option_menu(
            menu_title=None,
            options=menu_options,
            icons=['house', 'table', 'map', 'trending-up', 'thermometer-half', 
                   'cloud-rain', 'calendar', 'exclamation-triangle', 'globe', 'gear'],
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
        elif selected == "Configuración":
            self.render_configuration()
    
    def render_main_dashboard(self):
        """Renderizar dashboard principal"""
        st.header("Dashboard Principal")
        
        if not self.data or self.data.get('summary') is None:
            st.warning("No hay datos disponibles para mostrar el dashboard.")
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
                # Renderizar solo el mapa seleccionado
                if not filtered_summary_data.empty:
                    map_obj = self.map_component.render_map(filtered_summary_data, metric, map_type)
                    from streamlit_folium import st_folium
                    st_folium(map_obj, height=500, width=900, key=f"main_map_{map_type}")
                else:
                    st.warning("No hay datos para mostrar en el mapa.")
        
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
        """Renderizar tabla de datos avanzada"""
        st.header("Tabla de Datos Avanzada")
        
        # Selector de tipo de datos
        data_types = {
            'summary': 'Resumen Anual',
            'extreme': 'Días Extremos',
            'trends': 'Tendencias',
            'climate': 'Perfiles Climáticos',
            'alerts': 'Alertas Meteorológicas',
            'seasonal': 'Análisis Estacional',
            'comparison': 'Comparación Climática'
        }
        
        selected_data_type = st.selectbox(
            "Tipo de Datos",
            options=list(data_types.keys()),
            format_func=lambda x: data_types[x],
            help="Selecciona el tipo de datos a mostrar"
        )
        
        # Obtener datos seleccionados
        selected_data = self.data.get(selected_data_type, pd.DataFrame())
        
        if not selected_data.empty:
            # Aplicar filtros si están disponibles
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(selected_data)
            else:
                filtered_data = selected_data
            
            # Renderizar tabla
            self.table_component.render_table(
                filtered_data, 
                self.filter_manager.active_filters if self.filter_manager else {},
                f"Tabla de {data_types[selected_data_type]}"
            )
        else:
            st.warning(f"No hay datos disponibles para {data_types[selected_data_type]}.")
    
    def render_interactive_maps(self):
        """Renderizar mapas interactivos"""
        st.header("Mapas Interactivos")
        
        if not self.map_component:
            st.error("No se pudo inicializar el componente de mapas.")
            return
        
        # Selector de tipo de mapa
        map_type = self.map_component.render_map_selector("interactive")
        
        # Renderizar métrica solo para el mapa seleccionado
        if map_type in ['temperature', 'precipitation']:
            metric = self.map_component.render_metric_selector(map_type, "interactive")
        else:
            metric = 'default'
        
        # Renderizar mapa con lazy loading - solo el seleccionado
        st.subheader("Visualización del Mapa")
        
        # Obtener datos según el tipo de mapa seleccionado
        if map_type == 'temperature':
            map_data = self.data.get('summary', pd.DataFrame())
        elif map_type == 'precipitation':
            map_data = self.data.get('summary', pd.DataFrame())
        elif map_type == 'alerts':
            map_data = self.data.get('alerts', pd.DataFrame())
        elif map_type == 'comparison':
            map_data = self.data.get('comparison', pd.DataFrame())
        else:
            map_data = self.data.get('summary', pd.DataFrame())
        
        # Aplicar filtros
        if self.filter_manager and not map_data.empty:
            map_data = self.filter_manager.apply_filters(map_data)
        
        # Renderizar solo el mapa seleccionado
        if not map_data.empty:
            map_obj = self.map_component.render_map(map_data, metric, map_type)
            from streamlit_folium import st_folium
            st_folium(map_obj, height=600, width=1000, key=f"interactive_map_{map_type}")
        else:
            st.warning("No hay datos para mostrar en el mapa con los filtros seleccionados.")
    
    def render_trend_analysis(self):
        """Renderizar análisis de tendencias"""
        st.header("Análisis de Tendencias")
        
        trends_data = self.data.get('trends', pd.DataFrame())
        
        if not trends_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_trends = self.filter_manager.apply_filters(trends_data)
            else:
                filtered_trends = trends_data
            
            # Gráficos de tendencias
            self.chart_component.render_temperature_trends(filtered_trends, "Tendencias de Temperatura")
        else:
            st.warning("No hay datos de tendencias disponibles.")
    
    def render_temperature_analysis(self):
        """Renderizar análisis específico de temperatura"""
        st.header("Análisis de Temperatura")
        
        summary_data = self.data.get('summary', pd.DataFrame())
        
        if not summary_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(summary_data)
            else:
                filtered_data = summary_data
            
            # Gráficos de temperatura
            self.chart_component.render_temperature_trends(filtered_data, "Análisis Detallado de Temperatura")
        else:
            st.warning("No hay datos de temperatura disponibles.")
    
    def render_precipitation_analysis(self):
        """Renderizar análisis específico de precipitación"""
        st.header("Análisis de Precipitación")
        
        summary_data = self.data.get('summary', pd.DataFrame())
        
        if not summary_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(summary_data)
            else:
                filtered_data = summary_data
            
            # Gráficos de precipitación
            self.chart_component.render_precipitation_analysis(filtered_data, "Análisis Detallado de Precipitación")
        else:
            st.warning("No hay datos de precipitación disponibles.")
    
    def render_seasonal_analysis(self):
        """Renderizar análisis estacional"""
        st.header("Análisis Estacional")
        
        seasonal_data = self.data.get('seasonal', pd.DataFrame())
        
        if not seasonal_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(seasonal_data)
            else:
                filtered_data = seasonal_data
            
            # Gráficos estacionales
            self.chart_component.render_seasonal_analysis(filtered_data, "Análisis Estacional Detallado")
        else:
            st.warning("No hay datos estacionales disponibles.")
    
    def render_alert_analysis(self):
        """Renderizar análisis de alertas"""
        st.header("Análisis de Alertas Meteorológicas")
        
        alerts_data = self.data.get('alerts', pd.DataFrame())
        
        if not alerts_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(alerts_data)
            else:
                filtered_data = alerts_data
            
            # Gráficos de alertas
            self.chart_component.render_alert_analysis(filtered_data, "Análisis de Alertas Meteorológicas")
        else:
            st.warning("No hay datos de alertas disponibles.")
    
    def render_climate_comparison(self):
        """Renderizar comparación climática"""
        st.header("Comparación Climática")
        
        comparison_data = self.data.get('comparison', pd.DataFrame())
        
        if not comparison_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(comparison_data)
            else:
                filtered_data = comparison_data
            
            # Gráficos de comparación
            self.chart_component.render_climate_comparison(filtered_data, "Comparación Climática Detallada")
        else:
            st.warning("No hay datos de comparación climática disponibles.")
    
    def render_configuration(self):
        """Renderizar página de configuración"""
        st.header("Configuración del Sistema")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Configuración de Datos")
            
            # Información de la base de datos
            st.write("**Base de datos:** DuckDB")
            st.write("**Archivo:** meteopanda.duckdb")
            
            # Botones de control
            if st.button("Limpiar Caché"):
                self.data_manager.clear_cache()
                st.success("Caché limpiado correctamente")
            
            if st.button("Verificar Datos"):
                self._show_system_stats()
        
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
