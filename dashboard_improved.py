"""
Dashboard mejorado de MeteoPanda con arquitectura modular y funcionalidades avanzadas
"""
import streamlit as st
import pandas as pd
import yaml
from typing import Dict, Optional
import sys
import os

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
    page_title="MeteoPanda Dashboard Pro",
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
        with st.spinner("🔄 Inicializando dashboard..."):
            self.data = self.data_manager.get_all_data()
            
            if self.data is None:
                st.error("❌ Error al cargar los datos. Verifica la conexión a la base de datos.")
                return False
            
            # Inicializar componentes que dependen de datos
            if self.data.get('coords') is not None:
                self.map_component = AdvancedMapComponent(self.data['coords'])
            
            self.filter_manager = FilterManager(self.data)
            
            return True
    
    def render_header(self):
        """Renderizar cabecera del dashboard"""
        st.title("🐼 MeteoPanda Dashboard Pro")
        st.markdown("### 🌦️ Análisis Avanzado de Datos Meteorológicos de Andalucía")
        st.markdown("---")
        
        # Mostrar información del sistema
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.info("📊 **Datos Cargados:**")
            if self.data:
                data_info = self.data_manager.get_data_info()
                for key, count in data_info.items():
                    if count > 0:
                        st.write(f"• {key}: {count} registros")
        
        with col2:
            st.info("🔧 **Funcionalidades:**")
            st.write("• Filtros Avanzados")
            st.write("• Mapas Interactivos")
            st.write("• Gráficos Dinámicos")
            st.write("• Exportación de Datos")
        
        with col3:
            st.info("📈 **KPIs Principales:**")
            if self.data and self.data.get('summary') is not None:
                summary = self.data['summary']
                if not summary.empty:
                    st.write(f"• Ciudades: {summary['city'].nunique()}")
                    st.write(f"• Años: {summary['year'].nunique()}")
                    st.write(f"• Temp. Promedio: {summary['avg_temp'].mean():.1f}°C")
                    st.write(f"• Precipitación: {summary['total_precip'].sum():.0f} mm")
    
    def render_sidebar(self):
        """Renderizar sidebar con filtros y navegación"""
        with st.sidebar:
            st.header("🎛️ Panel de Control")
            
            # Renderizar filtros
            if self.filter_manager:
                active_filters = self.filter_manager.render_filters()
            
            st.markdown("---")
            
            # Navegación
            st.header("📄 Navegación")
            page = st.radio(
                "Selecciona una página:",
                [
                    "🏠 Dashboard Principal",
                    "📊 Tabla de Datos",
                    "🗺️ Mapas Interactivos",
                    "📈 Análisis de Tendencias",
                    "🌡️ Análisis de Temperatura",
                    "🌧️ Análisis de Precipitación",
                    "🌤️ Análisis Estacional",
                    "⚠️ Alertas Meteorológicas",
                    "🌍 Comparación Climática",
                    "⚙️ Configuración"
                ]
            )
            
            st.markdown("---")
            
            # Información del sistema
            st.header("ℹ️ Información")
            st.write("**Versión:** 2.0 Pro")
            st.write("**Última actualización:** Datos en tiempo real")
            
            # Botones de control
            st.header("🔧 Controles")
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🔄 Recargar Datos"):
                    self.data_manager.clear_cache()
                    st.rerun()
            
            with col2:
                if st.button("📊 Ver Estadísticas"):
                    self._show_system_stats()
            
            return page
    
    def render_main_dashboard(self):
        """Renderizar dashboard principal"""
        st.header("🏠 Dashboard Principal")
        
        if not self.data or self.data.get('summary') is None:
            st.warning("No hay datos disponibles para mostrar el dashboard.")
            return
        
        summary_data = self.data['summary']
        
        # KPIs principales
        self.chart_component.render_kpi_dashboard(summary_data, "📊 KPIs Principales")
        
        # Mapa interactivo
        if self.map_component:
            st.subheader("🗺️ Vista General del Clima")
            
            col1, col2 = st.columns([1, 3])
            
            with col1:
                map_type = self.map_component.render_map_selector()
                metric = self.map_component.render_metric_selector(map_type)
            
            with col2:
                map_obj = self.map_component.render_map(summary_data, metric, map_type)
                st.components.v1.html(map_obj._repr_html_(), height=500)
        
        # Resumen de datos por ciudad
        st.subheader("📋 Resumen por Ciudad")
        if not summary_data.empty:
            city_summary = summary_data.groupby('city').agg({
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
        st.header("📊 Tabla de Datos Avanzada")
        
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
            "📋 Tipo de Datos",
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
        st.header("🗺️ Mapas Interactivos")
        
        if not self.map_component:
            st.error("No se pudo inicializar el componente de mapas.")
            return
        
        # Selector de tipo de mapa
        map_type = self.map_component.render_map_selector()
        
        # Obtener datos según el tipo de mapa
        if map_type == 'temperature':
            map_data = self.data.get('summary', pd.DataFrame())
            metric = self.map_component.render_metric_selector(map_type)
        elif map_type == 'precipitation':
            map_data = self.data.get('summary', pd.DataFrame())
            metric = self.map_component.render_metric_selector(map_type)
        elif map_type == 'alerts':
            map_data = self.data.get('alerts', pd.DataFrame())
            metric = 'default'
        elif map_type == 'comparison':
            map_data = self.data.get('comparison', pd.DataFrame())
            metric = 'default'
        else:
            map_data = self.data.get('summary', pd.DataFrame())
            metric = 'avg_temp'
        
        # Aplicar filtros
        if self.filter_manager and not map_data.empty:
            map_data = self.filter_manager.apply_filters(map_data)
        
        # Renderizar mapa
        if not map_data.empty:
            map_obj = self.map_component.render_map(map_data, metric, map_type)
            st.components.v1.html(map_obj._repr_html_(), height=600)
        else:
            st.warning("No hay datos para mostrar en el mapa con los filtros seleccionados.")
    
    def render_trend_analysis(self):
        """Renderizar análisis de tendencias"""
        st.header("📈 Análisis de Tendencias")
        
        trends_data = self.data.get('trends', pd.DataFrame())
        
        if not trends_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_trends = self.filter_manager.apply_filters(trends_data)
            else:
                filtered_trends = trends_data
            
            # Gráficos de tendencias
            self.chart_component.render_temperature_trends(filtered_trends, "🌡️ Tendencias de Temperatura")
        else:
            st.warning("No hay datos de tendencias disponibles.")
    
    def render_temperature_analysis(self):
        """Renderizar análisis específico de temperatura"""
        st.header("🌡️ Análisis de Temperatura")
        
        summary_data = self.data.get('summary', pd.DataFrame())
        
        if not summary_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(summary_data)
            else:
                filtered_data = summary_data
            
            # Gráficos de temperatura
            self.chart_component.render_temperature_trends(filtered_data, "🌡️ Análisis Detallado de Temperatura")
        else:
            st.warning("No hay datos de temperatura disponibles.")
    
    def render_precipitation_analysis(self):
        """Renderizar análisis específico de precipitación"""
        st.header("🌧️ Análisis de Precipitación")
        
        summary_data = self.data.get('summary', pd.DataFrame())
        
        if not summary_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(summary_data)
            else:
                filtered_data = summary_data
            
            # Gráficos de precipitación
            self.chart_component.render_precipitation_analysis(filtered_data, "🌧️ Análisis Detallado de Precipitación")
        else:
            st.warning("No hay datos de precipitación disponibles.")
    
    def render_seasonal_analysis(self):
        """Renderizar análisis estacional"""
        st.header("🌤️ Análisis Estacional")
        
        seasonal_data = self.data.get('seasonal', pd.DataFrame())
        
        if not seasonal_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(seasonal_data)
            else:
                filtered_data = seasonal_data
            
            # Gráficos estacionales
            self.chart_component.render_seasonal_analysis(filtered_data, "🌤️ Análisis Estacional Detallado")
        else:
            st.warning("No hay datos estacionales disponibles.")
    
    def render_alert_analysis(self):
        """Renderizar análisis de alertas"""
        st.header("⚠️ Análisis de Alertas Meteorológicas")
        
        alerts_data = self.data.get('alerts', pd.DataFrame())
        
        if not alerts_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(alerts_data)
            else:
                filtered_data = alerts_data
            
            # Gráficos de alertas
            self.chart_component.render_alert_analysis(filtered_data, "⚠️ Análisis de Alertas Meteorológicas")
        else:
            st.warning("No hay datos de alertas disponibles.")
    
    def render_climate_comparison(self):
        """Renderizar comparación climática"""
        st.header("🌍 Comparación Climática")
        
        comparison_data = self.data.get('comparison', pd.DataFrame())
        
        if not comparison_data.empty:
            # Aplicar filtros
            if self.filter_manager:
                filtered_data = self.filter_manager.apply_filters(comparison_data)
            else:
                filtered_data = comparison_data
            
            # Gráficos de comparación
            self.chart_component.render_climate_comparison(filtered_data, "🌍 Comparación Climática Detallada")
        else:
            st.warning("No hay datos de comparación climática disponibles.")
    
    def render_configuration(self):
        """Renderizar página de configuración"""
        st.header("⚙️ Configuración del Sistema")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔧 Configuración de Datos")
            
            # Información de la base de datos
            st.write("**Base de datos:** DuckDB")
            st.write("**Archivo:** meteopanda.duckdb")
            
            # Botones de control
            if st.button("🗑️ Limpiar Caché"):
                self.data_manager.clear_cache()
                st.success("Caché limpiado correctamente")
            
            if st.button("📊 Verificar Datos"):
                self._show_system_stats()
        
        with col2:
            st.subheader("📋 Información del Sistema")
            
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
    
    def _show_system_stats(self):
        """Mostrar estadísticas del sistema"""
        st.subheader("📊 Estadísticas del Sistema")
        
        if self.data:
            data_info = self.data_manager.get_data_info()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.write("**Datos Cargados:**")
                for key, count in data_info.items():
                    if count > 0:
                        st.write(f"• {key}: {count}")
            
            with col2:
                st.write("**Filtros Activos:**")
                if self.filter_manager:
                    filter_summary = self.filter_manager.get_filter_summary()
                    st.write(f"• Total filtros: {filter_summary['total_filters']}")
                    st.write(f"• Impacto en datos: {filter_summary['data_impact']}")
            
            with col3:
                st.write("**Rendimiento:**")
                st.write("• Caché activo")
                st.write("• Conexión estable")
                st.write("• Componentes cargados")
    
    def run(self):
        """Ejecutar el dashboard"""
        # Inicializar
        if not self.initialize():
            return
        
        # Renderizar cabecera
        self.render_header()
        
        # Renderizar sidebar y obtener página seleccionada
        selected_page = self.render_sidebar()
        
        # Renderizar página seleccionada
        if selected_page == "🏠 Dashboard Principal":
            self.render_main_dashboard()
        elif selected_page == "📊 Tabla de Datos":
            self.render_data_table()
        elif selected_page == "🗺️ Mapas Interactivos":
            self.render_interactive_maps()
        elif selected_page == "📈 Análisis de Tendencias":
            self.render_trend_analysis()
        elif selected_page == "🌡️ Análisis de Temperatura":
            self.render_temperature_analysis()
        elif selected_page == "🌧️ Análisis de Precipitación":
            self.render_precipitation_analysis()
        elif selected_page == "🌤️ Análisis Estacional":
            self.render_seasonal_analysis()
        elif selected_page == "⚠️ Alertas Meteorológicas":
            self.render_alert_analysis()
        elif selected_page == "🌍 Comparación Climática":
            self.render_climate_comparison()
        elif selected_page == "⚙️ Configuración":
            self.render_configuration()

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
