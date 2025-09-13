"""
Gestor de filtros para el dashboard con validaciones y opciones avanzadas
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

class FilterManager:
    """Gestor de filtros con validaciones y opciones avanzadas"""
    
    def __init__(self, data: Dict[str, pd.DataFrame]):
        self.data = data
        self.summary = data.get('summary', pd.DataFrame()) if data else pd.DataFrame()
        self.active_filters = {}
    
    def render_filters(self) -> Dict[str, Any]:
        """Renderizar filtros en sidebar con validaciones"""
        with st.sidebar:
            # Obtener opciones disponibles
            filter_options = self._get_filter_options()
            
            # Envolver todos los filtros en un expander
            with st.expander("Filtros Avanzados", expanded=False):
                
                # Filtros principales
                self._render_date_filters(filter_options)
                self._render_location_filters(filter_options)
                self._render_weather_filters(filter_options)
                self._render_advanced_filters()
                
                st.divider()
                
                # Botones de control
                self._render_control_buttons()
            
            return self.active_filters
    
    def _get_filter_options(self) -> Dict[str, List]:
        """Obtener opciones disponibles para los filtros"""
        options = {
            'years': [],
            'months': [],
            'regions': [],
            'cities': [],
            'seasons': ['Invierno', 'Primavera', 'Verano', 'Otoño'],
            'alert_levels': ['Normal', 'ALERTA AMARILLA', 'ALERTA NARANJA', 'ALERTA ROJA']
        }
        
        # Verificar que self.summary existe y no está vacío
        if self.summary is not None and not self.summary.empty:
            try:
                options['years'] = sorted(self.summary['year'].unique().tolist())
                options['months'] = sorted(self.summary['month'].unique().tolist())
                options['regions'] = sorted(self.summary['region'].unique().tolist())
                options['cities'] = sorted(self.summary['city'].unique().tolist())
            except (KeyError, AttributeError):
                # Si hay algún error al acceder a las columnas, mantener las opciones por defecto
                pass
        
        return options
    
    def _render_date_filters(self, options: Dict[str, List]):
        """Renderizar filtros de fecha"""
        st.subheader("Filtros de Fecha")
        # Filtro de año
        selected_year = st.selectbox(
            "Año",
            options=['Todos'] + [str(y) for y in options['years']],
            index=0,
            help="Selecciona un año específico o 'Todos' para ver todos los años",
            key="filter_year"
        )
        
        # Filtro de mes
        selected_month = st.selectbox(
            "Mes",
            options=['Todos'] + [str(m) for m in options['months']],
            index=0,
            help="Selecciona un mes específico o 'Todos' para ver todos los meses",
            key="filter_month"
        )
        
        # Filtro de rango de fechas
        if selected_year != 'Todos':
            year = int(selected_year)
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            
            date_range = st.date_input(
                "Rango de fechas",
                value=(start_date, end_date),
                min_value=start_date,
                max_value=end_date,
                help="Selecciona un rango de fechas específico"
            )
        else:
            date_range = None
        
        self.active_filters.update({
            'year': selected_year if selected_year != 'Todos' else None,
            'month': selected_month if selected_month != 'Todos' else None,
            'date_range': date_range
        })
    
    def _render_location_filters(self, options: Dict[str, List]):
        """Renderizar filtros de ubicación"""
        st.subheader("Filtros de Ubicación")
        # Filtro de región
        selected_region = st.selectbox(
            "Región",
            options=['Todas'] + options['regions'],
            index=0,
            help="Selecciona una región específica o 'Todas' para ver todas las regiones",
            key="filter_region"
        )
        
        # Filtro de ciudades
        if selected_region != 'Todas' and self.summary is not None and not self.summary.empty:
            try:
                # Filtrar ciudades por región seleccionada
                region_cities = self.summary[self.summary['region'] == selected_region]['city'].unique()
                available_cities = sorted(region_cities)
            except (KeyError, AttributeError):
                available_cities = options['cities']
        else:
            available_cities = options['cities']
        
        selected_cities = st.multiselect(
            "Ciudades",
            options=available_cities,
            default=[],  # Sin ciudades seleccionadas por defecto
            help="Selecciona una o más ciudades. Por defecto no hay ninguna seleccionada."
        )
        
        self.active_filters.update({
            'region': selected_region if selected_region != 'Todas' else None,
            'cities': selected_cities
        })
    
    def _render_weather_filters(self, options: Dict[str, List]):
        """Renderizar filtros meteorológicos"""
        st.subheader("Filtros Meteorológicos")
        # Filtro de estación
        selected_season = st.selectbox(
            "Estación",
            options=['Todas'] + options['seasons'],
            index=0,
            help="Selecciona una estación específica o 'Todos' para ver todas las estaciones",
            key="filter_season"
        )
        
        # Filtro de nivel de alerta
        selected_alert_level = st.selectbox(
            "Nivel de Alerta",
            options=['Todos'] + options['alert_levels'],
            index=0,
            help="Selecciona un nivel de alerta específico o 'Todos' para ver todas las alertas",
            key="filter_alert_level"
        )
        
        # Filtros de temperatura
        col1, col2 = st.columns(2)
        with col1:
            min_temp = st.number_input(
                "Temp. Mínima (°C)",
                min_value=-50.0,
                max_value=50.0,
                value=-50.0,
                step=1.0,
                help="Temperatura mínima para filtrar"
            )
        
        with col2:
            max_temp = st.number_input(
                "Temp. Máxima (°C)",
                min_value=-50.0,
                max_value=50.0,
                value=50.0,
                step=1.0,
                help="Temperatura máxima para filtrar"
            )
        
        # Filtro de precipitación
        max_precip = st.slider(
            "Precipitación Máxima (mm)",
            min_value=0.0,
            max_value=500.0,
            value=500.0,
            step=10.0,
            help="Precipitación máxima para filtrar"
        )
        
        self.active_filters.update({
            'season': selected_season if selected_season != 'Todas' else None,
            'alert_level': selected_alert_level if selected_alert_level != 'Todos' else None,
            'min_temp': min_temp,
            'max_temp': max_temp,
            'max_precip': max_precip
        })
    
    def _render_advanced_filters(self):
        """Renderizar filtros avanzados"""
        st.subheader("Filtros Avanzados")
        # Filtro de fuente de datos
        sources = ['Todas', 'AEMET', 'Meteostat']
        selected_source = st.selectbox(
            "Fuente de Datos",
            options=sources,
            index=0,
            help="Selecciona la fuente de datos específica",
            key="filter_source"
        )
        
        self.active_filters.update({
            'source': selected_source if selected_source != 'Todas' else None
        })
    
    def _render_control_buttons(self):
        """Renderizar botones de control"""
        st.subheader("Controles")
        
        # Botón de reset
        if st.button("Resetear", help="Limpiar todos los filtros", use_container_width=True):
            self.active_filters.clear()
            st.rerun()
    
    

    
    def apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplicar filtros a un DataFrame"""
        if df.empty:
            return df
        
        # Si no hay filtros activos, devolver todos los datos
        if not self.active_filters or all(v is None or v == [] for v in self.active_filters.values()):
            return df
        
        filtered_df = df.copy()
        
        # Aplicar filtros de fecha
        if self.active_filters.get('year'):
            filtered_df = filtered_df[filtered_df['year'] == int(self.active_filters['year'])]
        
        if self.active_filters.get('month'):
            filtered_df = filtered_df[filtered_df['month'] == int(self.active_filters['month'])]
        
        # Aplicar filtros de ubicación
        if self.active_filters.get('region') and 'region' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['region'] == self.active_filters['region']]
        
        if self.active_filters.get('cities') and 'city' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['city'].isin(self.active_filters['cities'])]
        
        # Aplicar filtros meteorológicos
        if self.active_filters.get('min_temp') and 'temp_max_c' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['temp_max_c'] >= self.active_filters['min_temp']]
        
        if self.active_filters.get('max_temp') and 'temp_max_c' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['temp_max_c'] <= self.active_filters['max_temp']]
        
        if self.active_filters.get('max_precip') and 'precip_mm' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['precip_mm'] <= self.active_filters['max_precip']]
        
        # Aplicar filtros de fuente
        if self.active_filters.get('source') and 'source' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['source'] == self.active_filters['source']]
        
        return filtered_df
