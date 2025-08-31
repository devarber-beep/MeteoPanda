"""
Componente de mapa mejorado con funcionalidades avanzadas
"""
import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
from typing import Dict, List, Optional, Any
import numpy as np

class AdvancedMapComponent:
    """Componente de mapa avanzado con funcionalidades mejoradas"""
    
    def __init__(self, coords_df: pd.DataFrame):
        self.coords_df = coords_df
        self.map_center = [37.3891, -5.9845]  # Centrado en Sevilla
        self.default_zoom = 7
    
    def render_map(self, data: pd.DataFrame, metric: str = 'avg_temp', 
                   map_type: str = 'temperature', height: int = 600) -> folium.Map:
        """Renderizar mapa con métricas y funcionalidades avanzadas"""
        
        # Crear mapa base
        m = self._create_base_map(map_type)
        
        if data.empty:
            st.warning("No hay datos para mostrar en el mapa.")
            return m
        
        # Añadir marcadores según el tipo de mapa
        if map_type == 'temperature':
            self._add_temperature_markers(m, data, metric)
        elif map_type == 'precipitation':
            self._add_precipitation_markers(m, data, metric)
        elif map_type == 'alerts':
            self._add_alert_markers(m, data)
        elif map_type == 'comparison':
            self._add_comparison_markers(m, data)
        
        # Añadir controles adicionales
        self._add_map_controls(m)
        
        return m
    
    def _create_base_map(self, map_type: str) -> folium.Map:
        """Crear mapa base con configuración específica"""
        tile_options = {
            'temperature': 'CartoDB positron',
            'precipitation': 'CartoDB dark_matter',
            'alerts': 'OpenStreetMap',
            'comparison': 'Stamen Terrain'
        }
        
        m = folium.Map(
            location=self.map_center,
            zoom_start=self.default_zoom,
            tiles=tile_options.get(map_type, 'CartoDB positron'),
            control_scale=True
        )
        
        return m
    
    def _add_temperature_markers(self, m: folium.Map, data: pd.DataFrame, metric: str):
        """Añadir marcadores de temperatura"""
        for _, row in data.iterrows():
            city_name = row['city']
            city_coords = self.coords_df[self.coords_df['city'] == city_name]
            
            if not city_coords.empty:
                lat, lon = city_coords.iloc[0]['lat'], city_coords.iloc[0]['lon']
                value = row.get(metric, 'N/A')
                
                # Color basado en la temperatura
                color = self._get_temperature_color(value)
                
                # Tamaño del marcador basado en la importancia
                radius = self._get_marker_radius(value, metric)
                
                # Popup con información detallada
                popup_html = self._create_temperature_popup(row, city_name)
                
                # Tooltip con información rápida
                tooltip_text = f"{city_name.capitalize()}: {value}°C"
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radius,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=tooltip_text,
                    color=color,
                    fill=True,
                    fillOpacity=0.7,
                    weight=2
                ).add_to(m)
    
    def _add_precipitation_markers(self, m: folium.Map, data: pd.DataFrame, metric: str):
        """Añadir marcadores de precipitación"""
        for _, row in data.iterrows():
            city_name = row['city']
            city_coords = self.coords_df[self.coords_df['city'] == city_name]
            
            if not city_coords.empty:
                lat, lon = city_coords.iloc[0]['lat'], city_coords.iloc[0]['lon']
                value = row.get(metric, 'N/A')
                
                # Color basado en la precipitación
                color = self._get_precipitation_color(value)
                
                # Tamaño del marcador
                radius = self._get_marker_radius(value, metric)
                
                # Popup con información detallada
                popup_html = self._create_precipitation_popup(row, city_name)
                
                # Tooltip
                tooltip_text = f"{city_name.capitalize()}: {value} mm"
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radius,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=tooltip_text,
                    color=color,
                    fill=True,
                    fillOpacity=0.7,
                    weight=2
                ).add_to(m)
    
    def _add_alert_markers(self, m: folium.Map, data: pd.DataFrame):
        """Añadir marcadores de alertas"""
        for _, row in data.iterrows():
            city_name = row['city']
            city_coords = self.coords_df[self.coords_df['city'] == city_name]
            
            if not city_coords.empty:
                lat, lon = city_coords.iloc[0]['lat'], city_coords.iloc[0]['lon']
                alert_level = row.get('overall_alert', 'Normal')
                severity = row.get('alert_severity', 1)
                
                # Color basado en la severidad de la alerta
                color = self._get_alert_color(alert_level)
                
                # Tamaño basado en la severidad
                radius = 8 + (severity * 2)
                
                # Popup con información de alerta
                popup_html = self._create_alert_popup(row, city_name)
                
                # Tooltip
                tooltip_text = f"{city_name.capitalize()}: {alert_level}"
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radius,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=tooltip_text,
                    color=color,
                    fill=True,
                    fillOpacity=0.8,
                    weight=3
                ).add_to(m)
    
    def _add_comparison_markers(self, m: folium.Map, data: pd.DataFrame):
        """Añadir marcadores para comparación climática"""
        for _, row in data.iterrows():
            city_name = row['city']
            city_coords = self.coords_df[self.coords_df['city'] == city_name]
            
            if not city_coords.empty:
                lat, lon = city_coords.iloc[0]['lat'], city_coords.iloc[0]['lon']
                climate_type = row.get('climate_classification', 'Desconocido')
                avg_temp = row.get('avg_temp_city', 'N/A')
                
                # Color basado en el tipo de clima
                color = self._get_climate_color(climate_type)
                
                # Tamaño basado en la temperatura promedio
                radius = self._get_climate_radius(avg_temp)
                
                # Popup con información climática
                popup_html = self._create_climate_popup(row, city_name)
                
                # Tooltip
                tooltip_text = f"{city_name.capitalize()}: {climate_type}"
                
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=radius,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=tooltip_text,
                    color=color,
                    fill=True,
                    fillOpacity=0.7,
                    weight=2
                ).add_to(m)
    
    def _get_temperature_color(self, value) -> str:
        """Obtener color basado en la temperatura"""
        if pd.isna(value) or value == 'N/A':
            return 'gray'
        
        try:
            temp = float(value)
            if temp < 10:
                return 'blue'
            elif temp < 20:
                return 'green'
            elif temp < 30:
                return 'orange'
            else:
                return 'red'
        except:
            return 'gray'
    
    def _get_precipitation_color(self, value) -> str:
        """Obtener color basado en la precipitación"""
        if pd.isna(value) or value == 'N/A':
            return 'gray'
        
        try:
            precip = float(value)
            if precip < 50:
                return 'yellow'
            elif precip < 100:
                return 'orange'
            else:
                return 'red'
        except:
            return 'gray'
    
    def _get_alert_color(self, alert_level: str) -> str:
        """Obtener color basado en el nivel de alerta"""
        alert_colors = {
            'Normal': 'green',
            'ALERTA AMARILLA': 'yellow',
            'ALERTA NARANJA': 'orange',
            'ALERTA ROJA': 'red'
        }
        return alert_colors.get(alert_level, 'gray')
    
    def _get_climate_color(self, climate_type: str) -> str:
        """Obtener color basado en el tipo de clima"""
        climate_colors = {
            'Clima Mediterráneo Seco': 'red',
            'Clima Mediterráneo': 'orange',
            'Clima Templado': 'green',
            'Clima Fresco': 'blue',
            'Clima Frío': 'purple'
        }
        return climate_colors.get(climate_type, 'gray')
    
    def _get_marker_radius(self, value, metric: str) -> int:
        """Obtener radio del marcador basado en el valor"""
        if pd.isna(value) or value == 'N/A':
            return 5
        
        try:
            val = float(value)
            if metric == 'avg_temp':
                # Radio basado en temperatura (5-15)
                return max(5, min(15, int(val / 3)))
            elif 'precip' in metric:
                # Radio basado en precipitación (5-20)
                return max(5, min(20, int(val / 10)))
            else:
                return 10
        except:
            return 10
    
    def _get_climate_radius(self, avg_temp) -> int:
        """Obtener radio para marcadores climáticos"""
        if pd.isna(avg_temp) or avg_temp == 'N/A':
            return 8
        
        try:
            temp = float(avg_temp)
            return max(6, min(18, int(temp / 2)))
        except:
            return 10
    
    def _create_temperature_popup(self, row: pd.Series, city_name: str) -> str:
        """Crear popup HTML para marcadores de temperatura"""
        return f"""
        <div style="width: 250px;">
            <h4>🌡️ {city_name.capitalize()}</h4>
            <hr>
            <p><b>🌡️ Temperatura Promedio:</b> {row.get('avg_temp', 'N/A')}°C</p>
            <p><b>🔥 Temperatura Máxima:</b> {row.get('max_temp', 'N/A')}°C</p>
            <p><b>❄️ Temperatura Mínima:</b> {row.get('min_temp', 'N/A')}°C</p>
            <p><b>🌧️ Precipitación Total:</b> {row.get('total_precip', 'N/A')} mm</p>
            <p><b>💧 Humedad Promedio:</b> {row.get('avg_humidity', 'N/A')}%</p>
            <p><b>📅 Año:</b> {row.get('year', 'N/A')}</p>
            <p><b>📆 Mes:</b> {row.get('month', 'N/A')}</p>
        </div>
        """
    
    def _create_precipitation_popup(self, row: pd.Series, city_name: str) -> str:
        """Crear popup HTML para marcadores de precipitación"""
        return f"""
        <div style="width: 250px;">
            <h4>🌧️ {city_name.capitalize()}</h4>
            <hr>
            <p><b>🌧️ Precipitación Total:</b> {row.get('total_precip', 'N/A')} mm</p>
            <p><b>🌡️ Temperatura Promedio:</b> {row.get('avg_temp', 'N/A')}°C</p>
            <p><b>💧 Humedad Promedio:</b> {row.get('avg_humidity', 'N/A')}%</p>
            <p><b>☀️ Horas de Sol:</b> {row.get('total_sunshine', 'N/A')} h</p>
            <p><b>📅 Año:</b> {row.get('year', 'N/A')}</p>
            <p><b>📆 Mes:</b> {row.get('month', 'N/A')}</p>
        </div>
        """
    
    def _create_alert_popup(self, row: pd.Series, city_name: str) -> str:
        """Crear popup HTML para marcadores de alertas"""
        return f"""
        <div style="width: 250px;">
            <h4>⚠️ {city_name.capitalize()}</h4>
            <hr>
            <p><b>🚨 Alerta General:</b> {row.get('overall_alert', 'N/A')}</p>
            <p><b>🌡️ Alerta Temperatura:</b> {row.get('temperature_alert', 'N/A')}</p>
            <p><b>🌧️ Alerta Precipitación:</b> {row.get('precipitation_alert', 'N/A')}</p>
            <p><b>💧 Alerta Humedad:</b> {row.get('humidity_alert', 'N/A')}</p>
            <p><b>📊 Severidad:</b> {row.get('alert_severity', 'N/A')}/5</p>
            <p><b>📅 Fecha:</b> {row.get('date', 'N/A')}</p>
            <p><b>🌡️ Temp. Máxima:</b> {row.get('temp_max_c', 'N/A')}°C</p>
            <p><b>🌧️ Precipitación:</b> {row.get('precip_mm', 'N/A')} mm</p>
        </div>
        """
    
    def _create_climate_popup(self, row: pd.Series, city_name: str) -> str:
        """Crear popup HTML para marcadores climáticos"""
        return f"""
        <div style="width: 250px;">
            <h4>🌍 {city_name.capitalize()}</h4>
            <hr>
            <p><b>🌡️ Temperatura Promedio:</b> {row.get('avg_temp_city', 'N/A')}°C</p>
            <p><b>🌧️ Precipitación Total:</b> {row.get('total_precip_city', 'N/A')} mm</p>
            <p><b>💧 Humedad Promedio:</b> {row.get('avg_humidity_city', 'N/A')}%</p>
            <p><b>🌡️ Clasificación:</b> {row.get('climate_classification', 'N/A')}</p>
            <p><b>🔥 Ranking Calor:</b> {row.get('heat_rank_in_region', 'N/A')}</p>
            <p><b>🌧️ Ranking Precipitación:</b> {row.get('precip_rank_in_region', 'N/A')}</p>
            <p><b>📊 Días Calurosos:</b> {row.get('total_hot_days', 'N/A')}</p>
            <p><b>📊 Días Lluviosos:</b> {row.get('total_rainy_days', 'N/A')}</p>
        </div>
        """
    
    def _add_map_controls(self, m: folium.Map):
        """Añadir controles adicionales al mapa"""
        # Añadir control de capas
        folium.LayerControl().add_to(m)
        
        # Añadir control de pantalla completa
        folium.plugins.Fullscreen(
            position='topright',
            title='Expandir mapa',
            title_cancel='Salir pantalla completa',
            force_separate_button=True
        ).add_to(m)
        
        # Añadir control de minimap
        folium.plugins.MiniMap(
            tile_layer='CartoDB positron',
            position='bottomright',
            width=150,
            height=150,
            collapsed_width=25,
            collapsed_height=25
        ).add_to(m)
    
    def render_map_selector(self) -> str:
        """Renderizar selector de tipo de mapa"""
        map_types = {
            'temperature': '🌡️ Temperatura',
            'precipitation': '🌧️ Precipitación',
            'alerts': '⚠️ Alertas',
            'comparison': '🌍 Comparación Climática'
        }
        
        selected_map = st.selectbox(
            "🗺️ Tipo de Mapa",
            options=list(map_types.keys()),
            format_func=lambda x: map_types[x],
            help="Selecciona el tipo de visualización del mapa"
        )
        
        return selected_map
    
    def render_metric_selector(self, map_type: str) -> str:
        """Renderizar selector de métrica según el tipo de mapa"""
        if map_type == 'temperature':
            metrics = {
                'avg_temp': 'Temperatura Promedio',
                'max_temp': 'Temperatura Máxima',
                'min_temp': 'Temperatura Mínima'
            }
        elif map_type == 'precipitation':
            metrics = {
                'total_precip': 'Precipitación Total',
                'precip_mm': 'Precipitación Diaria'
            }
        else:
            metrics = {'default': 'Métrica por defecto'}
        
        selected_metric = st.selectbox(
            "📊 Métrica",
            options=list(metrics.keys()),
            format_func=lambda x: metrics[x],
            help="Selecciona la métrica a visualizar"
        )
        
        return selected_metric
