"""
Componente de mapa mejorado con funcionalidades avanzadas
"""
import streamlit as st
import pandas as pd
import folium
from typing import Dict, List, Optional, Any
import numpy as np

# Importar plugins de folium con manejo de errores
try:
    from folium import plugins
except ImportError:
    # Fallback si plugins no está disponible
    plugins = None

class AdvancedMapComponent:
    """Componente de mapa avanzado con funcionalidades mejoradas y caché inteligente"""
    
    def __init__(self, coords_df: pd.DataFrame):
        self.coords_df = coords_df
        self.map_center = [40.4168, -3.7038]  # Centrado en Madrid (centro de España)
        self.default_zoom = 6
        self.map_cache = {}
        self.data_cache = {}
        self.max_cache_size = 50  # Máximo 50 mapas en caché
    
    def render_map(self, data: pd.DataFrame, metric: str = 'avg_temp', 
                   map_type: str = 'temperature', height: int = 600) -> folium.Map:
        """Renderizar mapa con métricas y funcionalidades avanzadas - optimizado con caché inteligente"""
        
        # Crear clave única para el caché del mapa
        cache_key = self._create_cache_key(data, metric, map_type)
        
        # Verificar si el mapa ya está en caché y es válido
        if self._is_cached_map_valid(cache_key, data):
            return self.map_cache[cache_key]['map']
        
        # Crear nuevo mapa
        m = self._create_base_map(map_type)
        
        if data.empty:
            st.warning("No hay datos para mostrar en el mapa.")
            return m
        
        # Procesar solo los datos necesarios para este tipo de mapa
        processed_data = self._process_data_for_map_type(data, map_type)
        
        # Añadir marcadores según el tipo de mapa (solo con datos procesados)
        if map_type == 'temperature':
            self._add_temperature_markers(m, processed_data, metric)
        elif map_type == 'precipitation':
            self._add_precipitation_markers(m, processed_data, metric)
        elif map_type == 'alerts':
            self._add_alert_markers(m, processed_data)
        elif map_type == 'comparison':
            self._add_comparison_markers(m, processed_data)
        
        # Añadir controles adicionales solo si es necesario
        self._add_map_controls(m)
        
        # Guardar en caché inteligente
        self._cache_map(cache_key, m, data)
        
        return m
    
    @st.cache_data(ttl=7200)
    def _create_base_map(_self, map_type: str) -> folium.Map:
        """Crear mapa base con configuración específica"""
        tile_options = {
            'temperature': 'CartoDB positron',
            'precipitation': 'CartoDB dark_matter',
            'alerts': 'OpenStreetMap',
            'comparison': 'Stamen Terrain'
        }
        
        m = folium.Map(
            location=_self.map_center,
            zoom_start=_self.default_zoom,
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
        
        # Añadir controles de plugins solo si están disponibles
        if plugins is not None:
            try:
                # Añadir control de pantalla completa
                plugins.Fullscreen(
                    position='topright',
                    title='Expandir mapa',
                    title_cancel='Salir pantalla completa',
                    force_separate_button=True
                ).add_to(m)
                
                # Añadir control de minimap
                plugins.MiniMap(
                    tile_layer='CartoDB positron',
                    position='bottomright',
                    width=150,
                    height=150,
                    collapsed_width=25,
                    collapsed_height=25
                ).add_to(m)
            except Exception as e:
                # Si hay algún error con los plugins, continuar sin ellos
                st.warning(f"Algunos controles del mapa no están disponibles: {str(e)}")
        else:
            # Si plugins no está disponible, usar controles básicos
            st.info("Controles avanzados del mapa no disponibles. Usando controles básicos.")
    
    def render_map_selector(self, context: str = "main") -> str:
        """Renderizar selector de tipo de mapa con lazy loading - optimizado para evitar recargas"""
        map_types = {
            'temperature': '🌡️ Temperatura',
            'precipitation': '🌧️ Precipitación',
            'alerts': '⚠️ Alertas',
            'comparison': '🌍 Comparación Climática'
        }
        
        # Inicializar session state para el mapa seleccionado
        map_key = f"selected_map_{context}"
        if map_key not in st.session_state:
            st.session_state[map_key] = 'temperature'
        
        # Obtener índice actual sin modificar session_state
        current_index = list(map_types.keys()).index(st.session_state[map_key])
        
        selected_map = st.selectbox(
            "🗺️ Tipo de Mapa",
            options=list(map_types.keys()),
            format_func=lambda x: map_types[x],
            help="Selecciona el tipo de visualización del mapa",
            key=f"map_type_selector_{context}",
            index=current_index
        )
        
        # Actualizar session state solo si cambió
        if selected_map != st.session_state[map_key]:
            st.session_state[map_key] = selected_map
            # Limpiar caché específico del mapa anterior
            self._clear_map_cache_for_type(context, selected_map)
        
        return selected_map
    
    def _clear_map_cache_for_type(self, context: str, map_type: str):
        """Limpiar caché específico para un tipo de mapa"""
        # Limpiar caché de datos del mapa en session_state
        keys_to_remove = [k for k in st.session_state.keys() 
                         if k.startswith(f"map_data_{map_type}") or 
                            k.startswith(f"cached_map_{context}_{map_type}") or
                            k.startswith(f"rendered_map_{context}_{map_type}")]
        
        for key in keys_to_remove:
            if key in st.session_state:
                del st.session_state[key]
        
        # Limpiar caché interno del componente
        keys_to_remove = [k for k in self.map_cache.keys() if map_type in k]
        for key in keys_to_remove:
            if key in self.map_cache:
                del self.map_cache[key]
    
    def render_metric_selector(self, map_type: str, context: str = "main") -> str:
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
            help="Selecciona la métrica a visualizar",
            key=f"metric_selector_{map_type}_{context}"
        )
        
        return selected_metric
    
    def render_map_with_lazy_loading(self, data: pd.DataFrame, metric: str, map_type: str, context: str = "main") -> None:
        """Renderizar mapa con lazy loading real - optimizado para evitar recargas al interactuar"""
        map_key = f"selected_map_{context}"
        current_map = st.session_state.get(map_key, 'temperature')
        
        # Solo procesar y renderizar si el mapa seleccionado coincide con el tipo actual
        if current_map == map_type:
            if not data.empty:
                # Procesar solo los datos necesarios para este tipo de mapa
                processed_data = self._process_data_for_map_type(data, map_type)
                
                if not processed_data.empty:
                    map_obj = self.render_map(processed_data, metric, map_type)
                    
                    # Usar st_folium con configuración optimizada para evitar recargas
                    from streamlit_folium import st_folium
                    # Key estable que no cambia con interacciones
                    map_component_key = f"map_{context}_{map_type}_{metric}_static"
                    st_folium(map_obj, height=600, width=1000, key=map_component_key, returned_objects=[])
                else:
                    st.warning(f"No hay datos procesados disponibles para el mapa de {map_type}")
            else:
                st.warning(f"No hay datos disponibles para el mapa de {map_type}")
        else:
            # Mostrar placeholder real - no procesar datos innecesarios
            st.info(f"Mapa de {map_type} seleccionado. Los datos se cargarán cuando se seleccione este tipo de mapa.")
    
    def _process_data_for_map_type(self, data: pd.DataFrame, map_type: str) -> pd.DataFrame:
        """Procesar solo los datos necesarios para el tipo de mapa específico"""
        if data.empty:
            return data
        
        # Filtrar columnas según el tipo de mapa para optimizar rendimiento
        if map_type == 'temperature':
            # Solo columnas relacionadas con temperatura + coordenadas
            temp_columns = [col for col in data.columns if any(x in col.lower() for x in ['temp', 'city', 'year', 'month', 'lat', 'lon'])]
            return data[temp_columns] if temp_columns else data
        
        elif map_type == 'precipitation':
            # Solo columnas relacionadas con precipitación + coordenadas
            precip_columns = [col for col in data.columns if any(x in col.lower() for x in ['precip', 'rain', 'city', 'year', 'month', 'lat', 'lon'])]
            return data[precip_columns] if precip_columns else data
        
        elif map_type == 'alerts':
            # Solo columnas relacionadas con alertas + coordenadas
            alert_columns = [col for col in data.columns if any(x in col.lower() for x in ['alert', 'severity', 'city', 'date', 'lat', 'lon'])]
            return data[alert_columns] if alert_columns else data
        
        elif map_type == 'comparison':
            # Solo columnas relacionadas con comparación climática + coordenadas
            comparison_columns = [col for col in data.columns if any(x in col.lower() for x in ['climate', 'classification', 'city', 'temp', 'precip', 'lat', 'lon'])]
            return data[comparison_columns] if comparison_columns else data
        
        else:
            # Para otros tipos, devolver datos completos
            return data
    
    def _create_cache_key(self, data: pd.DataFrame, metric: str, map_type: str) -> str:
        """Crear clave única para el caché del mapa"""
        # Crear hash de los datos relevantes
        data_hash = self._create_data_hash(data, map_type)
        
        # Crear clave compuesta
        key_parts = [map_type, metric, data_hash]
        return "_".join(key_parts)
    
    def _create_data_hash(self, data: pd.DataFrame, map_type: str) -> str:
        """Crear hash de los datos relevantes para el tipo de mapa"""
        if data.empty:
            return "empty"
        
        # Filtrar solo las columnas relevantes para el tipo de mapa
        relevant_columns = self._get_relevant_columns(map_type, data)
        filtered_data = data[relevant_columns] if relevant_columns else data
        
        # Crear hash de los datos filtrados
        try:
            data_str = str(filtered_data.values.tobytes()) + str(filtered_data.index.tolist())
            return str(hash(data_str))[:8]
        except (AttributeError, TypeError):
            # Fallback para índices que no soportan tobytes()
            data_str = str(filtered_data.values.tobytes()) + str(filtered_data.index.values.tolist())
            return str(hash(data_str))[:8]
    
    def _get_relevant_columns(self, map_type: str, data: pd.DataFrame = None) -> List[str]:
        """Obtener columnas relevantes según el tipo de mapa, solo las que existen en los datos"""
        if map_type == 'temperature':
            required_cols = ['city', 'lat', 'lon', 'avg_temp', 'max_temp', 'min_temp']
        elif map_type == 'precipitation':
            required_cols = ['city', 'lat', 'lon', 'total_precip']
        elif map_type == 'alerts':
            required_cols = ['city', 'temperature_alert', 'precipitation_alert', 'humidity_alert', 'overall_alert', 'alert_severity']
        elif map_type == 'comparison':
            required_cols = ['city', 'lat', 'lon', 'avg_temp_city', 'total_precip_city', 'climate_classification', 'climate_comfort_score']
        else:
            required_cols = ['city']
        
        # Si se proporcionan datos, filtrar solo las columnas que existen
        if data is not None and not data.empty:
            available_cols = list(data.columns)
            existing_cols = [col for col in required_cols if col in available_cols]
            # Asegurar que siempre tengamos al menos 'city'
            if 'city' not in existing_cols and 'city' in available_cols:
                existing_cols = ['city'] + [col for col in existing_cols if col != 'city']
            return existing_cols
        
        # Si no hay datos, devolver las columnas requeridas (comportamiento original)
        return required_cols
    
    
    def _is_cached_map_valid(self, cache_key: str, data: pd.DataFrame) -> bool:
        """Verificar si el mapa en caché es válido"""
        if cache_key not in self.map_cache:
            return False
        
        cached_data = self.map_cache[cache_key]['data']
        current_data_hash = self._create_data_hash(data, self.map_cache[cache_key]['map_type'])
        
        return cached_data == current_data_hash
    
    def _cache_map(self, cache_key: str, map_obj: folium.Map, data: pd.DataFrame):
        """Guardar mapa en caché con límite de tamaño"""
        # Limpiar caché si está lleno
        if len(self.map_cache) >= self.max_cache_size:
            # Eliminar el 20% más antiguo
            keys_to_remove = list(self.map_cache.keys())[:self.max_cache_size // 5]
            for key in keys_to_remove:
                del self.map_cache[key]
        
        # Guardar nuevo mapa
        self.map_cache[cache_key] = {
            'map': map_obj,
            'data': self._create_data_hash(data, cache_key.split('_')[0]),
            'map_type': cache_key.split('_')[0],
            'timestamp': st.session_state.get('_map_timestamp', 0)
        }
        st.session_state['_map_timestamp'] = st.session_state.get('_map_timestamp', 0) + 1
    
    def clear_cache(self):
        """Limpiar caché de mapas"""
        self.map_cache.clear()
        self.data_cache.clear()
    
