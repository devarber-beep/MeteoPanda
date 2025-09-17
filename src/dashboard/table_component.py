"""
Componente de tabla mejorado con paginación, exportación y funcionalidades avanzadas
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import io

class AdvancedTableComponent:
    """Componente de tabla avanzado con paginación real y exportación"""
    
    def __init__(self, items_per_page: int = 50):
        self.items_per_page = items_per_page
        self.data_manager = None  # Se asignará desde el dashboard
    
    def render_table(self, data: pd.DataFrame, filters: Dict[str, Any], title: str = "Tabla de Datos"):
        """Renderizar tabla con paginación y funcionalidades avanzadas"""
        st.header(title)
        
        if data.empty:
            # Log interno para debugging (no necesita Streamlit aquí)
            import logging
            logger = logging.getLogger("table_component")
            logger.warning("No hay datos para mostrar con los filtros seleccionados", 
                          extra_data={"filters": filters, "data_shape": data.shape})
            st.warning("No hay datos para mostrar con los filtros seleccionados.")
            return
        
        # Mostrar filtros aplicados
        self._show_active_filters(filters)
        
        # Preparar datos para la tabla
        table_data = self._prepare_table_data(data)
        
        # Mostrar estadísticas de datos
        st.write(f"📊 Total de registros: {len(table_data)}")
        
        # Mostrar tabla
        st.dataframe(table_data, use_container_width=True, hide_index=True)
        
        # Funcionalidades de exportación
        self._render_export_section(table_data, "legacy", "main")
    
    def render_table_with_real_pagination(self, data_type: str, filters: Dict[str, Any], title: str = "Tabla de Datos", context: str = "main"):
        """Renderizar tabla con paginación real desde base de datos"""
        st.header(title)
        
        if not self.data_manager:
            st.error("DataManager no está disponible para paginación real")
            return
        
        # Inicializar session state para paginación
        page_key = f"current_page_{context}_{data_type}"
        sort_key = f"sort_by_{context}_{data_type}"
        sort_asc_key = f"sort_asc_{context}_{data_type}"
        data_cache_key = f"cached_data_{context}_{data_type}"
        
        if page_key not in st.session_state:
            st.session_state[page_key] = 1
        if sort_key not in st.session_state:
            st.session_state[sort_key] = None
        if sort_asc_key not in st.session_state:
            st.session_state[sort_asc_key] = True
        
        # Controles de tabla
        self._render_table_controls(data_type, context, filters)
        
        # Obtener datos paginados
        current_page = st.session_state[page_key]
        sort_by = st.session_state[sort_key]
        sort_ascending = st.session_state[sort_asc_key]
        
        # Crear clave de caché para los datos
        filters_hash = str(hash(str(sorted(filters.items())))) if filters else "no_filters"
        cache_key = f"{data_type}_{current_page}_{self.items_per_page}_{sort_by}_{sort_ascending}_{filters_hash}"
        
        # Verificar si los datos ya están en caché
        if cache_key in st.session_state:
            paginated_data = st.session_state[cache_key]['data']
            metadata = st.session_state[cache_key]['metadata']
        else:
            try:
                # Cargar datos con paginación real
                paginated_data, metadata = self.data_manager.get_paginated_data(
                    data_type=data_type,
                    page=current_page,
                    items_per_page=self.items_per_page,
                    filters=filters,
                    sort_by=sort_by,
                    sort_ascending=sort_ascending
                )
                
                # Guardar en caché
                st.session_state[cache_key] = {
                    'data': paginated_data,
                    'metadata': metadata
                }
                
            except Exception as e:
                st.error(f"Error cargando datos: {str(e)}")
                return
        
        if paginated_data.empty:
            self._render_no_data_message(filters)
            return
        
        # Mostrar estadísticas
        self._render_data_stats(metadata, data_type)
        
        # Preparar datos para visualización
        display_data = self._prepare_table_data(paginated_data)
        
        # Renderizar tabla
        self._render_data_table(display_data, data_type, context)
        
        # Renderizar controles de paginación
        new_page = self._render_pagination_controls(metadata, data_type, context)
        
        if new_page != current_page:
            st.session_state[page_key] = new_page
            # Limpiar caché cuando cambia la página
            for key in list(st.session_state.keys()):
                if key.startswith(f"cached_data_{context}_{data_type}"):
                    del st.session_state[key]
            st.rerun()
        
        # Funcionalidades adicionales
        self._render_export_section(display_data, data_type, context)
    
    def _render_table_controls(self, data_type: str, context: str, filters: Dict[str, Any]):
        """Renderizar controles de la tabla"""
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            # Mostrar filtros activos
            self._show_active_filters(filters)
        
        with col2:
            # Selector de ordenamiento
            sort_options = self._get_sort_options(data_type)
            if sort_options:
                sort_key = f"sort_by_{context}_{data_type}"
                sort_asc_key = f"sort_asc_{context}_{data_type}"
                
                selected_sort = st.selectbox(
                    "Ordenar por",
                    options=list(sort_options.keys()),
                    format_func=lambda x: sort_options[x],
                    key=f"sort_selector_{context}_{data_type}",
                    help="Selecciona columna para ordenar"
                )
                
                if selected_sort != st.session_state[sort_key]:
                    st.session_state[sort_key] = selected_sort
                    st.session_state[f"current_page_{context}_{data_type}"] = 1
                    st.rerun()
                
                # Dirección de ordenamiento
                sort_ascending = st.checkbox(
                    "Ascendente",
                    value=st.session_state[sort_asc_key],
                    key=f"sort_asc_checkbox_{context}_{data_type}",
                    help="Orden ascendente o descendente"
                )
                
                if sort_ascending != st.session_state[sort_asc_key]:
                    st.session_state[sort_asc_key] = sort_ascending
                    st.session_state[f"current_page_{context}_{data_type}"] = 1
                    st.rerun()
        
        with col3:
            # Selector de elementos por página
            items_per_page_options = [25, 50, 100, 250, 500]
            if self.items_per_page not in items_per_page_options:
                items_per_page_options.append(self.items_per_page)
                items_per_page_options.sort()
            
            new_items_per_page = st.selectbox(
                "Por página",
                options=items_per_page_options,
                index=items_per_page_options.index(self.items_per_page),
                key=f"items_per_page_{context}_{data_type}",
                help="Elementos por página"
            )
            
            if new_items_per_page != self.items_per_page:
                self.items_per_page = new_items_per_page
                st.session_state[f"current_page_{context}_{data_type}"] = 1
                st.rerun()
    
    def _get_sort_options(self, data_type: str) -> Dict[str, str]:
        """Obtener opciones de ordenamiento según el tipo de datos"""
        base_options = {
            'city': 'Ciudad',
            'year': 'Año',
            'month': 'Mes'
        }
        
        if data_type == 'summary':
            base_options.update({
                'avg_temp': 'Temperatura Promedio',
                'total_precip': 'Precipitación Total',
                'avg_humidity': 'Humedad Promedio'
            })
        elif data_type == 'alerts':
            base_options.update({
                'overall_alert': 'Nivel de Alerta',
                'alert_severity': 'Severidad',
                'date': 'Fecha'
            })
        elif data_type == 'trends':
            base_options.update({
                'trend_direction': 'Dirección de Tendencia',
                'change_percentage': 'Cambio Porcentual'
            })
        
        return base_options
    
    def _render_data_stats(self, metadata: Dict[str, Any], data_type: str):
        """Renderizar estadísticas de datos de forma compacta"""
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", f"{metadata['total_items']:,}")
        
        with col2:
            st.metric("Página Actual", f"{metadata['current_page']} / {metadata['total_pages']}")
        
        with col3:
            st.metric("Mostrando", f"{metadata['start_item']}-{metadata['end_item']}")
        
        with col4:
            st.metric("Por Página", metadata['items_per_page'])
    
    def _render_data_table(self, data: pd.DataFrame, data_type: str, context: str):
        """Renderizar tabla de datos"""
        if data.empty:
            return
        
        # Usar key única para evitar re-renderizados innecesarios
        table_key = f"data_table_{context}_{data_type}_{hash(str(data.values.tobytes()))}"
        
        st.dataframe(
            data,
            use_container_width=True,
            hide_index=True,
            key=table_key
        )
    
    def _render_no_data_message(self, filters: Dict[str, Any]):
        """Renderizar mensaje cuando no hay datos"""
        st.warning("No hay datos disponibles con los filtros seleccionados.")
        
        if filters:
            st.info("💡 Intenta ajustar los filtros para ver más datos.")
    
    def _render_pagination_controls(self, metadata: Dict[str, Any], data_type: str, context: str) -> int:
        """Renderizar controles de paginación"""
        current_page = metadata['current_page']
        total_pages = metadata['total_pages']
        
        if total_pages <= 1:
            return current_page
        
        # Controles de paginación
        col1, col2, col3, col4, col5 = st.columns([1, 2, 1, 1, 1])
        
        with col1:
            # Página anterior
            if metadata['has_prev']:
                if st.button("⬅️", key=f"prev_{context}_{data_type}", help="Página anterior"):
                    return max(1, current_page - 1)
            else:
                st.button("⬅️", disabled=True, key=f"prev_disabled_{context}_{data_type}")
        
        with col2:
            # Selector de página
            page_options = list(range(1, total_pages + 1))
            selected_page = st.selectbox(
                "Página",
                options=page_options,
                index=current_page - 1,
                key=f"page_selector_{context}_{data_type}",
                help=f"Página {current_page} de {total_pages}"
            )
            if selected_page != current_page:
                return selected_page
        
        with col3:
            # Página siguiente
            if metadata['has_next']:
                if st.button("➡️", key=f"next_{context}_{data_type}", help="Página siguiente"):
                    return min(total_pages, current_page + 1)
            else:
                st.button("➡️", disabled=True, key=f"next_disabled_{context}_{data_type}")
        
        with col4:
            # Información de paginación
            st.caption(f"📊 {metadata['start_item']}-{metadata['end_item']} de {metadata['total_items']}")
        
        with col5:
            # Botón de recarga
            if st.button("🔄", key=f"reload_{context}_{data_type}", help="Recargar datos"):
                st.rerun()
        
        return current_page
    
    def _render_export_section(self, data: pd.DataFrame, data_type: str, context: str):
        """Renderizar sección de exportación optimizada"""
        with st.expander("📤 Exportar Datos", expanded=False):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📥 CSV", key=f"export_csv_{context}_{data_type}"):
                    self._export_csv(data, data_type)
            
            with col2:
                if st.button("📊 Excel", key=f"export_excel_{context}_{data_type}"):
                    self._export_excel(data, data_type)
            
            with col3:
                if st.button("📋 Copiar", key=f"copy_{context}_{data_type}"):
                    self._copy_to_clipboard(data)
    
    def _export_csv(self, data: pd.DataFrame, data_type: str):
        """Exportar a CSV"""
        csv = data.to_csv(index=False, encoding='utf-8-sig')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        st.download_button(
            label="💾 Descargar CSV",
            data=csv,
            file_name=f"meteopanda_{data_type}_{timestamp}.csv",
            mime="text/csv"
        )
    
    def _export_excel(self, data: pd.DataFrame, data_type: str):
        """Exportar a Excel"""
        try:
            import openpyxl
            from openpyxl import Workbook
            from openpyxl.utils.dataframe import dataframe_to_rows
            
            wb = Workbook()
            ws = wb.active
            ws.title = f"Datos {data_type}"
            
            for r in dataframe_to_rows(data, index=False, header=True):
                ws.append(r)
            
            # Ajustar ancho de columnas
            for column in ws.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            st.download_button(
                label="💾 Descargar Excel",
                data=buffer.getvalue(),
                file_name=f"meteopanda_{data_type}_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        except ImportError:
            st.error("Para exportar a Excel, instala openpyxl: `pip install openpyxl`")
        except Exception as e:
            st.error(f"Error exportando a Excel: {str(e)}")
    
    def _copy_to_clipboard(self, data: pd.DataFrame):
        """Copiar al portapapeles"""
        try:
            clipboard_data = data.to_string(index=False)
            st.code(clipboard_data, language=None)
            st.success("Datos copiados al portapapeles")
        except Exception as e:
            st.error(f"Error copiando al portapapeles: {str(e)}")
    
    def set_data_manager(self, data_manager):
        """Asignar data manager para paginación real"""
        self.data_manager = data_manager
    
    def _show_active_filters(self, filters: Dict[str, Any]):
        """Mostrar filtros activos"""
        active_filters = []
        
        filter_labels = {
            'year': 'Año',
            'month': 'Mes',
            'region': 'Región',
            'cities': 'Ciudades',
            'season': 'Estación',
            'alert_level': 'Nivel de Alerta',
            'min_temp': 'Temp. Mínima',
            'max_temp': 'Temp. Máxima',
            'max_precip': 'Precip. Máxima',
            'source': 'Fuente'
        }
        
        for key, value in filters.items():
            if value is not None and value != []:
                label = filter_labels.get(key, key)
                if isinstance(value, list):
                    active_filters.append(f"{label}: {', '.join(map(str, value))}")
                else:
                    active_filters.append(f"{label}: {value}")
        
        if active_filters:
            with st.expander("🔍 Filtros Aplicados", expanded=False):
                for filter_info in active_filters:
                    st.write(f"• {filter_info}")
    
    def _prepare_table_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Preparar datos para la tabla"""
        if data.empty:
            return pd.DataFrame()
        
        # Mapeo de columnas para mejor visualización
        columns_mapping = {
            'city': 'Ciudad',
            'region': 'Región',
            'year': 'Año',
            'month': 'Mes',
            'date': 'Fecha',
            'temp_max_c': 'Temp. Máxima (°C)',
            'temp_min_c': 'Temp. Mínima (°C)',
            'avg_temp': 'Temp. Promedio (°C)',
            'precip_mm': 'Precipitación (mm)',
            'total_precip': 'Precipitación Total (mm)',
            'humidity_percent': 'Humedad (%)',
            'avg_humidity': 'Humedad Promedio (%)',
            'total_sunshine': 'Horas Sol',
            'source': 'Fuente',
            'station': 'Estación',
            'lat': 'Latitud',
            'lon': 'Longitud',
            'overall_alert': 'Alerta General',
            'alert_severity': 'Severidad',
            'temperature_alert': 'Alerta Temperatura',
            'precipitation_alert': 'Alerta Precipitación',
            'humidity_alert': 'Alerta Humedad',
            'season': 'Estación',
            'climate_classification': 'Clasificación Climática',
            'heat_rank_in_region': 'Ranking Calor',
            'precip_rank_in_region': 'Ranking Precipitación'
        }
        
        # Filtrar columnas que existen
        existing_columns = {k: v for k, v in columns_mapping.items() if k in data.columns}
        
        table_data = data[list(existing_columns.keys())].copy()
        table_data.columns = list(existing_columns.values())
        
        # Formatear valores numéricos
        self._format_numeric_columns(table_data)
        
        # Ordenar por columnas relevantes
        if 'Año' in table_data.columns and 'Mes' in table_data.columns:
            table_data = table_data.sort_values(['Año', 'Mes', 'Ciudad'])
        elif 'Ciudad' in table_data.columns:
            table_data = table_data.sort_values('Ciudad')
        
        return table_data
    
    def _format_numeric_columns(self, table_data: pd.DataFrame):
        """Formatear columnas numéricas"""
        numeric_columns = [col for col in table_data.columns if any(keyword in col for keyword in ['Temp.', 'Precipitación', 'Humedad', 'Latitud', 'Longitud'])]
        
        for col in numeric_columns:
            if col in table_data.columns:
                table_data[col] = pd.to_numeric(table_data[col], errors='ignore')
                if table_data[col].dtype in ['float64', 'int64']:
                    table_data[col] = table_data[col].round(2)
    
    
    

    
    
    
    
