"""
Componente de tabla mejorado con paginación, exportación y funcionalidades avanzadas
"""
import streamlit as st
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
import io
import base64

class AdvancedTableComponent:
    """Componente de tabla avanzado con paginación y exportación"""
    
    def __init__(self, items_per_page: int = 10):
        self.items_per_page = items_per_page
        self.current_page = 1
    
    def render_table(self, data: pd.DataFrame, filters: Dict[str, Any], title: str = "Tabla de Datos"):
        """Renderizar tabla con paginación y funcionalidades avanzadas"""
        st.header(title)
        
        if data.empty:
            st.warning("No hay datos para mostrar con los filtros seleccionados.")
            return
        
        # Mostrar filtros aplicados
        self._show_active_filters(filters)
        
        # Preparar datos para la tabla
        table_data = self._prepare_table_data(data)
        
        # Mostrar estadísticas de datos
        self._show_data_stats(table_data)
        
        # Configuración de paginación
        pagination_config = self._setup_pagination(table_data)
        
        # Mostrar tabla paginada
        self._render_paginated_table(table_data, pagination_config)
        
        # Funcionalidades de exportación
        self._render_export_options(table_data)
        
        # Funcionalidades adicionales
        self._render_additional_features(table_data)
    
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
    
    def _show_data_stats(self, table_data: pd.DataFrame):
        """Mostrar estadísticas de los datos"""
        if table_data.empty:
            return
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", len(table_data))
        
        with col2:
            if 'Ciudad' in table_data.columns:
                st.metric("Ciudades Únicas", table_data['Ciudad'].nunique())
        
        with col3:
            if 'Año' in table_data.columns:
                st.metric("Años", table_data['Año'].nunique())
        
        with col4:
            if 'Región' in table_data.columns:
                st.metric("Regiones", table_data['Región'].nunique())
    
    def _setup_pagination(self, table_data: pd.DataFrame) -> Dict[str, Any]:
        """Configurar paginación"""
        total_items = len(table_data)
        total_pages = (total_items + self.items_per_page - 1) // self.items_per_page
        
        # Selector de página
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            page = st.selectbox(
                "📄 Página",
                options=range(1, total_pages + 1),
                index=0,
                help=f"Página actual de {total_pages} páginas disponibles",
                key="table_page_selector"
            )
        
        # Selector de elementos por página
        with col3:
            items_per_page_options = [5, 10, 25, 50, 100]
            self.items_per_page = st.selectbox(
                "📊 Por página",
                options=items_per_page_options,
                index=1,
                help="Número de elementos por página",
                key="table_items_per_page"
            )
        
        return {
            'current_page': page,
            'total_pages': total_pages,
            'total_items': total_items,
            'items_per_page': self.items_per_page
        }
    
    def _render_paginated_table(self, table_data: pd.DataFrame, pagination_config: Dict[str, Any]):
        """Renderizar tabla paginada"""
        current_page = pagination_config['current_page']
        items_per_page = pagination_config['items_per_page']
        total_items = pagination_config['total_items']
        
        # Calcular rango de items a mostrar
        start_idx = (current_page - 1) * items_per_page
        end_idx = min(start_idx + items_per_page, total_items)
        
        # Mostrar tabla
        if not table_data.empty:
            st.dataframe(
                table_data.iloc[start_idx:end_idx],
                use_container_width=True,
                hide_index=True
            )
            
            # Información de paginación
            st.caption(f"📊 Mostrando {start_idx + 1}-{end_idx} de {total_items} registros | Página {current_page} de {pagination_config['total_pages']}")

    def _render_export_options(self, table_data: pd.DataFrame):
        """Renderizar opciones de exportación"""
        st.subheader("📤 Exportar Datos")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📥 Exportar CSV", help="Descargar datos en formato CSV"):
                self._export_csv(table_data)
        
        with col2:
            if st.button("📊 Exportar Excel", help="Descargar datos en formato Excel"):
                self._export_excel(table_data)
        
        with col3:
            if st.button("📋 Copiar al Portapapeles", help="Copiar datos al portapapeles"):
                self._copy_to_clipboard(table_data)
    
    def _export_csv(self, table_data: pd.DataFrame):
        """Exportar datos a CSV"""
        csv = table_data.to_csv(index=False, encoding='utf-8-sig')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        st.download_button(
            label="💾 Descargar CSV",
            data=csv,
            file_name=f"meteopanda_data_{timestamp}.csv",
            mime="text/csv",
            help="Haz clic para descargar el archivo CSV"
        )
    
    def _export_excel(self, table_data: pd.DataFrame):
        """Exportar datos a Excel"""
        try:
            # Intentar importar openpyxl
            import openpyxl
            from openpyxl import Workbook
            from openpyxl.utils.dataframe import dataframe_to_rows
            
            # Crear workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "Datos MeteoPanda"
            
            # Añadir datos
            for r in dataframe_to_rows(table_data, index=False, header=True):
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
            
            # Guardar en buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            st.download_button(
                label="💾 Descargar Excel",
                data=buffer.getvalue(),
                file_name=f"meteopanda_data_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Haz clic para descargar el archivo Excel"
            )
            
        except ImportError:
            st.error("Para exportar a Excel, instala openpyxl: `pip install openpyxl`")
        except Exception as e:
            st.error(f"Error exportando a Excel: {str(e)}")
    
    def _copy_to_clipboard(self, table_data: pd.DataFrame):
        """Copiar datos al portapapeles"""
        try:
            # Convertir DataFrame a texto tabulado
            clipboard_data = table_data.to_string(index=False)
            
            # Usar streamlit para copiar al portapapeles
            st.code(clipboard_data, language=None)
            st.success("Datos copiados al portapapeles")
            
        except Exception as e:
            st.error(f"Error copiando al portapapeles: {str(e)}")
    
    def _render_additional_features(self, table_data: pd.DataFrame):
        """Renderizar funcionalidades adicionales"""
        st.subheader("🔧 Funcionalidades Adicionales")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Búsqueda en la tabla
            search_term = st.text_input(
                "🔍 Buscar en la tabla",
                placeholder="Escribe para buscar...",
                help="Buscar texto en cualquier columna de la tabla"
            )
            
            if search_term:
                filtered_data = self._search_in_table(table_data, search_term)
                if not filtered_data.empty:
                    st.write(f"Resultados de búsqueda para '{search_term}':")
                    st.dataframe(filtered_data, use_container_width=True)
                else:
                    st.info(f"No se encontraron resultados para '{search_term}'")
        
        with col2:
            # Estadísticas rápidas
            if st.button("📊 Mostrar Estadísticas", help="Mostrar estadísticas de los datos actuales"):
                self._show_quick_stats(table_data)
    
    def _search_in_table(self, table_data: pd.DataFrame, search_term: str) -> pd.DataFrame:
        """Buscar texto en la tabla"""
        if not search_term:
            return table_data
        
        # Buscar en todas las columnas
        mask = pd.DataFrame([table_data[col].astype(str).str.contains(search_term, case=False, na=False) 
                           for col in table_data.columns]).any()
        
        return table_data[mask]
    
    def _show_quick_stats(self, table_data: pd.DataFrame):
        """Mostrar estadísticas rápidas"""
        if table_data.empty:
            return
        
        st.write("📈 Estadísticas Rápidas:")
        
        # Estadísticas por columna
        for col in table_data.columns:
            if table_data[col].dtype in ['float64', 'int64']:
                st.write(f"**{col}**:")
                st.write(f"  - Mín: {table_data[col].min():.2f}")
                st.write(f"  - Máx: {table_data[col].max():.2f}")
                st.write(f"  - Promedio: {table_data[col].mean():.2f}")
                st.write(f"  - Mediana: {table_data[col].median():.2f}")
            elif table_data[col].dtype == 'object':
                st.write(f"**{col}**: {table_data[col].nunique()} valores únicos")
