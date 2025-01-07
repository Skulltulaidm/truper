import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from io import BytesIO
from application.use_cases import ProcesarPedidosUseCase
from infrastructure.cache import InMemoryCacheManager
from domain.repositories import PedidoRepository

class StreamlitView:
    """
    Clase principal para la interfaz de usuario de Streamlit
    """
    def __init__(self):
        self.cache_manager = InMemoryCacheManager()
        self.use_case = ProcesarPedidosUseCase(
            pedido_repository=None,
            cache_manager=self.cache_manager
        )
        # Inicializar estado de la sesión
        if 'filters' not in st.session_state:
            st.session_state.filters = {
                'marcas': [],
                'pedidos': [],
                'materiales': [],
                'fecha_inicio': None,
                'fecha_fin': None
            }

    def render(self):
        """Método principal para renderizar la aplicación"""
        self._setup_page()
        
        # File uploaders y procesamiento inicial
        pedidos, completos, incompletos, reporte_marcas = self._handle_file_upload()
        
        if pedidos is not None:
            # Renderizar componentes principales
            filtered_data = self._render_filters(pedidos)
            if filtered_data:
                self._render_summary(*filtered_data)
                self._render_analytics(filtered_data[0], reporte_marcas)
                self._render_detailed_views(*filtered_data)
                self._render_export_options(*filtered_data)

    def _setup_page(self):
        """Configuración inicial de la página"""
        st.set_page_config(
            page_title="Sistema de Procesamiento de Pedidos",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        st.title("Sistema de Procesamiento de Pedidos e Inventarios")
        st.sidebar.header("Configuración")

    def _handle_file_upload(self) -> Tuple[Optional[pd.DataFrame], ...]:
        """Maneja la carga de archivos y el procesamiento inicial"""
        col1, col2 = st.columns(2)
        
        with col1:
            archivo_pedidos = st.file_uploader(
                "Archivo de Pedidos",
                type=['xlsx'],
                help="Seleccione el archivo Excel de pedidos"
            )
        
        with col2:
            archivo_inventarios = st.file_uploader(
                "Archivo de Inventarios",
                type=['xlsx'],
                help="Seleccione el archivo Excel de inventarios"
            )

        if all([archivo_pedidos, archivo_inventarios]):
            try:
                with st.spinner('Procesando archivos...'):
                    return self.use_case.execute(archivo_pedidos, archivo_inventarios)
            except Exception as e:
                st.error(f"Error durante el procesamiento: {str(e)}")
                return None, None, None, None
        return None, None, None, None

    def _render_filters(self, pedidos: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Renderiza y aplica los filtros de datos"""
        st.header("Filtros de Análisis")
        
        # Crear columnas para filtros
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            marcas_seleccionadas = st.multiselect(
                "Filtrar por Marca",
                options=sorted(pedidos['Marca'].unique()),
                default=st.session_state.filters['marcas']
            )
            st.session_state.filters['marcas'] = marcas_seleccionadas

        with col2:
            pedidos_seleccionados = st.multiselect(
                "Filtrar por Pedido",
                options=sorted(pedidos['Pedido'].unique()),
                default=st.session_state.filters['pedidos']
            )
            st.session_state.filters['pedidos'] = pedidos_seleccionados

        with col3:
            materiales_seleccionados = st.multiselect(
                "Filtrar por Material",
                options=sorted(pedidos['Material'].unique()),
                default=st.session_state.filters['materiales']
            )
            st.session_state.filters['materiales'] = materiales_seleccionados

        with col4:
            fecha_min = pedidos['Fecha Embarque'].min().date()
            fecha_max = pedidos['Fecha Embarque'].max().date()
            fechas = st.date_input(
                "Rango de Fechas",
                value=(
                    st.session_state.filters['fecha_inicio'] or fecha_min,
                    st.session_state.filters['fecha_fin'] or fecha_max
                ),
                min_value=fecha_min,
                max_value=fecha_max
            )
            if len(fechas) == 2:
                st.session_state.filters['fecha_inicio'] = fechas[0]
                st.session_state.filters['fecha_fin'] = fechas[1]

        # Aplicar filtros
        return self._apply_filters(pedidos, marcas_seleccionadas, 
                                 pedidos_seleccionados, materiales_seleccionados, 
                                 fechas if len(fechas) == 2 else None)

    def _apply_filters(self, 
                      pedidos: pd.DataFrame,
                      marcas: List[str],
                      pedidos_ids: List[str],
                      materiales: List[str],
                      fechas: Optional[Tuple[datetime, datetime]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Aplica los filtros seleccionados a los datos"""
        mask = pd.Series(True, index=pedidos.index)
        
        if marcas:
            mask &= pedidos['Marca'].isin(marcas)
        if pedidos_ids:
            mask &= pedidos['Pedido'].isin(pedidos_ids)
        if materiales:
            mask &= pedidos['Material'].isin(materiales)
        if fechas:
            mask &= (
                (pedidos['Fecha Embarque'].dt.date >= fechas[0]) &
                (pedidos['Fecha Embarque'].dt.date <= fechas[1])
            )

        filtered_pedidos = pedidos[mask]
        completos = filtered_pedidos[filtered_pedidos['Estatus'] == 'Completo']
        incompletos = filtered_pedidos[filtered_pedidos['Estatus'] == 'Incompleto']
        
        return filtered_pedidos, completos, incompletos

    def _render_summary(self, pedidos: pd.DataFrame, completos: pd.DataFrame, incompletos: pd.DataFrame):
        """Renderiza el resumen de datos"""
        st.header("Resumen General")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Pedidos",
                len(pedidos['Pedido'].unique()),
                help="Número total de pedidos únicos"
            )
        
        with col2:
            st.metric(
                "Pedidos Completos",
                len(completos['Pedido'].unique()),
                help="Pedidos que han sido completados"
            )
        
        with col3:
            st.metric(
                "Pedidos Incompletos",
                len(incompletos['Pedido'].unique()),
                help="Pedidos pendientes por completar"
            )
        
        with col4:
            porcentaje_completos = (
                len(completos['Pedido'].unique()) / 
                len(pedidos['Pedido'].unique()) * 100 
                if len(pedidos['Pedido'].unique()) > 0 else 0
            )
            st.metric(
                "% Completado",
                f"{porcentaje_completos:.1f}%",
                help="Porcentaje de pedidos completados"
            )

    def _render_analytics(self, pedidos: pd.DataFrame, reporte_marcas: pd.DataFrame):
        """Renderiza análisis y visualizaciones"""
        st.header("Análisis y Visualizaciones")
        
        tab1, tab2 = st.tabs(["Análisis por Marca", "Tendencias Temporales"])
        
        with tab1:
            self._render_marca_analysis(reporte_marcas)
        
        with tab2:
            self._render_temporal_analysis(pedidos)

    def _render_marca_analysis(self, reporte_marcas: pd.DataFrame):
        """Renderiza análisis específico por marca"""
        # Gráfica de barras para pedidos por marca
        fig_pedidos = px.bar(
            reporte_marcas,
            x=reporte_marcas.index,
            y=['Pedidos Completos', 'Pedidos Incompletos'],
            title='Distribución de Pedidos por Marca',
            labels={'value': 'Número de Pedidos', 'variable': 'Tipo'},
            barmode='stack'
        )
        st.plotly_chart(fig_pedidos, use_container_width=True)
        
        # Tabla de métricas por marca
        st.dataframe(
            reporte_marcas,
            use_container_width=True,
            height=400
        )

    def _render_temporal_analysis(self, pedidos: pd.DataFrame):
        """Renderiza análisis temporal de pedidos"""
        # Análisis de tendencias por fecha
        pedidos_por_fecha = pedidos.groupby('Fecha Embarque').agg({
            'Pedido': 'count',
            'Faltante': 'sum',
            'Inventario': 'sum'
        }).reset_index()
        
        fig_tendencia = px.line(
            pedidos_por_fecha,
            x='Fecha Embarque',
            y=['Pedido', 'Faltante', 'Inventario'],
            title='Tendencias Temporales'
        )
        st.plotly_chart(fig_tendencia, use_container_width=True)

    def _render_detailed_views(self, pedidos: pd.DataFrame, completos: pd.DataFrame, incompletos: pd.DataFrame):
        """Renderiza vistas detalladas de los datos"""
        st.header("Vistas Detalladas")
        
        tabs = st.tabs(["Todos los Pedidos", "Pedidos Completos", "Pedidos Incompletos"])
        
        with tabs[0]:
            st.dataframe(
                pedidos,
                use_container_width=True,
                height=400
            )
        
        with tabs[1]:
            st.dataframe(
                completos,
                use_container_width=True,
                height=400
            )
        
        with tabs[2]:
            st.dataframe(
                incompletos,
                use_container_width=True,
                height=400
            )

    def _render_export_options(self, pedidos: pd.DataFrame, completos: pd.DataFrame, incompletos: pd.DataFrame):
        """Renderiza opciones de exportación"""
        st.header("Exportar Datos")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Exportar Reporte Completo"):
                excel_file = self._generate_excel_report(pedidos, completos, incompletos)
                st.download_button(
                    label="Descargar Reporte Excel",
                    data=excel_file,
                    file_name=f"reporte_pedidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with col2:
            if st.button("Exportar Resumen"):
                csv_file = self._generate_csv_summary(pedidos)
                st.download_button(
                    label="Descargar Resumen CSV",
                    data=csv_file,
                    file_name=f"resumen_pedidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

    def _generate_excel_report(self, pedidos: pd.DataFrame, completos: pd.DataFrame, incompletos: pd.DataFrame) -> bytes:
        """Genera reporte Excel completo"""
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            pedidos.to_excel(writer, sheet_name='Todos los Pedidos', index=False)
            completos.to_excel(writer, sheet_name='Pedidos Completos', index=False)
            incompletos.to_excel(writer, sheet_name='Pedidos Incompletos', index=False)
        return output.getvalue()

    def _generate_csv_summary(self, pedidos: pd.DataFrame) -> str:
        """Genera resumen en formato CSV"""
        resumen = pedidos.groupby('Marca').agg({
            'Pedido': 'count',
            'Faltante': 'sum',
            'Inventario': 'sum'
        }).reset_index()
        return resumen.to_csv(index=False)

# Inicialización de la aplicación
def main():
    view = StreamlitView()
    view.render()

if __name__ == "__main__":
    main()
