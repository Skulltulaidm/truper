import streamlit as st
from application.use_cases import ProcesarPedidosUseCase
from infrastructure.repositories import PandasPedidoRepository
from infrastructure.cache import InMemoryCacheManager

class StreamlitView:
    def __init__(self):
        self.cache_manager = InMemoryCacheManager()
        self.use_case = ProcesarPedidosUseCase(
            pedido_repository=None,  # Se inicializa después
            cache_manager=self.cache_manager
        )

    def render(self):
        st.set_page_config(page_title="Procesador de Pedidos", layout="wide")
        st.title("Procesador de Pedidos e Inventarios")

        # File uploaders
        archivo_pedidos = st.file_uploader("Archivo de Pedidos", type=['xlsx'])
        archivo_inventarios = st.file_uploader("Archivo de Inventarios", type=['xlsx'])

        if all([archivo_pedidos, archivo_inventarios]):
            try:
                with st.spinner('Procesando archivos...'):
                    pedidos, completos, incompletos, reporte_marcas = (
                        self.use_case.execute(archivo_pedidos, archivo_inventarios)
                    )
                
                self._render_filters(pedidos)
                self._render_summary(pedidos, completos, incompletos)
                self._render_visualizations(reporte_marcas)
                self._render_detailed_views(pedidos, completos, incompletos)
                self._render_export_options(pedidos, completos, incompletos)

            except Exception as e:
                st.error(f"Error durante el procesamiento: {str(e)}")
        else:
            st.info("Por favor, suba todos los archivos requeridos.")

    def _render_filters(self, pedidos: pd.DataFrame):
        st.header("Filtros")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            selected_marcas = st.multiselect(
                "Marcas",
                options=sorted(pedidos['Marca'].unique())
            )
        
        # ... resto de los filtros

    def _render_summary(self, pedidos, completos, incompletos):
        st.header("Resumen")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Pedidos", len(pedidos))
        # ... resto del resumen
