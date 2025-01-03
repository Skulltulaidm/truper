import streamlit as st
import pandas as pd
from typing import Dict, Set
import logging
from datetime import datetime
from io import BytesIO
import requests
import plotly.express as px
import plotly.graph_objects as go

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# URL del archivo BD Brand en GitHub
BD_BRAND_URL = "https://github.com/Skulltulaidm/truper/blob/main/BD%20Brand.xlsx"

class ProcesadorPedidos:
    MARCAS_PERMITIDAS = {"Marca privada Exp.", "Producto de Catálogo Americano"}
    CENTROS_REQUERIDOS = {"EXPO", "LARE"}
    COLUMNAS_SALIDA = {
        'Doc.ventas': 'Pedido',
        'Descripción': 'Marca',
        'Nombre 1': 'Cliente',
        'Material': 'Material',
        'Texto breve de material': 'Descripción',
        ' Pendiente': 'Ctd. Sol.',
        'Tránsito': 'Tránsito',
        'Planta': 'CE',
        'Embarque': 'Fecha Embarque',
        'Liberación': 'Fecha Liberación',
        'Inventario': 'Inventario',
        'Faltante': 'Faltante',
        'Faltante Tránsito': 'Faltante Tránsito',
        'Estatus': 'Estatus',
        'Horario Entrega': 'Horario Entrega'
    }

    def __init__(self, archivo_pedidos, archivo_inventarios, archivo_bd_brand=None):
        """
        Inicializa el procesador con los archivos cargados.
        """
        self.archivo_pedidos = archivo_pedidos
        self.archivo_inventarios = archivo_inventarios
        self.archivo_bd_brand = archivo_bd_brand

    def cargar_bd_brand_desde_github(self):
        """
        Carga el archivo BD Brand desde GitHub
        """
        try:
            response = requests.get(BD_BRAND_URL)
            return pd.read_excel(BytesIO(response.content))
        except Exception as e:
            st.error(f"Error al cargar BD Brand desde GitHub: {str(e)}")
            return None

    def preprocesar_pedidos(self) -> pd.DataFrame:
        """
        Preprocesa el archivo de pedidos.
        """
        pedidos = pd.read_excel(
            self.archivo_pedidos,
            header=9
        )
        
        pedidos = pedidos.drop(index=0)
        pedidos = pedidos.iloc[:, 1:]

        for columna in ['Embarque', 'Liberación']:
            pedidos[columna] = pd.to_datetime(
                pedidos[columna],
                format='%d.%m.%Y',
                errors='coerce'
            )

        return pedidos

    def generar_reporte_marcas(self, pedidos: pd.DataFrame) -> tuple:
        """
        Genera reporte detallado por marca
        """
        # Resumen por marca
        resumen_marcas = pd.DataFrame()
        
        # Total de pedidos por marca
        total_por_marca = pedidos.groupby('Marca')['Pedido'].nunique()
        
        # Pedidos completos por marca
        completos_por_marca = pedidos[pedidos['Estatus'] == 'Completo'].groupby('Marca')['Pedido'].nunique()
        
        # Pedidos incompletos por marca
        incompletos_por_marca = pedidos[pedidos['Estatus'] == 'Incompleto'].groupby('Marca')['Pedido'].nunique()
        
        # Total de líneas por marca
        lineas_por_marca = pedidos.groupby('Marca').size()
        
        # Monto total pendiente por marca
        monto_pendiente = pedidos.groupby('Marca')['Faltante'].sum()
        
        # Consolidar todo en un DataFrame
        resumen_marcas['Total Pedidos'] = total_por_marca
        resumen_marcas['Pedidos Completos'] = completos_por_marca
        resumen_marcas['Pedidos Incompletos'] = incompletos_por_marca
        resumen_marcas['Total Líneas'] = lineas_por_marca
        resumen_marcas['Monto Pendiente'] = monto_pendiente
        resumen_marcas['% Cumplimiento'] = (resumen_marcas['Pedidos Completos'] / resumen_marcas['Total Pedidos'] * 100).round(2)
        
        # Generar gráficos
        fig_pedidos = px.bar(
            resumen_marcas,
            x=resumen_marcas.index,
            y=['Pedidos Completos', 'Pedidos Incompletos'],
            title='Distribución de Pedidos por Marca',
            barmode='group'
        )
        
        fig_cumplimiento = px.line(
            resumen_marcas,
            x=resumen_marcas.index,
            y='% Cumplimiento',
            title='Porcentaje de Cumplimiento por Marca',
            markers=True
        )
        
        # Gráfico de tendencia temporal
        tendencia_temporal = pedidos.groupby(['Marca', 'Fecha Embarque'])['Pedido'].count().reset_index()
        fig_tendencia = px.line(
            tendencia_temporal,
            x='Fecha Embarque',
            y='Pedido',
            color='Marca',
            title='Tendencia de Pedidos por Marca'
        )
        
        return resumen_marcas, fig_pedidos, fig_cumplimiento, fig_tendencia

    def procesar(self) -> tuple:
        """
        Procesa los pedidos e inventarios y retorna los DataFrames resultantes
        """
        try:
            # 1. Cargar datos
            pedidos = self.preprocesar_pedidos()
            inventarios = pd.read_excel(self.archivo_inventarios)
            
            # Cargar BD Brand desde archivo local o GitHub
            if self.archivo_bd_brand is not None:
                bd_brand = pd.read_excel(self.archivo_bd_brand)
            else:
                bd_brand = self.cargar_bd_brand_desde_github()
                if bd_brand is None:
                    raise Exception("No se pudo cargar el archivo BD Brand")

            # [Resto del código de procesamiento igual que antes...]
            # ... [Código anterior sin cambios hasta el final del método]

            # Generar reporte de marcas
            reporte_marcas, fig_pedidos, fig_cumplimiento, fig_tendencia = self.generar_reporte_marcas(pedidos)

            return pedidos, completos, incompletos, reporte_marcas, (fig_pedidos, fig_cumplimiento, fig_tendencia)

        except Exception as e:
            st.error(f"Error en el procesamiento: {str(e)}")
            raise

def to_excel(df: pd.DataFrame) -> bytes:
    """
    Convierte un DataFrame a bytes de Excel
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def main():
    st.set_page_config(page_title="Procesador de Pedidos", layout="wide")
    
    # Sidebar con información
    with st.sidebar:
        st.title("Información")
        st.info("""
        Esta aplicación procesa:
        - Pedidos
        - Inventarios
        - Base de datos de marcas
        
        La BD Brand se puede cargar desde GitHub o subir manualmente.
        """)
        
        st.markdown("### Características")
        st.markdown("""
        - Análisis detallado por marca
        - Visualizaciones interactivas
        - Reportes descargables
        - Métricas en tiempo real
        """)

    st.title("Procesador de Pedidos e Inventarios")
    
    # File uploaders
    col1, col2, col3 = st.columns(3)
    
    with col1:
        archivo_pedidos = st.file_uploader("Archivo de Pedidos", type=['xlsx'])
    
    with col2:
        archivo_inventarios = st.file_uploader("Archivo de Inventarios", type=['xlsx'])
    
    with col3:
        usar_github = st.checkbox("Usar BD Brand desde GitHub", value=True)
        if not usar_github:
            archivo_bd_brand = st.file_uploader("Archivo BD Brand", type=['xlsx'])
        else:
            archivo_bd_brand = None
            st.info("Usando BD Brand desde GitHub")

    if archivo_pedidos and archivo_inventarios and (usar_github or archivo_bd_brand):
        try:
            with st.spinner('Procesando archivos...'):
                procesador = ProcesadorPedidos(
                    archivo_pedidos,
                    archivo_inventarios,
                    archivo_bd_brand
                )
                pedidos, completos, incompletos, reporte_marcas, (fig_pedidos, fig_cumplimiento, fig_tendencia) = procesador.procesar()

            # Mostrar resumen general
            st.header("Resumen General", divider="rainbow")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Pedidos", len(pedidos['Pedido'].unique()))
            with col2:
                st.metric("Pedidos Completos", len(completos['Pedido'].unique()))
            with col3:
                st.metric("Pedidos Incompletos", len(incompletos['Pedido'].unique()))
            with col4:
                cumplimiento = (len(completos['Pedido'].unique()) / len(pedidos['Pedido'].unique()) * 100).round(2)
                st.metric("% Cumplimiento Total", f"{cumplimiento}%")

            # Análisis por Marca
            st.header("Análisis por Marca", divider="rainbow")
            
            # Mostrar métricas por marca
            st.subheader("Resumen por Marca")
            st.dataframe(reporte_marcas, use_container_width=True)
            
            # Descargar reporte de marcas
            st.download_button(
                "📊 Descargar Reporte de Marcas",
                data=to_excel(reporte_marcas),
                file_name="reporte_marcas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Visualizaciones
            st.subheader("Visualizaciones")
            tab1, tab2, tab3 = st.tabs(["Distribución de Pedidos", "Cumplimiento", "Tendencia Temporal"])
            
            with tab1:
                st.plotly_chart(fig_pedidos, use_container_width=True)
            with tab2:
                st.plotly_chart(fig_cumplimiento, use_container_width=True)
            with tab3:
                st.plotly_chart(fig_tendencia, use_container_width=True)

            # Datos Detallados
            st.header("Datos Detallados", divider="rainbow")
            tabs = st.tabs(["Todos los Pedidos", "Pedidos Completos", "Pedidos Incompletos"])
            
            with tabs[0]:
                st.dataframe(pedidos, use_container_width=True)
                st.download_button(
                    "📥 Descargar Todos los Pedidos",
                    data=to_excel(pedidos),
                    file_name="todos_los_pedidos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with tabs[1]:
                st.dataframe(completos, use_container_width=True)
                st.download_button(
                    "📥 Descargar Pedidos Completos",
                    data=to_excel(completos),
                    file_name="pedidos_completos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with tabs[2]:
                st.dataframe(incompletos, use_container_width=True)
                st.download_button(
                    "📥 Descargar Pedidos Incompletos",
                    data=to_excel(incompletos),
                    file_name="pedidos_incompletos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            # Reporte por Marca (Excel con múltiples hojas)
            st.header("Reporte Detallado por Marca", divider="rainbow")
            marcas = sorted(incompletos['Marca'].unique())
            
            # Create Excel file with multiple sheets
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for marca in marcas:
                    df_marca = incompletos[incompletos['Marca'] == marca]
                    nombre_hoja = marca[:31]  # Excel limit for sheet names
                    df_marca.to_excel(writer, sheet_name=nombre_hoja, index=False)
            
            st.download_button(
                "📊 Descargar Reporte Detallado por Marca",
                data=output.getvalue(),
                file_name="reporte_detallado_por_marca.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"Error durante el procesamiento: {str(e)}")
    else:
        st.info("Por favor, suba todos los archivos requeridos para comenzar el procesamiento.")

if __name__ == "__main__":
    main()
