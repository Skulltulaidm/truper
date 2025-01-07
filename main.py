import streamlit as st
import pandas as pd
from typing import Dict, Set
import logging
from datetime import datetime
from io import BytesIO
import plotly.express as px
import os

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


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
        'Liberación': 'Liberación en Sistema',
        'Inventario': 'Inventario',
        'Faltante': 'Faltante',
        'Faltante con Tránsito': 'Faltante con Tránsito',
        'Estatus': 'Estatus',
        'Horario Entrega': 'Horario Entrega'
    }

    def __init__(self, archivo_pedidos, archivo_inventarios):
        """
        Inicializa el procesador con los archivos cargados.
        """
        self.archivo_pedidos = archivo_pedidos
        self.archivo_inventarios = archivo_inventarios
        # Cargar BD Brand directamente desde el repositorio
        self.archivo_bd_brand = pd.read_excel("BD Brand.xlsx")

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

    def generar_reporte_marcas(self, pedidos: pd.DataFrame) -> pd.DataFrame:
        """
        Genera un reporte detallado por marca
        """
        if pedidos.empty:
            return pd.DataFrame()

        reporte_marcas = pd.DataFrame()

        # Agrupar por marca
        reporte_marcas['Total Pedidos'] = pedidos.groupby('Marca')['Pedido'].nunique()

        # Pedidos completos por marca
        pedidos_completos = pedidos[pedidos['Estatus'] == 'Completo']
        reporte_marcas['Pedidos Completos'] = pedidos_completos.groupby('Marca')['Pedido'].nunique()

        # Pedidos incompletos por marca
        pedidos_incompletos = pedidos[pedidos['Estatus'] == 'Incompleto']
        reporte_marcas['Pedidos Incompletos'] = pedidos_incompletos.groupby('Marca')['Pedido'].nunique()

        # Calcular porcentajes
        reporte_marcas['% Completos'] = (
                    reporte_marcas['Pedidos Completos'] / reporte_marcas['Total Pedidos'] * 100).round(2)
        reporte_marcas['% Incompletos'] = (
                    reporte_marcas['Pedidos Incompletos'] / reporte_marcas['Total Pedidos'] * 100).round(2)

        # Valor total de pedidos
        reporte_marcas['Total Solicitado (pz)'] = pedidos.groupby('Marca')['Ctd. Sol.'].sum()

        # Valor faltante
        reporte_marcas['Faltante (pz)'] = pedidos.groupby('Marca')['Faltante'].sum()
        reporte_marcas['% Faltante'] = (
                    reporte_marcas['Faltante (pz)'] / reporte_marcas['Total Solicitado (pz)'] * 100).round(2)

        return reporte_marcas

    def procesar(self) -> tuple:
        """
        Procesa los pedidos e inventarios y retorna los DataFrames resultantes
        """
        try:
            # 1. Cargar datos
            pedidos = self.preprocesar_pedidos()
            inventarios = pd.read_excel(self.archivo_inventarios)
            bd_brand = self.archivo_bd_brand

            # 2. Filtrar pedidos
            pedidos = pedidos[
                (pedidos['Muestra'] != 'X') &
                (pedidos['Descripción'].isin(self.MARCAS_PERMITIDAS)) &
                (~pedidos['Nombre 1'].str.contains('James Palin', na=False))
                ]

            # 3. Preparar inventarios
            inventarios = inventarios[inventarios['Carac. Planif.'] != 'ND']
            materiales_por_centro = inventarios.groupby('Material')['Centro'].apply(set)
            materiales_validos = materiales_por_centro[
                materiales_por_centro.apply(lambda x: x >= self.CENTROS_REQUERIDOS)
            ].index
            inventarios = inventarios[
                ~((inventarios['Material'].isin(materiales_validos)) &
                  (inventarios['Centro'] == 'EXPO'))
            ]

            # 4. Actualizar marcas usando BD Brand
            mapeo_marcas = dict(zip(bd_brand['Solic.'], bd_brand['Marca']))
            pedidos['Descripción'] = pedidos['Solic.'].map(mapeo_marcas).fillna(pedidos['Descripción'])

            # 5. Ordenar pedidos
            pedidos['Marca Prioridad'] = pedidos['Descripción'].apply(
                lambda x: 0 if x == 'Producto de Catálogo Americano' else 1
            )
            pedidos = pedidos.sort_values(['Embarque', 'Doc.ventas', 'Marca Prioridad'])
            pedidos = pedidos.drop(columns=['Marca Prioridad'])

            # 6. Procesar inventarios y tránsitos
            mapa_inventario = inventarios.set_index('Material')['Disponible'].to_dict()
            mapa_transito = inventarios.set_index('Material')['Traslado'].to_dict()

            pedidos['Inventario'] = 0.0
            pedidos['Tránsito'] = 0.0
            pedidos['Faltante'] = 0.0
            pedidos['Faltante con Tránsito'] = 0.0
            pedidos['Estatus'] = ''
            pedidos['Horario Entrega'] = ''

            # 7. Procesar cada línea de pedido
            for idx, row in pedidos.iterrows():
                material = row['Material']
                cantidad = row[' Pendiente']

                if row.get('TpMt') == 'ZCOM':
                    pedidos.at[idx, 'Faltante'] = 0
                    pedidos.at[idx, 'Faltante con Tránsito'] = 0
                    pedidos.at[idx, 'Horario Entrega'] = 'ZCOM'
                elif row.get('Planta') == 'P5':
                    pedidos.at[idx, 'Faltante'] = 0
                    pedidos.at[idx, 'Faltante con Tránsito'] = 0
                    pedidos.at[idx, 'Horario Entrega'] = 'P5/Expo'

                if material in mapa_inventario:
                    disponible = mapa_inventario[material]
                    pedidos.at[idx, 'Inventario'] = disponible
                    if disponible >= cantidad:
                        pedidos.at[idx, 'Faltante'] = 0
                        mapa_inventario[material] = disponible - cantidad
                    else:
                        pedidos.at[idx, 'Faltante'] = cantidad - disponible
                        mapa_inventario[material] = 0

                if material in mapa_transito:
                    transito = mapa_transito[material]
                    faltante_actual = pedidos.at[idx, 'Faltante']
                    pedidos.at[idx, 'Tránsito'] = transito

                    if transito >= faltante_actual:
                        pedidos.at[idx, 'Faltante con Tránsito'] = 0
                        mapa_transito[material] = transito - faltante_actual
                    else:
                        pedidos.at[idx, 'Faltante con Tránsito'] = faltante_actual - transito
                        mapa_transito[material] = 0

            # 8. Preparar DataFrame
            pedidos = pedidos[list(self.COLUMNAS_SALIDA.keys())]
            pedidos = pedidos.rename(columns=self.COLUMNAS_SALIDA)

            # 9. Procesar Estatus y Horario Entrega
            suma_faltantes_transito = pedidos.groupby('Pedido')['Faltante con Tránsito'].sum()
            pedidos_completos = suma_faltantes_transito[suma_faltantes_transito == 0].index

            pedidos['Estatus'] = pedidos['Pedido'].apply(
                lambda x: 'Completo' if x in pedidos_completos else 'Incompleto'
            )

            pedidos.loc[
                (pedidos['Faltante'] > 0) & (pedidos['Faltante con Tránsito'] == 0), 'Horario Entrega'] = 'Transito'

            # 10. Separar completos e incompletos
            columnas_completos = ['Pedido', 'Marca', 'Cliente', 'Fecha Embarque', 'Liberación en Sistema',
                                  'Horario Entrega']
            completos = pedidos[pedidos['Estatus'] == 'Completo'][columnas_completos].drop_duplicates(subset=['Pedido'])

            incompletos = pedidos[
                (pedidos['Estatus'] == 'Incompleto') &
                ((pedidos['Faltante'] > 0) | (pedidos['Horario Entrega'] == 'Transito'))
                ]

            # Generar reporte de marcas
            reporte_marcas = self.generar_reporte_marcas(pedidos)

            return pedidos, completos, incompletos, reporte_marcas

        except Exception as e:
            st.error(f"Error en el procesamiento: {str(e)}")
            raise


def to_excel(df: pd.DataFrame) -> bytes:
    """
    Convierte un DataFrame a bytes de Excel
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=True)
    return output.getvalue()


def crear_graficas_marca(reporte_marcas: pd.DataFrame):
    """
    Crea visualizaciones para el reporte de marcas de manera segura
    """
    if reporte_marcas.empty:
        return None, None

    # Preparar datos para gráfica de barras apiladas
    df_pedidos = reporte_marcas.reset_index()
    df_pedidos = pd.melt(
        df_pedidos,
        id_vars=['Marca'],
        value_vars=['Pedidos Completos', 'Pedidos Incompletos']
    )

    fig_pedidos = px.bar(
        df_pedidos,
        x='Marca',
        y='value',
        color='variable',
        title='Distribución de Pedidos por Marca',
        labels={'value': 'Número de Pedidos', 'variable': 'Tipo de Pedido'},
        barmode='stack'
    )

    # Gráfica de porcentaje de faltantes
    fig_faltantes = px.bar(
        reporte_marcas.reset_index(),
        x='Marca',
        y='% Faltante',
        title='Porcentaje de Faltantes por Marca',
        labels={'% Faltante': 'Porcentaje Faltante'},
        color='% Faltante',
        color_continuous_scale='Reds'
    )

    return fig_pedidos, fig_faltantes


def aplicar_filtros(df: pd.DataFrame, filtros: dict) -> pd.DataFrame:
    """
    Aplica filtros a un DataFrame de manera independiente
    """
    df_filtrado = df.copy()

    if filtros.get('pedidos') and 'Pedido' in df.columns:
        df_filtrado = df_filtrado[df_filtrado['Pedido'].isin(filtros['pedidos'])]

    if filtros.get('marcas') and 'Marca' in df.columns:
        df_filtrado = df_filtrado[df_filtrado['Marca'].isin(filtros['marcas'])]

    if filtros.get('materiales') and 'Material' in df.columns:
        df_filtrado = df_filtrado[df_filtrado['Material'].isin(filtros['materiales'])]

    if filtros.get('fechas') and 'Fecha Embarque' in df.columns:
        inicio, fin = filtros['fechas']
        df_filtrado = df_filtrado[
            (df_filtrado['Fecha Embarque'].dt.date >= inicio) &
            (df_filtrado['Fecha Embarque'].dt.date <= fin)
            ]

    return df_filtrado


def main():
    st.set_page_config(page_title="Procesador de Pedidos", layout="wide")

    st.title("Procesador de Pedidos e Inventarios")

    # File uploaders
    col1, col2 = st.columns(2)
    with col1:
        archivo_pedidos = st.file_uploader("Archivo de Pedidos", type=['xlsx'])
    with col2:
        archivo_inventarios = st.file_uploader("Archivo de Inventarios", type=['xlsx'])

    if all([archivo_pedidos, archivo_inventarios]):
        try:
            # Procesar datos
            with st.spinner('Procesando archivos...'):
                procesador = ProcesadorPedidos(archivo_pedidos, archivo_inventarios)
                pedidos, completos, incompletos, reporte_marcas = procesador.procesar()

            # Sección de Filtros
            st.header("Filtros de Visualización")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                pedidos_unicos = sorted(pedidos['Pedido'].unique())
                pedidos_filtrados = st.multiselect('Filtrar por Pedido', pedidos_unicos)

            with col2:
                marcas_unicas = sorted(pedidos['Marca'].unique())
                marcas_filtradas = st.multiselect('Filtrar por Marca', marcas_unicas)

            with col3:
                materiales_unicos = sorted(pedidos['Material'].unique())
                materiales_filtrados = st.multiselect('Filtrar por Material', materiales_unicos)

            with col4:
                fecha_min = pedidos['Fecha Embarque'].min().date()
                fecha_max = pedidos['Fecha Embarque'].max().date()
                fechas_filtradas = st.date_input(
                    'Rango de Fechas de Embarque',
                    value=(fecha_min, fecha_max),
                    min_value=fecha_min,
                    max_value=fecha_max
                )

            # Crear diccionario de filtros
            filtros = {
                'pedidos': pedidos_filtrados if pedidos_filtrados else None,
                'marcas': marcas_filtradas if marcas_filtradas else None,
                'materiales': materiales_filtrados if materiales_filtrados else None,
                'fechas': fechas_filtradas if len(fechas_filtradas) == 2 else None
            }

            # Aplicar filtros de manera independiente
            pedidos_viz = aplicar_filtros(pedidos, filtros)
            completos_viz = aplicar_filtros(completos, filtros)
            incompletos_viz = aplicar_filtros(incompletos, filtros)

            # Generar reporte de marcas con datos filtrados
            reporte_marcas_viz = procesador.generar_reporte_marcas(pedidos_viz)

            # Mostrar métricas
            st.header("Resumen General")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Pedidos", len(pedidos_viz['Pedido'].unique()))
            with col2:
                st.metric("Pedidos Completos", len(completos_viz['Pedido'].unique()))
            with col3:
                st.metric("Pedidos Incompletos", len(incompletos_viz['Pedido'].unique()))

            # Análisis por marca
            st.header("Análisis por Marca")
            if not reporte_marcas_viz.empty:
                st.subheader("Resumen por Marca")
                st.dataframe(reporte_marcas_viz, use_container_width=True)

                # Visualizaciones
                fig_pedidos, fig_faltantes = crear_graficas_marca(reporte_marcas_viz)
                if fig_pedidos and fig_faltantes:
                    tab1, tab2 = st.tabs(["Distribución de Pedidos", "Análisis de Faltantes"])
                    with tab1:
                        st.plotly_chart(fig_pedidos, use_container_width=True)
                    with tab2:
                        st.plotly_chart(fig_faltantes, use_container_width=True)
            else:
                st.warning("No hay datos disponibles para mostrar el reporte de marcas")

            # Detalle de pedidos
            st.header("Detalle de Pedidos")
            if not pedidos_viz.empty:
                tabs = st.tabs(["Todos los Pedidos", "Pedidos Completos", "Pedidos Incompletos"])
                with tabs[0]:
                    st.dataframe(pedidos_viz, use_container_width=True)
                with tabs[1]:
                    st.dataframe(completos_viz, use_container_width=True)
                with tabs[2]:
                    st.dataframe(incompletos_viz, use_container_width=True)

                # Botón de descarga
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl', datetime_format='dd/mm/yy') as writer:
                    pedidos_viz.to_excel(writer, sheet_name='Todos los Pedidos', index=False)
                    completos_viz.to_excel(writer, sheet_name='Pedidos Completos', index=False)
                    incompletos_viz.to_excel(writer, sheet_name='Pedidos Incompletos', index=False)

                st.download_button(
                    "Descargar Reporte de Pedidos",
                    data=output.getvalue(),
                    file_name="reporte_pedidos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            # Reporte por marca
            st.header("Reporte por Marca")
            marcas = sorted(incompletos_viz['Marca'].unique())
            if marcas:
                marca_tabs = st.tabs(marcas)
                for marca, tab in zip(marcas, marca_tabs):
                    with tab:
                        df_marca = incompletos_viz[incompletos_viz['Marca'] == marca]
                        st.dataframe(df_marca, use_container_width=True)

                # Botón de descarga para reporte por marca
                output_marca = BytesIO()
                with pd.ExcelWriter(output_marca, engine='openpyxl', datetime_format='dd/mm/yy') as writer:
                    for marca in marcas:
                        df_marca = incompletos_viz[incompletos_viz['Marca'] == marca]
                        nombre_hoja = marca[:31]
                        df_marca.to_excel(writer, sheet_name=nombre_hoja, index=False)

                st.download_button(
                    "Descargar Reporte Detallado por Marca",
                    data=output_marca.getvalue(),
                    file_name="reporte_detallado_por_marca.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.warning("No hay datos de marcas para mostrar")

        except Exception as e:
            st.error(f"Error durante el procesamiento: {str(e)}")
    else:
        st.info("Por favor, suba todos los archivos requeridos para comenzar el procesamiento.")


if __name__ == "__main__":
    main()
