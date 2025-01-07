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
        self.archivo_pedidos = archivo_pedidos
        self.archivo_inventarios = archivo_inventarios
        self.archivo_bd_brand = pd.read_excel("BD Brand.xlsx")

    def preprocesar_pedidos(self) -> pd.DataFrame:
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
        if pedidos.empty:
            return pd.DataFrame()

        reporte_marcas = pd.DataFrame()

        # Calculamos el estado de cada pedido
        estado_pedidos = pedidos.groupby('Pedido')['Estatus'].agg(
            lambda x: 'Completo' if all(x == 'Completo') else 'Incompleto'
        )

        # Agrupar por marca
        reporte_marcas['Total Pedidos'] = pedidos.groupby('Marca')['Pedido'].nunique()

        # Pedidos completos por marca
        pedidos_marca = pedidos.groupby(['Marca', 'Pedido'])['Estatus'].agg(
            lambda x: 'Completo' if all(x == 'Completo') else 'Incompleto'
        ).reset_index()

        reporte_marcas['Pedidos Completos'] = pedidos_marca[
            pedidos_marca['Estatus'] == 'Completo'
        ].groupby('Marca')['Pedido'].nunique()

        reporte_marcas['Pedidos Incompletos'] = pedidos_marca[
            pedidos_marca['Estatus'] == 'Incompleto'
        ].groupby('Marca')['Pedido'].nunique()

        # Llenar NaN con 0
        reporte_marcas = reporte_marcas.fillna(0)

        # Calcular porcentajes
        reporte_marcas['% Completos'] = (reporte_marcas['Pedidos Completos'] / reporte_marcas['Total Pedidos'] * 100).round(2)
        reporte_marcas['% Incompletos'] = (reporte_marcas['Pedidos Incompletos'] / reporte_marcas['Total Pedidos'] * 100).round(2)

        # Valor total de pedidos
        reporte_marcas['Total Solicitado (pz)'] = pedidos.groupby('Marca')['Ctd. Sol.'].sum()

        # Valor faltante
        reporte_marcas['Faltante (pz)'] = pedidos.groupby('Marca')['Faltante'].sum()
        reporte_marcas['% Faltante'] = (reporte_marcas['Faltante (pz)'] / reporte_marcas['Total Solicitado (pz)'] * 100).round(2)

        return reporte_marcas

    def procesar(self) -> tuple:
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

            # 9. Procesar Estatus
            pedidos['Estatus'] = pedidos.apply(
                lambda row: 'Completo' if row['Faltante'] == 0 else 'Incompleto',
                axis=1
            )

            return pedidos, None, None, None  # Solo retornamos pedidos, el resto se calculará después

        except Exception as e:
            st.error(f"Error en el procesamiento: {str(e)}")
            raise

def aplicar_filtros_y_contar(pedidos: pd.DataFrame, filtros: dict):
    """
    Aplica filtros al DataFrame principal y calcula todas las métricas
    """
    # Primero aplicamos los filtros al DataFrame principal
    df_filtrado = pedidos.copy()

    if filtros.get('pedidos'):
        df_filtrado = df_filtrado[df_filtrado['Pedido'].isin(filtros['pedidos'])]

    if filtros.get('marcas'):
        df_filtrado = df_filtrado[df_filtrado['Marca'].isin(filtros['marcas'])]

    if filtros.get('materiales'):
        df_filtrado = df_filtrado[df_filtrado['Material'].isin(filtros['materiales'])]

    if filtros.get('fechas'):
        inicio, fin = filtros['fechas']
        df_filtrado = df_filtrado[
            (df_filtrado['Fecha Embarque'].dt.date >= inicio) &
            (df_filtrado['Fecha Embarque'].dt.date <= fin)
        ]

    # Calculamos las métricas desde el DataFrame filtrado
    total_pedidos = len(df_filtrado['Pedido'].unique())

    # Para pedidos completos/incompletos, primero agrupamos por pedido y verificamos el estado
    estado_pedidos = df_filtrado.groupby('Pedido')['Estatus'].agg(
        lambda x: 'Completo' if all(x == 'Completo') else 'Incompleto'
    )

    total_completos = sum(estado_pedidos == 'Completo')
    total_incompletos = sum(estado_pedidos == 'Incompleto')

    # Separar los DataFrames filtrados
    pedidos_completos = df_filtrado[df_filtrado['Pedido'].isin(estado_pedidos[estado_pedidos == 'Completo'].index)]
    pedidos_incompletos = df_filtrado[df_filtrado['Pedido'].isin(estado_pedidos[estado_pedidos == 'Incompleto'].index)]

    # Agrupar pedidos completos por datos únicos
    if not pedidos_completos.empty:
        pedidos_completos = pedidos_completos.groupby('Pedido').agg({
            'Marca': 'first',
            'Cliente': 'first',
            'Fecha Embarque': 'first',
            'Liberación en Sistema': 'first',
            'Horario Entrega': 'first'
        }).reset_index()

    return {
        'df_filtrado': df_filtrado,
        'pedidos_completos': pedidos_completos,
        'pedidos_incompletos': pedidos_incompletos,
        'metricas': {
            'total_pedidos': total_pedidos,
            'total_completos': total_completos,
            'total_incompletos': total_incompletos
        }
    }

def crear_graficas_marca(reporte_marcas: pd.DataFrame):
    """
    Crea visualizaciones para el reporte de marcas
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
                pedidos, _, _, _ = procesador.procesar()  # Solo necesitamos pedidos inicialmente

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

            # Aplicar filtros y obtener resultados
            resultados = aplicar_filtros_y_contar(pedidos, filtros)

            # Mostrar métricas
            st.header("Resumen General")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Pedidos", resultados['metricas']['total_pedidos'])
            with col2:
                st.metric("Pedidos Completos", resultados['metricas']['total_completos'])
            with col3:
                st.metric("Pedidos Incompletos", resultados['metricas']['total_incompletos'])

            # Generar reporte de marcas con datos filtrados
            reporte_marcas_viz = procesador.generar_reporte_marcas(resultados['df_filtrado'])

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
            if not resultados['df_filtrado'].empty:
                tabs = st.tabs(["Todos los Pedidos", "Pedidos Completos", "Pedidos Incompletos"])
                with tabs[0]:
                    st.dataframe(resultados['df_filtrado'], use_container_width=True)
                with tabs[1]:
                    if not resultados['pedidos_completos'].empty:
                        st.dataframe(resultados['pedidos_completos'], use_container_width=True)
                    else:
                        st.info("No hay pedidos completos para mostrar")
                with tabs[2]:
                    if not resultados['pedidos_incompletos'].empty:
                        st.dataframe(resultados['pedidos_incompletos'], use_container_width=True)
                    else:
                        st.info("No hay pedidos incompletos para mostrar")

                # Botón de descarga
                output = BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl', datetime_format='dd/mm/yy') as writer:
                    resultados['df_filtrado'].to_excel(writer, sheet_name='Todos los Pedidos', index=False)
                    if not resultados['pedidos_completos'].empty:
                        resultados['pedidos_completos'].to_excel(writer, sheet_name='Pedidos Completos', index=False)
                    if not resultados['pedidos_incompletos'].empty:
                        resultados['pedidos_incompletos'].to_excel(writer, sheet_name='Pedidos Incompletos', index=False)

                st.download_button(
                    "Descargar Reporte de Pedidos",
                    data=output.getvalue(),
                    file_name="reporte_pedidos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            # Reporte por marca
            st.header("Reporte por Marca")
            marcas = sorted(resultados['pedidos_incompletos']['Marca'].unique()) if not resultados['pedidos_incompletos'].empty else []
            if marcas:
                marca_tabs = st.tabs(marcas)
                for marca, tab in zip(marcas, marca_tabs):
                    with tab:
                        df_marca = resultados['pedidos_incompletos'][
                            resultados['pedidos_incompletos']['Marca'] == marca
                        ]
                        st.dataframe(df_marca, use_container_width=True)

                # Botón de descarga para reporte por marca
                output_marca = BytesIO()
                with pd.ExcelWriter(output_marca, engine='openpyxl', datetime_format='dd/mm/yy') as writer:
                    for marca in marcas:
                        df_marca = resultados['pedidos_incompletos'][
                            resultados['pedidos_incompletos']['Marca'] == marca
                        ]
                        nombre_hoja = marca[:31]  # Excel tiene un límite de 31 caracteres para nombres de hojas
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
            logging.error(f"Error durante el procesamiento: {str(e)}")
    else:
        st.info("Por favor, suba todos los archivos requeridos para comenzar el procesamiento.")

if __name__ == "__main__":
    main()
