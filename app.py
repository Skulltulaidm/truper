import streamlit as st
import pandas as pd
from typing import Dict, Set
import logging
from datetime import datetime
from io import BytesIO

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
        'Liberación': 'Fecha Liberación',
        'Inventario': 'Inventario',
        'Faltante': 'Faltante',
        'Faltante Tránsito': 'Faltante Tránsito',
        'Estatus': 'Estatus',
        'Horario Entrega': 'Horario Entrega'
    }

    def __init__(self, archivo_pedidos, archivo_inventarios, archivo_bd_brand):
        """
        Inicializa el procesador con los archivos cargados.
        """
        self.archivo_pedidos = archivo_pedidos
        self.archivo_inventarios = archivo_inventarios
        self.archivo_bd_brand = archivo_bd_brand

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

    def procesar(self) -> tuple:
        """
        Procesa los pedidos e inventarios y retorna los DataFrames resultantes
        """
        try:
            # 1. Cargar datos
            pedidos = self.preprocesar_pedidos()
            inventarios = pd.read_excel(self.archivo_inventarios)
            bd_brand = pd.read_excel(self.archivo_bd_brand)

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
            pedidos['Faltante Tránsito'] = 0.0
            pedidos['Estatus'] = ''
            pedidos['Horario Entrega'] = ''

            # 7. Procesar cada línea de pedido
            for idx, row in pedidos.iterrows():
                material = row['Material']
                cantidad = row[' Pendiente']

                if row.get('Tipo material') == 'ZCOM':
                    pedidos.at[idx, 'Faltante'] = 0
                    pedidos.at[idx, 'Faltante Tránsito'] = 0
                    pedidos.at[idx, 'Horario Entrega'] = 'ZCOM'
                elif row.get('Planta') == 'P5':
                    pedidos.at[idx, 'Faltante'] = 0
                    pedidos.at[idx, 'Faltante Tránsito'] = 0
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
                        pedidos.at[idx, 'Faltante Tránsito'] = 0
                        mapa_transito[material] = transito - faltante_actual
                    else:
                        pedidos.at[idx, 'Faltante Tránsito'] = faltante_actual - transito
                        mapa_transito[material] = 0

            # 8. Preparar DataFrame
            pedidos = pedidos[list(self.COLUMNAS_SALIDA.keys())]
            pedidos = pedidos.rename(columns=self.COLUMNAS_SALIDA)

            # 9. Procesar Estatus y Horario Entrega
            suma_faltantes_transito = pedidos.groupby('Pedido')['Faltante Tránsito'].sum()
            pedidos_completos = suma_faltantes_transito[suma_faltantes_transito == 0].index

            pedidos['Estatus'] = pedidos['Pedido'].apply(
                lambda x: 'Completo' if x in pedidos_completos else 'Incompleto'
            )

            pedidos.loc[(pedidos['Faltante'] > 0) & (pedidos['Faltante Tránsito'] == 0), 'Horario Entrega'] = 'Transito'

            # 10. Separar completos e incompletos
            columnas_completos = ['Pedido', 'Marca', 'Cliente', 'Fecha Embarque', 'Fecha Liberación', 'Horario Entrega']
            completos = pedidos[pedidos['Estatus'] == 'Completo'][columnas_completos].drop_duplicates(subset=['Pedido'])

            incompletos = pedidos[
                (pedidos['Estatus'] == 'Incompleto') &
                ((pedidos['Faltante'] > 0) | (pedidos['Horario Entrega'] == 'Transito'))
            ]

            return pedidos, completos, incompletos

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
    
    st.title("Procesador de Pedidos e Inventarios")
    
    st.markdown("""
    Esta aplicación procesa archivos de pedidos, inventarios y base de datos de marcas.
    Por favor, suba los archivos requeridos en formato Excel (.xlsx).
    """)

    # File uploaders
    col1, col2, col3 = st.columns(3)
    
    with col1:
        archivo_pedidos = st.file_uploader("Archivo de Pedidos", type=['xlsx'])
    
    with col2:
        archivo_inventarios = st.file_uploader("Archivo de Inventarios", type=['xlsx'])
    
    with col3:
        archivo_bd_brand = st.file_uploader("Archivo BD Brand", type=['xlsx'])

    if all([archivo_pedidos, archivo_inventarios, archivo_bd_brand]):
        try:
            with st.spinner('Procesando archivos...'):
                procesador = ProcesadorPedidos(
                    archivo_pedidos,
                    archivo_inventarios,
                    archivo_bd_brand
                )
                pedidos, completos, incompletos = procesador.procesar()

            # Mostrar resumen
            st.subheader("Resumen del Proceso")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Pedidos", len(pedidos['Pedido'].unique()))
            with col2:
                st.metric("Pedidos Completos", len(completos['Pedido'].unique()))
            with col3:
                st.metric("Pedidos Incompletos", len(incompletos['Pedido'].unique()))

            # Tabs para mostrar los diferentes DataFrames
            tab1, tab2, tab3 = st.tabs(["Todos los Pedidos", "Pedidos Completos", "Pedidos Incompletos"])
            
            with tab1:
                st.dataframe(pedidos)
                st.download_button(
                    "Descargar Todos los Pedidos",
                    data=to_excel(pedidos),
                    file_name="todos_los_pedidos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with tab2:
                st.dataframe(completos)
                st.download_button(
                    "Descargar Pedidos Completos",
                    data=to_excel(completos),
                    file_name="pedidos_completos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with tab3:
                st.dataframe(incompletos)
                st.download_button(
                    "Descargar Pedidos Incompletos",
                    data=to_excel(incompletos),
                    file_name="pedidos_incompletos.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            # Generar reporte por marca
            st.subheader("Reporte por Marca")
            marcas = sorted(incompletos['Marca'].unique())
            
            # Create Excel file with multiple sheets
            output = BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for marca in marcas:
                    df_marca = incompletos[incompletos['Marca'] == marca]
                    nombre_hoja = marca[:31]  # Excel limit for sheet names
                    df_marca.to_excel(writer, sheet_name=nombre_hoja, index=False)
            
            st.download_button(
                "Descargar Reporte por Marca",
                data=output.getvalue(),
                file_name="reporte_por_marca.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            st.error(f"Error durante el procesamiento: {str(e)}")
    else:
        st.info("Por favor, suba todos los archivos requeridos para comenzar el procesamiento.")

if __name__ == "__main__":
    main()
