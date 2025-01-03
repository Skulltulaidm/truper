import streamlit as st
import pandas as pd
from typing import Dict, Set
import logging
from datetime import datetime
import io

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

    def __init__(self, df_pedidos: pd.DataFrame, df_inventarios: pd.DataFrame, df_bd_brand: pd.DataFrame):
        """
        Inicializa el procesador con DataFrames.
        """
        self.df_pedidos = df_pedidos
        self.df_inventarios = df_inventarios
        self.df_bd_brand = df_bd_brand

    def procesar(self) -> tuple:
        """
        Procesa los pedidos e inventarios y retorna los DataFrames resultantes
        """
        try:
            # 1. Usar los DataFrames cargados
            pedidos = self.df_pedidos.copy()
            inventarios = self.df_inventarios.copy()
            bd_brand = self.df_bd_brand.copy()

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

                # Procesar inventario disponible
                if material in mapa_inventario:
                    disponible = mapa_inventario[material]
                    pedidos.at[idx, 'Inventario'] = disponible
                    if disponible >= cantidad:
                        pedidos.at[idx, 'Faltante'] = 0
                        mapa_inventario[material] = disponible - cantidad
                    else:
                        pedidos.at[idx, 'Faltante'] = cantidad - disponible
                        mapa_inventario[material] = 0

                # Procesar tránsito
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

            # Asignar Estatus
            pedidos['Estatus'] = pedidos['Pedido'].apply(
                lambda x: 'Completo' if x in pedidos_completos else 'Incompleto'
            )

            # Asignar Horario Entrega para casos de Tránsito
            pedidos.loc[(pedidos['Faltante'] > 0) & (pedidos['Faltante Tránsito'] == 0), 'Horario Entrega'] = 'Transito'

            # 10. Separar completos e incompletos
            columnas_completos = ['Pedido', 'Marca', 'Cliente', 'Fecha Embarque', 'Fecha Liberación', 'Horario Entrega']
            completos = pedidos[pedidos['Estatus'] == 'Completo'][columnas_completos].drop_duplicates(subset=['Pedido'])

            incompletos = pedidos[
                (pedidos['Estatus'] == 'Incompleto') &
                ((pedidos['Faltante'] > 0) | (pedidos['Horario Entrega'] == 'Transito'))
            ]

            # 11. Generar reporte por marca
            dfs_por_marca = self.generar_reporte_por_marca(incompletos)

            return pedidos, completos, incompletos, dfs_por_marca

        except Exception as e:
            st.error(f"Error en el procesamiento: {str(e)}")
            raise

    @staticmethod
    def generar_reporte_por_marca(df_incompletos: pd.DataFrame) -> dict:
        """
        Genera un diccionario de DataFrames por marca, con pedidos incompletos.
        """
        try:
            marcas = df_incompletos['Marca'].unique()
            dfs_por_marca = {}
            for marca in sorted(marcas):
                df_marca = df_incompletos[df_incompletos['Marca'] == marca]
                dfs_por_marca[marca] = df_marca
            return dfs_por_marca
        except Exception as e:
            st.error(f"Error al generar reporte por marca: {str(e)}")
            raise

def main():
    st.set_page_config(
        page_title="Procesador de Pedidos",
        page_icon="📦",
        layout="wide"
    )
    
    st.title("📦 Procesador de Pedidos e Inventarios")
    
    # Sidebar para cargar archivos
    with st.sidebar:
        st.header("📤 Cargar Archivos")
        st.markdown("---")
        
        file_pedidos = st.file_uploader("📋 Archivo de Pedidos", type=['xlsx'])
        file_inventarios = st.file_uploader("📊 Archivo de Inventarios", type=['xlsx'])
        file_bd_brand = st.file_uploader("🏷️ Archivo BD Brand", type=['xlsx'])

        if all([file_pedidos, file_inventarios, file_bd_brand]):
            st.success("✅ Todos los archivos cargados")
        else:
            st.warning("⚠️ Faltan archivos por cargar")

    if all([file_pedidos, file_inventarios, file_bd_brand]):
        try:
            # Cargar los archivos
            with st.spinner("Cargando archivos..."):
                df_pedidos = pd.read_excel(file_pedidos, parse_dates=['Embarque', 'Liberación'])
                df_inventarios = pd.read_excel(file_inventarios)
                df_bd_brand = pd.read_excel(file_bd_brand)

            # Iniciar procesamiento
            procesador = ProcesadorPedidos(df_pedidos, df_inventarios, df_bd_brand)
            
            if st.button("🚀 Procesar Archivos", use_container_width=True):
                with st.spinner("⏳ Procesando archivos..."):
                    pedidos, completos, incompletos, dfs_por_marca = procesador.procesar()

                    # Mostrar resumen
                    st.header("📊 Resumen del Proceso")
                    st.markdown("---")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📝 Total Pedidos", len(pedidos['Pedido'].unique()))
                    with col2:
                        st.metric("✅ Pedidos Completos", len(completos['Pedido'].unique()))
                    with col3:
                        st.metric("⚠️ Pedidos Incompletos", len(incompletos['Pedido'].unique()))

                    # Función para convertir DataFrame a Excel
                    def to_excel(df):
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl', datetime_format='dd/mm/yy') as writer:
                            df.to_excel(writer, index=False)
                        return output.getvalue()

                    # Función para generar Excel con múltiples hojas
                    def to_excel_multiple_sheets(dfs_dict):
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl', datetime_format='dd/mm/yy') as writer:
                            for name, df in dfs_dict.items():
                                sheet_name = name[:31]  # Excel tiene límite de 31 caracteres
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                        return output.getvalue()

                    # Sección de descargas
                    st.header("⬇️ Descargar Resultados")
                    st.markdown("---")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    # Excel con todas las hojas
                    all_sheets = {
                        'Todos los Pedidos': pedidos,
                        'Pedidos Completos': completos,
                        'Pedidos Incompletos': incompletos
                    }
                    
                    with col1:
                        if st.download_button(
                            label="📥 Descargar Reporte Completo",
                            data=to_excel_multiple_sheets(all_sheets),
                            file_name="reporte_pedidos.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        ):
                            st.success("✅ Archivo descargado!")

                    with col2:
                        if st.download_button(
                            label="📥 Descargar Por Marca",
                            data=to_excel_multiple_sheets(dfs_por_marca),
                            file_name="reporte_por_marca.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        ):
                            st.success("✅ Archivo descargado!")

                    # Mostrar tablas en pestañas
                    st.header("👀 Vista Previa de Resultados")
                    st.markdown("---")
                    
                    tabs = st.tabs(["📋 Todos los Pedidos", "✅ Pedidos Completos", "⚠️ Pedidos Incompletos"])
                    
                    with tabs[0]:
                        st.dataframe(pedidos, use_container_width=True)
                    with tabs[1]:
                        st.dataframe(completos, use_container_width=True)
                    with tabs[2]:
                        st.dataframe(incompletos, use_container_width=True)

                    # Mostrar detalles por marca
                    st.header("🏷️ Detalles por Marca")
                    st.markdown("---")
                    
                    # Crear tabs dinámicamente para cada marca
                    if dfs_por_marca:
                        marca_tabs = st.tabs([f"📊 {marca}" for marca in dfs_por_marca.keys()])
                        for tab, (marca, df) in zip(marca_tabs, dfs_por_marca.items()):
                            with tab:
                                st.subheader(f"Pedidos Incompletos - {marca}")
                                st.dataframe(df, use_container_width=True)
                                
                                # Mostrar estadísticas específicas por marca
                                total_faltante = df['Faltante'].sum()
                                total_transito = df['Tránsito'].sum()
                                
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Faltante", f"{total_faltante:,.2f}")
                                with col2:
                                    st.metric("Total en Tránsito", f"{total_transito:,.2f}")
                    else:
                        st.info("No hay pedidos incompletos para mostrar por marca")

        except Exception as e:
            st.error(f"❌ Error durante el procesamiento: {str(e)}")
            logging.error(f"Error detallado: {str(e)}", exc_info=True)
    else:
        st.info("👆 Por favor, carga todos los archivos necesarios en el panel lateral para comenzar el procesamiento.")

if __name__ == "__main__":
    main()
