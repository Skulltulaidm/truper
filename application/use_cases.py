# application/use_cases.py
from typing import Optional, Tuple, List, Dict
import pandas as pd
import numpy as np
from collections import defaultdict
from domain.repositories import PedidoRepository
from infrastructure.cache import CacheManager, InMemoryCacheManager
from io import BytesIO
import hashlib

class ProcesarPedidosUseCase:
    MARCAS_PERMITIDAS = {"Marca privada Exp.", "Producto de Catálogo Americano"}
    CENTROS_REQUERIDOS = {"EXPO", "LARE"}
    
    def __init__(
        self,
        pedido_repository: Optional[PedidoRepository] = None,
        cache_manager: Optional[CacheManager] = None
    ):
        self.pedido_repository = pedido_repository
        self.cache_manager = cache_manager or InMemoryCacheManager()

    def _generate_cache_key(self, archivo_pedidos: BytesIO, archivo_inventarios: BytesIO) -> str:
        """
        Genera una clave única para el caché basada en el contenido de los archivos
        """
        try:
            # Generar hash del contenido de los archivos
            pedidos_hash = hashlib.md5(archivo_pedidos.getvalue()).hexdigest()
            inventarios_hash = hashlib.md5(archivo_inventarios.getvalue()).hexdigest()
            
            # Combinar los hashes para crear una clave única
            return f"pedidos_{pedidos_hash}_inventarios_{inventarios_hash}"
        except Exception as e:
            # Si hay algún error, retornar None para evitar el uso de caché
            return None

    def _preprocesar_pedidos(self, archivo_pedidos: BytesIO) -> pd.DataFrame:
        """
        Preprocesa el archivo de pedidos
        """
        try:
            pedidos = pd.read_excel(
                archivo_pedidos,
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
        except Exception as e:
            raise Exception(f"Error en preprocesamiento de pedidos: {str(e)}")

    def _preprocesar_inventarios(self, archivo_inventarios: BytesIO) -> pd.DataFrame:
        """
        Preprocesa el archivo de inventarios
        """
        try:
            return pd.read_excel(archivo_inventarios)
        except Exception as e:
            raise Exception(f"Error en preprocesamiento de inventarios: {str(e)}")

    def _generar_reporte_marcas(self, pedidos: pd.DataFrame) -> pd.DataFrame:
        """
        Genera reporte detallado por marca
        """
        try:
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
            reporte_marcas['% Completos'] = (reporte_marcas['Pedidos Completos'] / reporte_marcas['Total Pedidos'] * 100).round(2)
            reporte_marcas['% Incompletos'] = (reporte_marcas['Pedidos Incompletos'] / reporte_marcas['Total Pedidos'] * 100).round(2)
            
            # Valor total de pedidos
            reporte_marcas['Total Solicitado (pz)'] = pedidos.groupby('Marca')['Ctd. Sol.'].sum()
            
            # Valor faltante
            reporte_marcas['Faltante (pz)'] = pedidos.groupby('Marca')['Faltante'].sum()
            reporte_marcas['% Faltante'] = (reporte_marcas['Faltante (pz)'] / reporte_marcas['Total Solicitado (pz)'] * 100).round(2)
            
            return reporte_marcas
        except Exception as e:
            raise Exception(f"Error generando reporte de marcas: {str(e)}")

    def execute(
        self,
        archivo_pedidos: BytesIO,
        archivo_inventarios: BytesIO
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Ejecuta el procesamiento principal de pedidos e inventarios
        """
        try:
            # Verificar caché
            cache_key = self._generate_cache_key(archivo_pedidos, archivo_inventarios)
            if cache_key:
                cached_result = self.cache_manager.get(cache_key)
                if cached_result:
                    return cached_result

            # Preprocesar datos
            pedidos = self._preprocesar_pedidos(archivo_pedidos)
            inventarios = self._preprocesar_inventarios(archivo_inventarios)

            # Filtrar pedidos
            pedidos = pedidos[
                (pedidos['Muestra'] != 'X') &
                (pedidos['Descripción'].isin(self.MARCAS_PERMITIDAS)) &
                (~pedidos['Nombre 1'].str.contains('James Palin', na=False))
            ]

            # Procesar inventarios
            inventarios = inventarios[inventarios['Carac. Planif.'] != 'ND']
            
            # Crear mapas para búsqueda O(1)
            mapa_inventario = defaultdict(float, inventarios.set_index('Material')['Disponible'].to_dict())
            mapa_transito = defaultdict(float, inventarios.set_index('Material')['Traslado'].to_dict())

            # Procesar pedidos de manera vectorizada
            pedidos['Inventario'] = pedidos['Material'].map(mapa_inventario)
            pedidos['Tránsito'] = pedidos['Material'].map(mapa_transito)
            
            # Calcular faltantes
            pedidos['Faltante'] = np.maximum(0, pedidos['Pendiente'] - pedidos['Inventario'])
            pedidos['Faltante con Tránsito'] = np.maximum(0, pedidos['Faltante'] - pedidos['Tránsito'])

            # Determinar estado de pedidos
            pedidos['Estatus'] = np.where(pedidos['Faltante con Tránsito'] == 0, 'Completo', 'Incompleto')

            # Separar pedidos
            completos = pedidos[pedidos['Estatus'] == 'Completo']
            incompletos = pedidos[pedidos['Estatus'] == 'Incompleto']

            # Generar reporte de marcas
            reporte_marcas = self._generar_reporte_marcas(pedidos)

            # Guardar en caché
            if cache_key:
                self.cache_manager.set(
                    cache_key,
                    (pedidos, completos, incompletos, reporte_marcas),
                    ttl=3600  # 1 hora de caché
                )

            return pedidos, completos, incompletos, reporte_marcas

        except Exception as e:
            raise Exception(f"Error en procesamiento: {str(e)}")
