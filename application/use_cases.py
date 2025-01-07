from typing import List, Tuple, Dict
from domain.entities import Pedido, Inventario
from domain.repositories import PedidoRepository
import pandas as pd
from collections import defaultdict

class ProcesarPedidosUseCase:
    def __init__(
        self,
        pedido_repository: PedidoRepository,
        cache_manager: Optional[CacheManager] = None
    ):
        self.pedido_repository = pedido_repository
        self.cache_manager = cache_manager or InMemoryCacheManager()

    def execute(
        self,
        archivo_pedidos: BytesIO,
        archivo_inventarios: BytesIO
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # Usar caché si está disponible
        cache_key = self._generate_cache_key(archivo_pedidos, archivo_inventarios)
        cached_result = self.cache_manager.get(cache_key)
        if cached_result:
            return cached_result

        try:
            pedidos_df = self._procesar_pedidos(archivo_pedidos)
            inventarios_df = self._procesar_inventarios(archivo_inventarios)
            
            # Usar estructuras de datos optimizadas
            inventario_map = defaultdict(float)
            transito_map = defaultdict(float)
            
            # Procesar inventarios usando diccionarios para O(1) lookup
            for _, row in inventarios_df.iterrows():
                inventario_map[row['Material']] = row['Disponible']
                transito_map[row['Material']] = row['Traslado']

            # Procesar pedidos de manera vectorizada
            resultado = self._procesar_pedidos_vectorizado(
                pedidos_df,
                inventario_map,
                transito_map
            )

            # Guardar en caché
            self.cache_manager.set(cache_key, resultado)
            
            return resultado

        except Exception as e:
            logging.error(f"Error en procesamiento: {str(e)}")
            raise

    def _procesar_pedidos_vectorizado(
        self,
        pedidos: pd.DataFrame,
        inventario_map: Dict[str, float],
        transito_map: Dict[str, float]
    ) -> pd.DataFrame:
        # Aplicar operaciones vectorizadas
        pedidos['Inventario'] = pedidos['Material'].map(inventario_map)
        pedidos['Tránsito'] = pedidos['Material'].map(transito_map)
        
        # Calcular faltantes de manera vectorizada
        pedidos['Faltante'] = np.maximum(
            0,
            pedidos['Cantidad'] - pedidos['Inventario']
        )
        
        # Calcular faltantes con tránsito
        pedidos['Faltante con Tránsito'] = np.maximum(
            0,
            pedidos['Faltante'] - pedidos['Tránsito']
        )

        return pedidos
