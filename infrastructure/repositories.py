import pandas as pd
from typing import List, Dict
from domain.entities import Pedido, Inventario
from domain.repositories import PedidoRepository

class PandasPedidoRepository(PedidoRepository):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_all(self) -> List[Pedido]:
        return [self._to_entity(row) for _, row in self.df.iterrows()]

    def get_by_marca(self, marca: str) -> List[Pedido]:
        filtered_df = self.df[self.df['Marca'] == marca]
        return [self._to_entity(row) for _, row in filtered_df.iterrows()]

    def _to_entity(self, row: pd.Series) -> Pedido:
        return Pedido(
            id=row['Pedido'],
            marca=row['Marca'],
            cliente=row['Cliente'],
            material=row['Material'],
            descripcion=row['Descripción'],
            cantidad_solicitada=row['Ctd. Sol.'],
            transito=row['Tránsito'],
            centro=row['CE'],
            fecha_embarque=row['Fecha Embarque'],
            liberacion=row['Liberación en Sistema'],
            inventario=row['Inventario'],
            faltante=row['Faltante'],
            faltante_transito=row['Faltante con Tránsito'],
            estatus=row['Estatus'],
            horario_entrega=row['Horario Entrega']
        )
