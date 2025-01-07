from abc import ABC, abstractmethod
from typing import List, Dict
from .entities import Pedido, Inventario

class PedidoRepository(ABC):
    @abstractmethod
    def get_all(self) -> List[Pedido]:
        pass

    @abstractmethod
    def get_by_marca(self, marca: str) -> List[Pedido]:
        pass

    @abstractmethod
    def save(self, pedido: Pedido) -> None:
        pass
