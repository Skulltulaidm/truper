from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict

@dataclass
class Pedido:
    id: str
    marca: str
    cliente: str
    material: str
    descripcion: str
    cantidad_solicitada: float
    transito: float
    centro: str
    fecha_embarque: datetime
    liberacion: datetime
    inventario: float
    faltante: float
    faltante_transito: float
    estatus: str
    horario_entrega: str

@dataclass
class Inventario:
    material: str
    centro: str
    disponible: float
    transito: float
    caracteristica: str
