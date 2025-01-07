from enum import Enum
from dataclasses import dataclass

class EstadoPedido(Enum):
    COMPLETO = "Completo"
    INCOMPLETO = "Incompleto"

class TipoPedido(Enum):
    ZCOM = "ZCOM"
    P5 = "P5"
    REGULAR = "REGULAR"
