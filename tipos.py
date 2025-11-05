"""Tipos de dados e estruturas do conversor."""

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd  # type: ignore[import]


@dataclass
class FormatoConversao:
    """Representa um formato de conversão suportado."""

    nome: str
    funcao: Callable[[pd.DataFrame, str, str | None], None]
    descricao: str
    extensao: str
