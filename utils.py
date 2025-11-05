"""Funções utilitárias para manipulação de caminhos."""

from pathlib import Path


def garantir_caminho_absoluto(caminho: str | Path) -> str:
    """Garante que o caminho seja absoluto"""
    objeto_caminho = Path(caminho)
    if not objeto_caminho.is_absolute():
        objeto_caminho = objeto_caminho.resolve()
    return str(objeto_caminho.absolute())

