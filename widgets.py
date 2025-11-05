from tkinter import Label, Toplevel
from typing import Any

from .config import TOOLTIP_BACKGROUND, TOOLTIP_OFFSET_X, TOOLTIP_OFFSET_Y


class DicaTooltip:
    """Widget tooltip que exibe informações ao passar o mouse sobre um elemento."""

    def __init__(self, widget: Any, texto: str) -> None:
        """Inicializa o tooltip para um widget."""
        self.widget: Any = widget
        self.texto: str = texto
        self.tooltip: Toplevel | None = None
        widget.bind('<Enter>', self._entrar)
        widget.bind('<Leave>', self._sair)

    def _entrar(self, evento: Any = None) -> None:
        """Exibe o tooltip quando o mouse entra no widget."""
        pos_x, pos_y, _, _ = self.widget.bbox('insert')
        pos_x += self.widget.winfo_rootx() + TOOLTIP_OFFSET_X
        pos_y += self.widget.winfo_rooty() + TOOLTIP_OFFSET_Y
        self.tooltip = Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f'+{pos_x}+{pos_y}')
        rotulo = Label(
            self.tooltip,
            text=self.texto,
            background=TOOLTIP_BACKGROUND,
            relief='solid',
            borderwidth=1,
        )
        rotulo.pack()

    def _sair(self, evento: Any = None) -> None:
        """Remove o tooltip quando o mouse sai do widget."""
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

