"""Interface gráfica do conversor CSV."""

import logging
import threading
import webbrowser
from pathlib import Path
from tkinter import filedialog, messagebox

import customtkinter as ctk  # type: ignore[import]

from .config import (
    ALTURA_JANELA,
    ALTURA_LISTA,
    LARGURA_BOTAO_CREDITOS,
    LARGURA_JANELA,
    PASTA_CONVERTIDOS,
)
from .conversor import ConversorCSV
from .tipos import FormatoConversao
from .utils import garantir_caminho_absoluto
from .widgets import DicaTooltip

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class AplicacaoConversor(ctk.CTk):
    """Aplicação GUI para conversão de arquivos CSV."""

    def __init__(self) -> None:
        """Inicializa a aplicação e cria a interface gráfica."""
        super().__init__()
        self.title("Conversor CSV Inteligente")
        self.geometry(f"{LARGURA_JANELA}x{ALTURA_JANELA}")
        self.arquivos: list[str] = []
        self.conversor: ConversorCSV = ConversorCSV()
        self.formatos: list[FormatoConversao] = [
            FormatoConversao(
                "Parquet",
                lambda dataframe, caminho, caminho_csv: self.conversor.salvar(dataframe, caminho, "parquet", caminho_csv),
                "Compactado para Big Data",
                "parquet",
            ),
            FormatoConversao(
                "Feather",
                lambda dataframe, caminho, caminho_csv: self.conversor.salvar(dataframe, caminho, "feather"),
                "Leitura rápida",
                "feather",
            ),
            FormatoConversao(
                "ORC",
                lambda dataframe, caminho, caminho_csv: self.conversor.salvar(dataframe, caminho, "orc"),
                "Compatível Hive/Spark",
                "orc",
            ),
            FormatoConversao(
                "HDF5",
                lambda dataframe, caminho, caminho_csv: self.conversor.salvar(dataframe, caminho, "h5", caminho_csv),
                "Formato binário estruturado",
                "h5",
            ),
            FormatoConversao(
                "Pickle",
                lambda dataframe, caminho, caminho_csv: self.conversor.salvar(dataframe, caminho, "pkl"),
                "Serialização Python",
                "pkl",
            ),
            FormatoConversao(
                "JSON",
                lambda dataframe, caminho, caminho_csv: self.conversor.salvar(dataframe, caminho, "json", caminho_csv),
                "Formato universal",
                "json",
            ),
        ]
        self.progresso: ctk.CTkProgressBar | None = None
        self.status: ctk.CTkLabel | None = None
        self.lista: ctk.CTkTextbox | None = None
        self._criar_widgets()

    def _criar_widgets(self) -> None:
        """Cria e organiza todos os widgets da interface gráfica."""
        quadro_principal: ctk.CTkFrame = ctk.CTkFrame(self)
        quadro_principal.pack(fill="both", expand=True, padx=10, pady=10)

        quadro_superior: ctk.CTkFrame = ctk.CTkFrame(quadro_principal)
        quadro_superior.pack(fill="both", expand=True, pady=(0, 10))

        ctk.CTkButton(quadro_superior, text="Selecionar Arquivos", command=self.selecionar_arquivos).pack(pady=10)

        self.lista = ctk.CTkTextbox(quadro_superior, height=ALTURA_LISTA)
        self.lista.pack(pady=5, fill="both", expand=True)

        quadro_conversao: ctk.CTkFrame = ctk.CTkFrame(quadro_principal)
        quadro_conversao.pack(fill="x", pady=(0, 10))

        for formato in self.formatos:
            quadro: ctk.CTkFrame = ctk.CTkFrame(quadro_conversao)
            quadro.pack(pady=2, padx=5, fill="x")
            ctk.CTkButton(
                quadro,
                text=f"Converter para {formato.nome}",
                command=lambda f=formato: self.converter_em_thread(f),
            ).pack(side="left")
            rotulo: ctk.CTkLabel = ctk.CTkLabel(quadro, text="i")
            rotulo.pack(side="left", padx=5)
            DicaTooltip(rotulo, formato.descricao)

        quadro_inferior: ctk.CTkFrame = ctk.CTkFrame(quadro_principal)
        quadro_inferior.pack(fill="x", pady=(0, 10))

        self.status = ctk.CTkLabel(quadro_inferior, text="")
        self.status.pack(pady=10)

        self.progresso = ctk.CTkProgressBar(quadro_inferior)
        self.progresso.pack(pady=5, fill="x", padx=10)
        self.progresso.set(0)

        quadro_creditos: ctk.CTkFrame = ctk.CTkFrame(quadro_principal)
        quadro_creditos.pack(fill="x", side="bottom", pady=(10, 0))
        ctk.CTkButton(
            quadro_creditos,
            text="Desenvolvido por Isllan Toso",
            width=LARGURA_BOTAO_CREDITOS,
            command=self._abrir_linkedin,
        ).pack(side="right", padx=10, pady=5)

    def selecionar_arquivos(self) -> None:
        """Abre diálogo para seleção de arquivos CSV."""
        resultado: str | tuple[str, ...] = filedialog.askopenfilenames(filetypes=[("CSV", "*.csv")])
        arquivos: tuple[str, ...] = resultado if isinstance(resultado, tuple) and resultado else ()

        if not arquivos:
            return

        erro: str | None = self.conversor.verificar_recursos(list(arquivos))
        if erro:
            messagebox.showerror("Erro", erro)
            return

        self.arquivos = list(arquivos)
        if self.lista is not None and self.status is not None:
            self.lista.delete("1.0", "end")
            nomes: list[str] = [f"{Path(a).name}\n" for a in self.arquivos]
            self.lista.insert("end", "".join(nomes))
            if self.conversor.baixo_consumo:
                self.status.configure(text="AVISO: Modo de Baixo Consumo de RAM ativado.")

    def _abrir_linkedin(self) -> None:
        """Abre o perfil do LinkedIn do desenvolvedor."""
        webbrowser.open("https://www.linkedin.com/in/isllantoso/")

    def converter_em_thread(self, formato: FormatoConversao) -> None:
        """Inicia a conversão em thread separada para não bloquear a UI."""
        if not self.arquivos:
            messagebox.showwarning("Aviso", "Por favor, selecione arquivos primeiro.")
            return

        threading.Thread(target=self.converter, args=(formato,), daemon=True).start()

    def converter(self, formato: FormatoConversao) -> None:
        """Realiza a conversão dos arquivos selecionados."""
        if self.status is None or self.progresso is None:
            return

        try:
            self.status.configure(text=f"Convertendo para {formato.nome}...")
            self.progresso.set(0)
            total_arquivos: int = len(self.arquivos)

            for indice, arquivo in enumerate(self.arquivos):
                try:
                    arquivo_absoluto: str = garantir_caminho_absoluto(arquivo)
                    dataframe = self.conversor.ler_csv(arquivo_absoluto)
                    if dataframe is not None:
                        caminho_saida: str = garantir_caminho_absoluto(str(Path(PASTA_CONVERTIDOS).resolve() / f"{Path(arquivo_absoluto).resolve().stem}.{formato.extensao}"))
                        formato.funcao(dataframe, caminho_saida, arquivo_absoluto)
                        logging.info(f"Convertido: {caminho_saida}")
                    self.progresso.set((indice + 1) / total_arquivos)
                except (OSError, ValueError, ImportError) as erro:
                    logging.error(f"Erro ao converter {arquivo}: {str(erro)}")
                    messagebox.showerror("Erro", f"Erro ao converter {Path(arquivo).name}: {str(erro)}")
                except Exception as erro:
                    logging.error(f"Erro inesperado ao converter {arquivo}: {str(erro)}")
                    messagebox.showerror(
                        "Erro",
                        f"Erro inesperado ao converter {Path(arquivo).name}: {str(erro)}",
                    )

            self.status.configure(text=f"SUCESSO: Conversão para {formato.nome} finalizada.")
            self.progresso.set(1)
        except Exception as erro:
            logging.error(f"Erro durante a conversão: {str(erro)}")
            messagebox.showerror("Erro", f"Erro durante a conversão: {str(erro)}")
