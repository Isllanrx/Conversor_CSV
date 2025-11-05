"""Conversor CSV Inteligente - Converte arquivos CSV para múltiplos formatos."""

import gc
import logging
import os
import threading
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from tkinter import Label, Toplevel, filedialog, messagebox
from typing import Any

import customtkinter as ctk  # type: ignore[import]
import pandas as pd  # type: ignore[import]
import psutil  # type: ignore[import]
from tqdm import tqdm  # type: ignore[import]

try:
    import pyarrow as pa  # type: ignore[import]
    import pyarrow.orc as orc  # type: ignore[import]
except ImportError:
    pa = None
    orc = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constantes de configuração
DIRETORIO_SCRIPT: Path = Path(__file__).parent.absolute()
PASTA_CONVERTIDOS: str = str(DIRETORIO_SCRIPT / "CSV_Convertidos")
CODIFICACOES: tuple[str, ...] = ("utf-8", "ISO-8859-1")
TAMANHO_PEDACO: int = 100_000
MAX_ARQUIVOS: int = 100
MAX_TAMANHO_ARQUIVO: int = 2 * 1024 * 1024 * 1024
MAX_TENTATIVAS: int = 3
LIMITE_RAM_BAIXO: int = 3 * 1024 * 1024 * 1024
FORMATOS_PEDACO: set[str] = {"parquet", "h5", "json"}


def garantir_caminho_absoluto(caminho: str | Path) -> str:
    """Garante que o caminho seja absoluto.

    Args:
        caminho: Caminho relativo ou absoluto

    Returns:
        Caminho absoluto como string
    """
    objeto_caminho = Path(caminho)
    if not objeto_caminho.is_absolute():
        objeto_caminho = objeto_caminho.resolve()
    return str(objeto_caminho.absolute())


# Constantes de UI e processamento
TOOLTIP_OFFSET_X: int = 25
TOOLTIP_OFFSET_Y: int = 25
TOOLTIP_BACKGROUND: str = "#ffffe0"
ALTURA_LISTA: int = 100
LARGURA_BOTAO_CREDITOS: int = 150
LARGURA_JANELA: int = 800
ALTURA_JANELA: int = 600


@dataclass
class FormatoConversao:
    """Representa um formato de conversão suportado."""

    nome: str
    funcao: Callable[[pd.DataFrame, str, str | None], None]
    descricao: str
    extensao: str


class DicaTooltip:
    """Widget tooltip que exibe informações ao passar o mouse sobre um elemento."""

    def __init__(self, widget: Any, texto: str) -> None:
        """Inicializa o tooltip para um widget."""
        self.widget: Any = widget
        self.texto: str = texto
        self.tooltip: Toplevel | None = None
        widget.bind("<Enter>", self._entrar)
        widget.bind("<Leave>", self._sair)

    def _entrar(self, evento: Any = None) -> None:
        """Exibe o tooltip quando o mouse entra no widget."""
        pos_x, pos_y, _, _ = self.widget.bbox("insert")
        pos_x += self.widget.winfo_rootx() + TOOLTIP_OFFSET_X
        pos_y += self.widget.winfo_rooty() + TOOLTIP_OFFSET_Y
        self.tooltip = Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f"+{pos_x}+{pos_y}")
        rotulo = Label(
            self.tooltip,
            text=self.texto,
            background=TOOLTIP_BACKGROUND,
            relief="solid",
            borderwidth=1,
        )
        rotulo.pack()

    def _sair(self, evento: Any = None) -> None:
        """Remove o tooltip quando o mouse sai do widget."""
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None


class ConversorCSV:
    """Classe principal para conversão de arquivos CSV."""

    def __init__(self) -> None:
        """Inicializa o conversor e configura o ambiente."""
        os.makedirs(PASTA_CONVERTIDOS, exist_ok=True)
        self.baixo_consumo: bool = psutil.virtual_memory().available < LIMITE_RAM_BAIXO
        self._limpar_memoria()

    def _limpar_memoria(self) -> None:
        """Força coleta de lixo para liberar memória."""
        gc.collect()

    def verificar_recursos(self, arquivos: list[str]) -> str | None:
        """Verifica se os arquivos atendem aos limites de recursos."""
        if len(arquivos) > MAX_ARQUIVOS:
            return f"Limite de {MAX_ARQUIVOS} arquivos excedido."

        try:
            arquivos_absolutos = [garantir_caminho_absoluto(arq) for arq in arquivos]
            tamanho_total: int = sum(os.path.getsize(arq) for arq in arquivos_absolutos)
            if tamanho_total > MAX_TAMANHO_ARQUIVO:
                return "Arquivos muito grandes."
        except (FileNotFoundError, OSError) as erro:
            logging.error(f"Erro ao verificar tamanho dos arquivos: {str(erro)}")
            return f"Erro ao verificar arquivos: {str(erro)}"

        return None

    def ler_csv(self, caminho: str) -> pd.DataFrame | None:
        """Lê um arquivo CSV com múltiplas tentativas e codificações."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)

        if not Path(caminho_absoluto).exists():
            logging.error(f"Arquivo não encontrado: {caminho_absoluto}")
            return None

        for tentativa in range(MAX_TENTATIVAS):
            for codificacao in CODIFICACOES:
                try:
                    dataframe: pd.DataFrame = pd.read_csv(caminho_absoluto, low_memory=False, encoding=codificacao)
                    if dataframe.empty:
                        logging.warning(f"Arquivo {caminho_absoluto} está vazio")
                        return None
                    return dataframe
                except (UnicodeDecodeError, pd.errors.EmptyDataError) as erro:
                    logging.debug(f"Erro ao ler arquivo {caminho_absoluto} com codificação {codificacao}: {str(erro)}")
                    continue
                except (FileNotFoundError, PermissionError) as erro:
                    logging.error(f"Erro de acesso ao arquivo {caminho_absoluto}: {str(erro)}")
                    return None
                except Exception as erro:
                    logging.error(f"Erro inesperado ao ler arquivo {caminho_absoluto}: {str(erro)}")
                    continue
            if tentativa < MAX_TENTATIVAS - 1:
                logging.info(f"Tentativa {tentativa + 1} falhou, tentando novamente...")

        logging.error(f"Falha ao ler arquivo {caminho_absoluto} após {MAX_TENTATIVAS} tentativas")
        return None

    def _salvar_pedaco_parquet(self, pedaco: pd.DataFrame, caminho_saida: str, numero_pedaco: int, total_pedacos: int) -> None:
        """Salva um pedaço de dados em formato Parquet."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho_saida)
        objeto_caminho: Path = Path(caminho_absoluto)

        arquivo_final: str = str(objeto_caminho) if total_pedacos == 1 else str(objeto_caminho.parent / f"{objeto_caminho.stem}_pedaco{numero_pedaco}{objeto_caminho.suffix}")

        Path(arquivo_final).parent.mkdir(parents=True, exist_ok=True)
        try:
            pedaco.to_parquet(arquivo_final, index=False, compression="snappy")
        except OSError as erro:
            logging.error(f"Erro ao salvar pedaço Parquet {numero_pedaco}: {str(erro)}")
            raise

    def _salvar_pedaco_h5(self, pedaco: pd.DataFrame, caminho_saida: str) -> None:
        """Salva um pedaço de dados em formato HDF5."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho_saida)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            with pd.HDFStore(caminho_absoluto, mode="a", complevel=9, complib="blosc") as armazenamento:
                armazenamento.append("dados", pedaco, format="table", data_columns=True)
        except OSError as erro:
            logging.error(f"Erro ao salvar pedaço HDF5: {str(erro)}")
            raise

    def _salvar_pedaco_json(self, pedaco: pd.DataFrame, caminho_saida: str) -> None:
        """Salva um pedaço de dados em formato JSON."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho_saida)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(caminho_absoluto, "a", encoding="utf-8") as arquivo_aberto:
                pedaco.to_json(arquivo_aberto, orient="records", lines=True)
        except OSError as erro:
            logging.error(f"Erro ao salvar pedaço JSON: {str(erro)}")
            raise

    def _processar_pedaco(
        self,
        pedaco: pd.DataFrame,
        caminho_saida: str,
        formato: str,
        numero_pedaco: int,
        total_pedacos: int,
    ) -> None:
        """Processa e salva um pedaço de dados no formato especificado."""
        if formato == "parquet":
            self._salvar_pedaco_parquet(pedaco, caminho_saida, numero_pedaco, total_pedacos)
        elif formato == "h5":
            self._salvar_pedaco_h5(pedaco, caminho_saida)
        elif formato == "json":
            self._salvar_pedaco_json(pedaco, caminho_saida)
        else:
            raise ValueError(f"Formato {formato} não suporta processamento em pedaços")

    def salvar_em_pedacos(self, caminho_csv: str, caminho_saida: str, formato: str) -> None:
        """Salva um arquivo CSV grande processando em pedaços."""
        caminho_csv_absoluto: str = garantir_caminho_absoluto(caminho_csv)
        caminho_saida_absoluto: str = garantir_caminho_absoluto(caminho_saida)

        if not Path(caminho_csv_absoluto).exists():
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {caminho_csv_absoluto}")

        # Otimização: processar pedaços sem carregar tudo na memória
        pedacos_iterador = pd.read_csv(caminho_csv_absoluto, chunksize=TAMANHO_PEDACO)

        # Primeira passagem: contar pedaços e preparar lista
        lista_pedacos: list[pd.DataFrame] = []
        for pedaco in pedacos_iterador:
            lista_pedacos.append(pedaco)

        total_pedacos: int = len(lista_pedacos)

        if total_pedacos == 0:
            logging.warning(f"Arquivo CSV {caminho_csv_absoluto} está vazio ou não pôde ser lido")
            return

        with tqdm(total=total_pedacos, desc="Processando pedaços") as barra_progresso:
            for numero_pedaco, pedaco in enumerate(lista_pedacos, start=1):
                try:
                    self._processar_pedaco(pedaco, caminho_saida_absoluto, formato, numero_pedaco, total_pedacos)
                    barra_progresso.update(1)
                    self._limpar_memoria()
                except (OSError, ValueError) as erro:
                    logging.error(f"Erro ao processar pedaço {numero_pedaco}/{total_pedacos}: {str(erro)}")
                    raise
                except Exception as erro:
                    logging.error(f"Erro inesperado ao processar pedaço {numero_pedaco}: {str(erro)}")
                    raise

    def _salvar_parquet(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato Parquet."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_parquet(caminho_absoluto, index=False, compression="snappy")
        except OSError as erro:
            logging.error(f"Erro ao salvar Parquet {caminho}: {str(erro)}")
            raise

    def _salvar_feather(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato Feather."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_feather(caminho_absoluto)
        except OSError as erro:
            logging.error(f"Erro ao salvar Feather {caminho}: {str(erro)}")
            raise

    def _salvar_h5(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato HDF5."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_hdf(
                caminho_absoluto,
                key="dados",
                mode="w",
                format="table",
                complevel=9,
                complib="blosc",
            )
        except OSError as erro:
            logging.error(f"Erro ao salvar HDF5 {caminho}: {str(erro)}")
            raise

    def _salvar_json(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato JSON."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_json(caminho_absoluto, orient="records", lines=True)
        except OSError as erro:
            logging.error(f"Erro ao salvar JSON {caminho}: {str(erro)}")
            raise

    def _salvar_pkl(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato Pickle."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_pickle(caminho_absoluto)
        except OSError as erro:
            logging.error(f"Erro ao salvar Pickle {caminho}: {str(erro)}")
            raise

    def _salvar_orc(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato ORC."""
        if pa is None or orc is None:
            raise ImportError("pyarrow é necessário para salvar em formato ORC")

        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(caminho_absoluto, "wb") as arquivo_aberto:
                orc.write_table(pa.Table.from_pandas(dataframe), arquivo_aberto)
        except OSError as erro:
            logging.error(f"Erro ao salvar ORC {caminho_absoluto}: {str(erro)}")
            raise

    def salvar(
        self,
        dataframe: pd.DataFrame,
        caminho: str,
        formato: str,
        caminho_csv: str | None = None,
    ) -> None:
        """Salva um DataFrame no formato especificado."""
        if dataframe is None or dataframe.empty:
            raise ValueError("DataFrame vazio ou inválido")

        # Garantir que todos os caminhos sejam absolutos
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        caminho_csv_absoluto: str | None = garantir_caminho_absoluto(caminho_csv) if caminho_csv else None

        if self.baixo_consumo and formato in FORMATOS_PEDACO and caminho_csv_absoluto:
            self.salvar_em_pedacos(caminho_csv_absoluto, caminho_absoluto, formato)
            return

        manipuladores: dict[str, Callable[[pd.DataFrame, str], None]] = {
            "parquet": self._salvar_parquet,
            "feather": self._salvar_feather,
            "h5": self._salvar_h5,
            "json": self._salvar_json,
            "pkl": self._salvar_pkl,
            "orc": self._salvar_orc,
        }

        manipulador: Callable[[pd.DataFrame, str], None] | None = manipuladores.get(formato)
        if manipulador:
            try:
                manipulador(dataframe, caminho_absoluto)
            except (OSError, ImportError) as erro:
                logging.error(f"Erro ao salvar arquivo {caminho_absoluto}: {str(erro)}")
                raise
        else:
            raise ValueError(f"Formato {formato} não suportado")


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
        """Abre diálogo para seleção de arquivos CSV.

        Valida os arquivos selecionados contra limites de recursos e
        atualiza a interface com os arquivos escolhidos.
        """
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
                    dataframe: pd.DataFrame | None = self.conversor.ler_csv(arquivo_absoluto)
                    if dataframe is not None:
                        caminho_saida: str = garantir_caminho_absoluto(Path(PASTA_CONVERTIDOS) / f"{Path(arquivo_absoluto).stem}.{formato.extensao}")
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


if __name__ == "__main__":
    app: AplicacaoConversor = AplicacaoConversor()
    app.mainloop()
