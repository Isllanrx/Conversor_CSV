import gc
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import]
import psutil  # type: ignore[import]

from .config import (
    FORMATOS_PEDACO,
    LIMITE_RAM_BAIXO,
    MAX_ARQUIVOS,
    MAX_TAMANHO_ARQUIVO,
    PASTA_CONVERTIDOS,
)
from .detector_csv import DetectorCSV
from .salvadores import Salvadores
from .utils import garantir_caminho_absoluto

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class ConversorCSV:
    """Classe principal para conversão de arquivos CSV."""

    def __init__(self) -> None:
        """Inicializa o conversor e configura o ambiente."""
        os.makedirs(PASTA_CONVERTIDOS, exist_ok=True)
        self.baixo_consumo: bool = psutil.virtual_memory().available < LIMITE_RAM_BAIXO
        self._limpar_memoria()
        self.salvadores = Salvadores(self.baixo_consumo)
        self.detector = DetectorCSV()

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
        """Lê um arquivo CSV com detecção automática de características."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)

        if not Path(caminho_absoluto).resolve().exists():
            logging.error(f"Arquivo não encontrado: {caminho_absoluto}")
            return None

        try:
            # Detectar configuração automaticamente
            config = self.detector.detectar_configuracao(caminho_absoluto)

            logging.info(f"Lendo CSV com: encoding={config['encoding']}, delimiter={repr(config['delimiter'])}, compression={config['compression']}")

            # Preparação para pd.read_csv
            parametros_leitura: dict[str, Any] = {
                "low_memory": False,
                "encoding": config["encoding"],
                "sep": config["delimiter"],
            }

            # Adicionar aspas se detectadas (apenas se não for None)
            if config.get("quote_char"):
                parametros_leitura["quotechar"] = config["quote_char"]

            # Configurar doublequote se detectado
            if config.get("doublequote", False):
                parametros_leitura["doublequote"] = True
                # Quando doublequote=True
                parametros_leitura.pop("escapechar", None)
            elif config.get("escape_char"):
                parametros_leitura["escapechar"] = config["escape_char"]

            # Tratar compactação
            if config["compression"] == "gz":
                parametros_leitura["compression"] = "gzip"
            elif config["compression"] == "zip":
                # Para ZIP, precisa extrair primeiro
                import zipfile

                with zipfile.ZipFile(caminho_absoluto, "r") as zip_file:
                    nome_arquivo = zip_file.namelist()[0]
                    # Ler do ZIP em memória
                    with zip_file.open(nome_arquivo) as arquivo_zip:
                        # Criar arquivo temporário ou usar StringIO
                        import io

                        conteudo = arquivo_zip.read().decode(config["encoding"], errors="replace")
                        dataframe = pd.read_csv(io.StringIO(conteudo), **parametros_leitura)
                        if dataframe.empty:
                            logging.warning(f"Arquivo {caminho_absoluto} está vazio")
                            return None
                        return dataframe

            # Ler arquivo normalmente
            dataframe: pd.DataFrame = pd.read_csv(caminho_absoluto, **parametros_leitura)

            if dataframe.empty:
                logging.warning(f"Arquivo {caminho_absoluto} está vazio")
                return None

            # Validação: se tiver apenas 1 coluna, tentar delimitadores alternativos
            if len(dataframe.columns) == 1:
                logging.warning("Arquivo tem apenas 1 coluna, tentando delimitadores alternativos...")
                for delim_alt in self.detector.DELIMITADORES:
                    if delim_alt == config["delimiter"]:
                        continue
                    try:
                        parametros_alt = parametros_leitura.copy()
                        parametros_alt["sep"] = delim_alt
                        dataframe_alt = pd.read_csv(caminho_absoluto, **parametros_alt)
                        if len(dataframe_alt.columns) > 1:
                            logging.info(f"Delimitador alternativo funcionou: {repr(delim_alt)}")
                            dataframe = dataframe_alt
                            break
                    except Exception as erro:
                        logging.debug(f"Falha ao tentar delimitador {repr(delim_alt)}: {erro}")
                        continue

            return dataframe

        except UnicodeDecodeError as erro:
            # Tentar encodings alternativos se o detectado falhar
            logging.warning(f"Erro de encoding, tentando alternativas: {erro}")
            for encoding_alt in self.detector.ENCODINGS:
                if encoding_alt == config.get("encoding"):
                    continue
                try:
                    parametros_alt = parametros_leitura.copy()
                    parametros_alt["encoding"] = encoding_alt
                    dataframe_alt = pd.read_csv(caminho_absoluto, **parametros_alt)
                    if not dataframe_alt.empty:
                        logging.info(f"Encoding alternativo funcionou: {encoding_alt}")
                        return dataframe_alt
                except Exception as erro_alt:
                    logging.debug(f"Falha ao tentar encoding {encoding_alt}: {erro_alt}")
                    continue
            logging.error(f"Falha ao ler arquivo com todos os encodings: {caminho_absoluto}")
            return None

        except (FileNotFoundError, PermissionError) as erro:
            logging.error(f"Erro de acesso ao arquivo {caminho_absoluto}: {str(erro)}")
            return None

        except Exception as erro:
            logging.error(f"Erro inesperado ao ler arquivo {caminho_absoluto}: {str(erro)}")
            return None

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
            # Detectar configuração para usar no processamento em pedaços
            config_pedacos = self.detector.detectar_configuracao(caminho_csv_absoluto)
            self.salvadores.salvar_em_pedacos(
                caminho_csv_absoluto,
                caminho_absoluto,
                formato,
                self._limpar_memoria,
                config_pedacos,
            )
            return

        manipuladores: dict[str, Callable[[pd.DataFrame, str], None]] = {
            "parquet": self.salvadores.salvar_parquet,
            "feather": self.salvadores.salvar_feather,
            "h5": self.salvadores.salvar_h5,
            "json": self.salvadores.salvar_json,
            "pkl": self.salvadores.salvar_pkl,
            "orc": self.salvadores.salvar_orc,
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
