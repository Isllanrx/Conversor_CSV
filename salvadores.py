import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd  # type: ignore[import]

from .config import TAMANHO_PEDACO
from .utils import garantir_caminho_absoluto

try:
    import pyarrow as pa  # type: ignore[import]
    import pyarrow.orc as orc  # type: ignore[import]
except ImportError:
    pa = None
    orc = None


class Salvadores:
    """Classe responsável por salvar DataFrames em diferentes formatos."""

    def __init__(self, baixo_consumo: bool) -> None:
        """Inicializa os salvadores"""
        self.baixo_consumo = baixo_consumo

    def salvar_pedaco_parquet(self, pedaco: pd.DataFrame, caminho_saida: str, numero_pedaco: int, total_pedacos: int) -> None:
        """Salva um pedaço de dados em formato Parquet."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho_saida)
        objeto_caminho: Path = Path(caminho_absoluto).resolve()

        arquivo_final_str: str = str(objeto_caminho.resolve()) if total_pedacos == 1 else str(objeto_caminho.resolve().parent / f"{objeto_caminho.stem}_pedaco{numero_pedaco}{objeto_caminho.suffix}")

        arquivo_final: str = garantir_caminho_absoluto(arquivo_final_str)
        Path(arquivo_final).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            pedaco.to_parquet(arquivo_final, index=False, compression="snappy")
        except OSError as erro:
            logging.error(f"Erro ao salvar pedaço Parquet {numero_pedaco}: {str(erro)}")
            raise

    def salvar_pedaco_h5(self, pedaco: pd.DataFrame, caminho_saida: str) -> None:
        """Salva um pedaço de dados em formato HDF5."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho_saida)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            with pd.HDFStore(caminho_absoluto, mode="a", complevel=9, complib="blosc") as armazenamento:
                armazenamento.append("dados", pedaco, format="table", data_columns=True)
        except OSError as erro:
            logging.error(f"Erro ao salvar pedaço HDF5: {str(erro)}")
            raise

    def salvar_pedaco_json(self, pedaco: pd.DataFrame, caminho_saida: str) -> None:
        """Salva um pedaço de dados em formato JSON."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho_saida)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(caminho_absoluto, "a", encoding="utf-8") as arquivo_aberto:
                pedaco.to_json(arquivo_aberto, orient="records", lines=True, force_ascii=False)
        except OSError as erro:
            logging.error(f"Erro ao salvar pedaço JSON: {str(erro)}")
            raise

    def processar_pedaco(
        self,
        pedaco: pd.DataFrame,
        caminho_saida: str,
        formato: str,
        numero_pedaco: int,
        total_pedacos: int,
    ) -> None:
        """Processa e salva um pedaço de dados no formato especificado."""
        if formato == "parquet":
            self.salvar_pedaco_parquet(pedaco, caminho_saida, numero_pedaco, total_pedacos)
        elif formato == "h5":
            self.salvar_pedaco_h5(pedaco, caminho_saida)
        elif formato == "json":
            self.salvar_pedaco_json(pedaco, caminho_saida)
        else:
            raise ValueError(f"Formato {formato} não suporta processamento em pedaços")

    def salvar_parquet(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato Parquet."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_parquet(caminho_absoluto, index=False, compression="snappy")
        except OSError as erro:
            logging.error(f"Erro ao salvar Parquet {caminho}: {str(erro)}")
            raise

    def salvar_feather(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato Feather."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_feather(caminho_absoluto)
        except OSError as erro:
            logging.error(f"Erro ao salvar Feather {caminho}: {str(erro)}")
            raise

    def salvar_h5(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato HDF5."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
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

    def salvar_json(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato JSON."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_json(caminho_absoluto, orient="records", lines=True, force_ascii=False)
        except OSError as erro:
            logging.error(f"Erro ao salvar JSON {caminho}: {str(erro)}")
            raise

    def salvar_pkl(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato Pickle."""
        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            dataframe.to_pickle(caminho_absoluto)
        except OSError as erro:
            logging.error(f"Erro ao salvar Pickle {caminho}: {str(erro)}")
            raise

    def salvar_orc(self, dataframe: pd.DataFrame, caminho: str) -> None:
        """Salva um DataFrame em formato ORC."""
        if pa is None or orc is None:
            raise ImportError("pyarrow é necessário para salvar em formato ORC")

        caminho_absoluto: str = garantir_caminho_absoluto(caminho)
        Path(caminho_absoluto).resolve().parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(caminho_absoluto, "wb") as arquivo_aberto:
                orc.write_table(pa.Table.from_pandas(dataframe), arquivo_aberto)
        except OSError as erro:
            logging.error(f"Erro ao salvar ORC {caminho_absoluto}: {str(erro)}")
            raise

    def salvar_em_pedacos(
        self,
        caminho_csv: str,
        caminho_saida: str,
        formato: str,
        limpar_memoria: Callable,
        config_csv: dict[str, Any] | None = None,
    ) -> None:
        """Salva um arquivo CSV grande processando em chunks"""
        caminho_csv_absoluto: str = garantir_caminho_absoluto(caminho_csv)
        caminho_saida_absoluto: str = garantir_caminho_absoluto(caminho_saida)

        if not Path(caminho_csv_absoluto).resolve().exists():
            raise FileNotFoundError(f"Arquivo CSV não encontrado: {caminho_csv_absoluto}")

        # Preparar parâmetros de leitura com configuração detectada
        parametros_leitura: dict[str, Any] = {
            "chunksize": TAMANHO_PEDACO,
            "low_memory": False,
        }

        if config_csv:
            parametros_leitura["encoding"] = config_csv.get("encoding", "utf-8")
            parametros_leitura["sep"] = config_csv.get("delimiter", ",")
            if config_csv.get("quote_char"):
                parametros_leitura["quotechar"] = config_csv["quote_char"]

            # Configurar doublequote se detectado
            if config_csv.get("doublequote", False):
                parametros_leitura["doublequote"] = True
                parametros_leitura.pop("escapechar", None)
            elif config_csv.get("escape_char"):
                parametros_leitura["escapechar"] = config_csv["escape_char"]

        # processar pedaços sem carregar tudo na memória
        pedacos_iterador = pd.read_csv(caminho_csv_absoluto, **parametros_leitura)

        # Primeira passagem: contar pedaços e preparar lista
        lista_pedacos: list[pd.DataFrame] = []
        for pedaco in pedacos_iterador:
            lista_pedacos.append(pedaco)

        total_pedacos: int = len(lista_pedacos)

        if total_pedacos == 0:
            logging.warning(f"Arquivo CSV {caminho_csv_absoluto} está vazio ou não pôde ser lido")
            return

        from tqdm import tqdm  # type: ignore[import]

        with tqdm(total=total_pedacos, desc="Processando pedaços") as barra_progresso:
            for numero_pedaco, pedaco in enumerate(lista_pedacos, start=1):
                try:
                    self.processar_pedaco(pedaco, caminho_saida_absoluto, formato, numero_pedaco, total_pedacos)
                    barra_progresso.update(1)
                    limpar_memoria()
                except (OSError, ValueError) as erro:
                    logging.error(f"Erro ao processar pedaço {numero_pedaco}/{total_pedacos}: {str(erro)}")
                    raise
                except Exception as erro:
                    logging.error(f"Erro inesperado ao processar pedaço {numero_pedaco}: {str(erro)}")
                    raise
