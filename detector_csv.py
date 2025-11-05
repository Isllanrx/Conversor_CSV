"""Módulo para detecção automática de características de arquivos CSV."""

import gzip
import logging
from pathlib import Path
from typing import Any
from zipfile import ZipFile

try:
    import chardet  # type: ignore[import]

    CHARDET_DISPONIVEL = True
except ImportError:
    CHARDET_DISPONIVEL = False


class DetectorCSV:
    """Classe responsável por detectar automaticamente características de arquivos CSV."""

    # Delimitadores comuns
    DELIMITADORES: tuple[str, ...] = (",", ";", "\t", "|")

    # Encodings comuns
    ENCODINGS: tuple[str, ...] = ("utf-8", "ISO-8859-1", "utf-16", "latin1", "cp1252")

    def __init__(self) -> None:
        """Inicializa o detector."""
        self.logger = logging.getLogger(__name__)

    def _detectar_compactacao(self, caminho: str) -> tuple[str, Any]:
        """Detecta se o arquivo está compactado e retorna o objeto de leitura"""
        caminho_obj = Path(caminho)

        # Verificar extensão
        if caminho_obj.suffix == ".gz":
            return ("gz", gzip.open(caminho, "rb"))
        elif caminho_obj.suffix == ".zip":
            return ("zip", ZipFile(caminho, "r"))
        else:
            return ("none", open(caminho, "rb"))

    def _detectar_encoding(self, caminho: str, amostra_bytes: int = 10000) -> str:
        """Detecta o encoding do arquivo."""
        try:
            # Ler amostra do arquivo
            with open(caminho, "rb") as arquivo:
                amostra = arquivo.read(amostra_bytes)

            # Usar chardet se disponível
            if CHARDET_DISPONIVEL:
                resultado = chardet.detect(amostra)
                encoding_detectado = resultado.get("encoding", "utf-8")
                confianca = resultado.get("confidence", 0)

                # Se confiança for baixa (<0.7)
                if confianca < 0.7:
                    try:
                        texto_iso = amostra.decode("ISO-8859-1", errors="strict")
                        if any(char in texto_iso for char in "áéíóúàèìòùâêîôûãõçÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇ"):
                            encoding_detectado = "ISO-8859-1"
                    except (UnicodeDecodeError, UnicodeError):
                        pass
            else:
                # Fallback: tentar detectar por BOM e análise de padrões
                if amostra.startswith(b"\xff\xfe"):
                    encoding_detectado = "utf-16"
                elif amostra.startswith(b"\xfe\xff"):
                    encoding_detectado = "utf-16-be"
                else:
                    # Tentar detectar analisando padrões de bytes
                    try:
                        texto_iso = amostra.decode("ISO-8859-1", errors="strict")
                        # Se decodifica sem erro e tem caracteres acentuados
                        if any(char in texto_iso for char in "áéíóúàèìòùâêîôûãõçÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇ"):
                            encoding_detectado = "ISO-8859-1"
                        else:
                            # Tentar UTF-8
                            try:
                                amostra.decode("utf-8", errors="strict")
                                encoding_detectado = "utf-8"
                            except (UnicodeDecodeError, UnicodeError):
                                # Se UTF-8 falha mas ISO-8859-1 funciona
                                encoding_detectado = "ISO-8859-1"
                    except (UnicodeDecodeError, UnicodeError):
                        # Se ISO-8859-1 falha, tentar UTF-8
                        try:
                            amostra.decode("utf-8", errors="strict")
                            encoding_detectado = "utf-8"
                        except (UnicodeDecodeError, UnicodeError):
                            # Fallback final: UTF-8
                            encoding_detectado = "utf-8"

            # Normalizar encoding
            encoding_map = {
                "iso-8859-1": "ISO-8859-1",
                "windows-1252": "cp1252",
                "utf-8": "utf-8",
                "utf-16": "utf-16",
                "utf-16-le": "utf-16",
                "utf-16-be": "utf-16",
            }

            encoding_normalizado = encoding_map.get(encoding_detectado.lower(), encoding_detectado)

            self.logger.debug(f"Encoding detectado: {encoding_normalizado}")

            return encoding_normalizado
        except Exception as erro:
            self.logger.warning(f"Erro ao detectar encoding, usando UTF-8: {erro}")
            return "utf-8"

    def _detectar_delimitador(self, caminho: str, encoding: str, amostra_linhas: int = 5) -> str:
        """Detecta o delimitador do CSV analisando as primeiras linhas."""
        try:
            # Ler primeiras linhas como texto
            linhas: list[str] = []
            with open(caminho, encoding=encoding, errors="ignore") as arquivo:
                for _ in range(amostra_linhas):
                    linha = arquivo.readline()
                    if not linha:
                        break
                    linhas.append(linha)

            if not linhas:
                return ","

            # Contar ocorrências de cada delimitador nas primeiras linhas
            contadores: dict[str, int] = dict.fromkeys(self.DELIMITADORES, 0)

            for linha in linhas:
                for delim in self.DELIMITADORES:
                    contadores[delim] += linha.count(delim)

            # Escolher delimitador com mais ocorrências
            delimitador_detectado = max(contadores.items(), key=lambda x: x[1])[0]

            # Se nenhum delimitador foi encontrado significativamente, manter padrao com virgula
            if contadores[delimitador_detectado] == 0:
                delimitador_detectado = ","

            self.logger.debug(f"Delimitador detectado: {repr(delimitador_detectado)}")

            return delimitador_detectado
        except Exception as erro:
            self.logger.warning(f"Erro ao detectar delimitador, usando vírgula: {erro}")
            return ","

    def _detectar_aspas(self, caminho: str, encoding: str) -> tuple[str | None, str | None, bool]:
        """Detecta o caractere de aspas usado."""
        try:
            with open(caminho, encoding=encoding, errors="ignore") as arquivo:
                primeira_linha = arquivo.readline()
                segunda_linha = arquivo.readline() if primeira_linha else ""

            # Analisar padrão de aspas
            aspas_duplas = primeira_linha.count('"')
            aspas_simples = primeira_linha.count("'")

            # Verificar se há aspas duplas
            if aspas_duplas > 0:
                # Verificar padrões de aspas
                # Padrão doublequote: "texto com ""aspas"" internas" ou "texto","outro"
                linhas_analisar = primeira_linha + segunda_linha

                # Contar aspas pares
                if linhas_analisar.count('"') >= 4:  # Pelo menos 2 campos com aspas
                    import re

                    padrao_csv_aspas = re.search(r'"[^"]*"[,\t;|]', linhas_analisar)
                    if padrao_csv_aspas:
                        return ('"', None, True)  # doublequote=True

                # Se tem "" dentro de campos, é doublequote
                if '""' in primeira_linha or '""' in segunda_linha:
                    return ('"', None, True)  # doublequote=True
                return ('"', None, True)  # Default para doublequote=True
            elif aspas_simples > 0:
                return ("'", None, False)  # aspas simples
            else:
                return (None, None, False)  # sem aspas
        except Exception:
            return (None, None, False)

    def _detectar_quebras_linha(self, caminho: str, encoding: str) -> str:
        """Detecta o tipo de quebra de linha."""
        try:
            with open(caminho, "rb") as arquivo:
                conteudo = arquivo.read(1000)

            # Verificar presença de CRLF
            if b"\r\n" in conteudo:
                return "\r\n"
            elif b"\n" in conteudo:
                return "\n"
            else:
                return "\n"  # padrão
        except Exception:
            return "\n"

    def detectar_configuracao(self, caminho: str) -> dict[str, Any]:
        """Detecta automaticamente todas as características do CSV."""
        caminho_absoluto = str(Path(caminho).absolute().resolve())

        if not Path(caminho_absoluto).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho_absoluto}")

        self.logger.info(f"Detectando características do CSV: {Path(caminho_absoluto).name}")

        # Detectar compactação
        tipo_compact, _ = self._detectar_compactacao(caminho_absoluto)

        # Detectar encoding
        encoding = self._detectar_encoding(caminho_absoluto)

        # Detectar delimitador
        if tipo_compact == "gz":
            # Para .gz, precisa descompactar temporariamente
            try:
                with gzip.open(caminho_absoluto, "rt", encoding=encoding, errors="ignore") as arquivo:
                    linhas = [arquivo.readline() for _ in range(5) if arquivo.readline()]
                    if linhas:
                        delimitador = max(
                            self.DELIMITADORES,
                            key=lambda d: sum(linha.count(d) for linha in linhas),
                        )
                    else:
                        delimitador = ","
            except Exception:
                delimitador = ","
        elif tipo_compact == "zip":
            # Para .zip, extrair primeiro arquivo
            try:
                with ZipFile(caminho_absoluto, "r") as zip_file:
                    if zip_file.namelist():
                        nome_arquivo = zip_file.namelist()[0]
                        with zip_file.open(nome_arquivo) as arquivo:
                            conteudo = arquivo.read(1000).decode(encoding, errors="ignore")
                            delimitador = max(
                                self.DELIMITADORES,
                                key=lambda d: conteudo.count(d),
                            )
                    else:
                        delimitador = ","
            except Exception:
                delimitador = ","
        else:
            delimitador = self._detectar_delimitador(caminho_absoluto, encoding)

        # Detectar aspas e escape
        quote_char, escape_char, doublequote = self._detectar_aspas(caminho_absoluto, encoding)

        # Detectar quebra de linha
        line_terminator = self._detectar_quebras_linha(caminho_absoluto, encoding)

        configuracao = {
            "encoding": encoding,
            "delimiter": delimitador,
            "quote_char": quote_char,
            "escape_char": escape_char,
            "doublequote": doublequote,
            "line_terminator": line_terminator,
            "compression": tipo_compact,
        }

        self.logger.info(f"Configuração detectada: {configuracao}")

        return configuracao
