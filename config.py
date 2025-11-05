from pathlib import Path

# Constantes de configuração
DIRETORIO_SCRIPT: Path = Path(__file__).parent.absolute().resolve()
PASTA_CONVERTIDOS: str = str(DIRETORIO_SCRIPT.resolve() / 'CSV_Convertidos')
CODIFICACOES: tuple[str, ...] = ('utf-8', 'ISO-8859-1')
TAMANHO_PEDACO: int = 100_000
MAX_ARQUIVOS: int = 100
MAX_TAMANHO_ARQUIVO: int = 2 * 1024 * 1024 * 1024
MAX_TENTATIVAS: int = 3
LIMITE_RAM_BAIXO: int = 3 * 1024 * 1024 * 1024
FORMATOS_PEDACO: set[str] = {'parquet', 'h5', 'json'}

# Constantes de UI e processamento
TOOLTIP_OFFSET_X: int = 25
TOOLTIP_OFFSET_Y: int = 25
TOOLTIP_BACKGROUND: str = '#ffffe0'
ALTURA_LISTA: int = 100
LARGURA_BOTAO_CREDITOS: int = 150
LARGURA_JANELA: int = 800
ALTURA_JANELA: int = 600
