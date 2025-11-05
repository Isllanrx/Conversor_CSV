# Conversor CSV Modular

Conversor CSV com estrutura modular para fácil manutenção e extensão.

## Como Usar

### Interface Gráfica
```bash
python -m conversor_csv_modular.main
```

### Como Módulo Python
```python
from conversor_csv_modular import ConversorCSV

conversor = ConversorCSV()
dataframe = conversor.ler_csv('arquivo.csv')
conversor.salvar(dataframe, 'saida.parquet', 'parquet')
```

## Estrutura

| Módulo | Descrição |
|--------|-----------|
| `config.py` | Configurações e constantes |
| `detector_csv.py` | Detecção automática (encoding, delimitador, aspas) |
| `conversor.py` | Lógica principal de conversão |
| `salvadores.py` | Salvamento em diferentes formatos |
| `interface.py` | Interface gráfica (CustomTkinter) |
| `tipos.py` | Estruturas de dados |
| `widgets.py` | Widgets customizados |
| `utils.py` | Funções utilitárias |

## Formatos Suportados

- **Parquet** - Compactado para Big Data
- **Feather** - Leitura rápida
- **ORC** - Compatível Hive/Spark
- **HDF5** - Formato binário estruturado
- **JSON** - Formato universal
- **Pickle** - Serialização Python

## Funcionalidades

- Detecção automática de delimitadores (`,`, `;`, `\t`, `|`)
- Detecção automática de encoding (UTF-8, ISO-8859-1, UTF-16)
- Detecção automática de aspas e escape
- Suporte a arquivos compactados (.gz, .zip)
- Processamento em pedaços para arquivos grandes
- Interface gráfica intuitiva

## Requisitos

Veja `requirements.txt` na pasta raiz do projeto.
