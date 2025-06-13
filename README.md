# Conversor CSV Inteligente

Este projeto implementa uma interface gráfica para conversão de arquivos CSV para diversos formatos de dados, incluindo Parquet, Feather, ORC, HDF5, Pickle e JSON.

## Funcionalidades

- Conversão de arquivos CSV para múltiplos formatos.
- Suporte a processamento em chunks para arquivos grandes.
- Verificação de recursos (memória, tamanho dos arquivos).
- Logs detalhados para debug e informações de conversão.

## Como Executar

1. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```

2. Execute o programa:
   ```
   python main.py
   ```

## Dependências

- customtkinter
- pandas
- aiofiles
- psutil

## Logs

- `conversor.log`: Logs normais de conversão.
- `conversor_log_dev.log`: Logs detalhados para debug.