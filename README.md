# Conversor CSV Inteligente

Uma aplicação desktop moderna para conversão de arquivos CSV em diversos formatos otimizados, desenvolvida com Python e CustomTkinter.

## 🚀 Funcionalidades

- Interface gráfica moderna e intuitiva
- Suporte para múltiplos formatos de saída:
  - Parquet (otimizado para Big Data)
  - Feather (leitura rápida)
  - ORC (compatível com Hive/Spark)
  - HDF5 (formato binário estruturado)
  - Pickle (serialização Python)
  - JSON (formato universal)
- Processamento em chunks para arquivos grandes
- Modo de baixo consumo de memória
- Suporte a múltiplas codificações (UTF-8, ISO-8859-1)
- Tooltips informativos para cada formato
- Processamento em thread separada para não travar a interface
- Barra de progresso para acompanhamento visual
- Sistema de tentativas múltiplas para leitura de arquivos
- Gerenciamento automático de memória
- Logs detalhados de operações e erros

## 📋 Pré-requisitos

- Python 3.x
- Bibliotecas necessárias:
  - customtkinter
  - pandas
  - pyarrow (para suporte ORC)
  - psutil
  - tqdm (para barras de progresso)

## 🛠️ Instalação

1. Clone este repositório
2. Instale as dependências:
```bash
pip install -r requirements.txt
```

## 💻 Uso

1. Execute o programa:
```bash
python main.py
```

2. Clique em "Selecionar Arquivos" para escolher os arquivos CSV
3. Selecione o formato desejado para conversão
4. Acompanhe o progresso através da barra de progresso
5. Os arquivos convertidos serão salvos na pasta "CSV_Convertidos"

## ⚙️ Configurações

O programa possui algumas configurações globais que podem ser ajustadas no código:

- `PASTA_CONVERTIDOS`: Pasta onde os arquivos convertidos serão salvos
- `CODIFICACOES`: Lista de codificações suportadas (UTF-8, ISO-8859-1)
- `TAMANHO_CHUNK`: Tamanho do chunk para processamento de arquivos grandes (100.000 linhas)
- `MAX_ARQUIVOS`: Limite máximo de arquivos para conversão (100)
- `MAX_TAMANHO_ARQUIVO`: Tamanho máximo total dos arquivos (2GB)
- `MAX_TENTATIVAS`: Número máximo de tentativas para leitura de arquivos (3)

## 🔒 Limitações

- Limite de 100 arquivos por conversão
- Tamanho máximo total de 2GB para os arquivos
- Modo de baixo consumo de memória ativado automaticamente quando a memória disponível é menor que 3GB

## 📝 Logs

O programa mantém logs detalhados das operações, incluindo:
- Conversões bem-sucedidas
- Erros durante o processo
- Ativação do modo de baixo consumo
- Tentativas de leitura de arquivos
- Erros de processamento de chunks
- Validações de dados

## 🔄 Processamento de Arquivos

- **Modo Normal**: Processamento direto para arquivos menores
- **Modo Chunks**: Processamento em partes para arquivos grandes
- **Gerenciamento de Memória**: Limpeza automática após processamento de chunks
- **Validação de Dados**: Verificação de DataFrames vazios ou inválidos
- **Tentativas Múltiplas**: Sistema de retry para leitura de arquivos

## 🤝 Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.

## 📄 Licença

Este projeto está sob a licença MIT.

## 👨‍💻 Desenvolvido por

Isllan Toso