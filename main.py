"""
Conversor CSV Inteligente

Este módulo implementa uma interface gráfica para conversão de arquivos CSV
para diversos formatos de dados, incluindo Parquet, Feather, ORC, HDF5, Pickle e JSON.
"""

import customtkinter as ctk
import pandas as pd
import os
from tkinter import messagebox, Label, Toplevel, filedialog
import aiofiles
import logging
from typing import List, Callable, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import psutil
import asyncio

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('conversor.log', encoding='utf-8'),
        logging.FileHandler('conversor_log_dev.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class DebugFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.DEBUG

for handler in logging.getLogger().handlers:
    if isinstance(handler, logging.FileHandler) and handler.baseFilename.endswith('conversor_log_dev.log'):
        handler.addFilter(DebugFilter())
        handler.setLevel(logging.DEBUG)

# Constantes
PASTA_CONVERTIDOS = "CSV_Convertidos"
ENCODINGS = ['utf-8', 'ISO-8859-1']
MAX_ARQUIVOS = 100
MAX_TAMANHO_ARQUIVO = 2 * 1024 * 1024 * 1024
MIN_MEMORIA_LIVRE = 4 * 1024 * 1024 * 1024
CHUNK_SIZE = 100_000

@dataclass
class InfoArquivo:
    """Informações sobre um arquivo CSV."""
    nome: str
    tamanho: int
    linhas: int
    colunas: int
    memoria_estimada: int

@dataclass
class FormatoConversao:
    """Representa um formato de conversão disponível na aplicação."""
    nome: str
    funcao: Callable
    descricao: str
    extensao: str
    def __init__(self, nome: str, funcao: Callable, descricao: str, extensao: str) -> None:
        self.nome = nome
        self.funcao = funcao
        self.descricao = descricao
        self.extensao = extensao

class ToolTip:
    """Gerencia tooltips na interface gráfica."""
    def __init__(self, widget: ctk.CTkBaseClass, text: str) -> None:
        self.widget = widget
        self.text = text
        self.tooltip: Optional[Toplevel] = None
        self._configurar_eventos()

    def _configurar_eventos(self) -> None:
        """Configura os eventos de mouse para o tooltip."""
        self.widget.bind('<Enter>', self.enter)
        self.widget.bind('<Leave>', self.leave)

    def enter(self, event: Any = None) -> None:
        """Exibe o tooltip quando o mouse entra no widget."""
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 25

        self.tooltip = Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f"+{x}+{y}")

        label = Label(
            self.tooltip,
            text=self.text,
            justify='left',
            background="#ffffe0",
            relief='solid',
            borderwidth=1,
            font=("Arial", "10", "normal")
        )
        label.pack()

    def leave(self, event: Any = None) -> None:
        """Remove o tooltip quando o mouse sai do widget."""
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

class ConversorCSV:
    """Classe responsável pela conversão de arquivos CSV para outros formatos."""
    def __init__(self, pasta_saida: str = PASTA_CONVERTIDOS) -> None:
        self.pasta_saida = pasta_saida
        self._criar_pasta_saida()

    def _criar_pasta_saida(self) -> None:
        """Cria o diretório de saída se não existir."""
        if not os.path.exists(self.pasta_saida):
            os.makedirs(self.pasta_saida)

    def _verificar_recursos(self, arquivos: List[str]) -> Tuple[bool, str]:
        """
        Verifica se há recursos suficientes para processar os arquivos.
        
        Args:
            arquivos: Lista de arquivos a serem processados
            
        Returns:
            Tuple[bool, str]: (True se há recursos suficientes, mensagem de erro)
        """
        # Verificar quantidade de arquivos
        if len(arquivos) > MAX_ARQUIVOS:
            return False, f"Máximo de {MAX_ARQUIVOS} arquivos permitido"

        # Verificar tamanho total dos arquivos
        tamanho_total = sum(os.path.getsize(arquivo) for arquivo in arquivos)
        if tamanho_total > MAX_TAMANHO_ARQUIVO:
            return False, f"Tamanho total dos arquivos excede {MAX_TAMANHO_ARQUIVO / (1024*1024*1024):.1f}GB"

        # Verificar memória disponível
        memoria_livre = psutil.virtual_memory().available
        if memoria_livre < MIN_MEMORIA_LIVRE:
            return False, f"Memória insuficiente. Necessário {MIN_MEMORIA_LIVRE / (1024*1024*1024):.1f}GB livre"

        return True, ""

    async def analisar_arquivo(self, arquivo: str) -> Optional[InfoArquivo]:
        """
        Analisa um arquivo CSV e retorna informações sobre ele.
        
        Args:
            arquivo: Caminho do arquivo CSV
            
        Returns:
            InfoArquivo com informações do arquivo ou None em caso de erro
        """
        try:
            tamanho = os.path.getsize(arquivo)
            df = await self.ler_csv_async(arquivo)
            if df is not None:
                return InfoArquivo(
                    nome=Path(arquivo).name,
                    tamanho=tamanho,
                    linhas=len(df),
                    colunas=len(df.columns),
                    memoria_estimada=df.memory_usage(deep=True).sum()
                )
        except Exception as e:
            logging.error(f"Erro ao analisar arquivo {arquivo}: {str(e)}")
        return None

    async def ler_csv_async(self, caminho: str) -> Optional[pd.DataFrame]:
        """
        Lê um arquivo CSV de forma assíncrona.
        
        Args:
            caminho: Caminho do arquivo CSV
            
        Returns:
            DataFrame com os dados do CSV ou None em caso de erro
        """
        try:
            async with aiofiles.open(caminho, mode='rb') as f:
                conteudo = await f.read()
                for encoding in ENCODINGS:
                    try:
                        return pd.read_csv(
                            pd.io.common.BytesIO(conteudo),
                            low_memory=False,
                            encoding=encoding
                        )
                    except UnicodeDecodeError:
                        continue
                raise UnicodeDecodeError("encoding", b"", 0, 1, "Nenhum encoding suportado")
        except Exception as e:
            logging.error(f"Erro ao ler CSV {Path(caminho).name}: {str(e)}")
            return None

    def _imprimir_info_arquivo(self, info: InfoArquivo) -> None:
        """
        Imprime informações detalhadas sobre um arquivo.
        
        Args:
            info: Informações do arquivo
        """
        print("\n" + "="*50)
        print(f"Arquivo: {info.nome}")
        print(f"Tamanho: {info.tamanho / (1024*1024):.2f} MB")
        print(f"Linhas: {info.linhas:,}")
        print(f"Colunas: {info.colunas}")
        print(f"Memória estimada: {info.memoria_estimada / (1024*1024):.2f} MB")
        print("="*50 + "\n")

    async def converter_para_parquet(self, df: pd.DataFrame, arquivo: str, chunked: bool = True) -> None:
        """
        Converte DataFrame para formato Parquet, suportando escrita em chunks.
        """
        if not chunked or len(df) <= CHUNK_SIZE:
            df.to_parquet(arquivo, compression="snappy", index=False)
        else:
            # Leitura e escrita em chunks
            first_chunk = True
            for chunk in pd.read_csv(df.attrs['csv_path'], chunksize=CHUNK_SIZE, low_memory=False):
                if first_chunk:
                    chunk.to_parquet(arquivo, compression="snappy", index=False)
                    first_chunk = False
                else:
                    arquivo_chunk = arquivo.replace('.parquet', f'_part{int(chunk.index[0])}.parquet')
                    chunk.to_parquet(arquivo_chunk, compression="snappy", index=False)

    async def converter_para_feather(self, df: pd.DataFrame, arquivo: str, chunked: bool = True) -> None:
        if len(df) > CHUNK_SIZE:
            print(f"[AVISO] Feather não suporta escrita incremental. O arquivo será processado inteiro na memória.")
        df.to_feather(arquivo)

    async def converter_para_orc(self, df: pd.DataFrame, arquivo: str, chunked: bool = True) -> None:
        if len(df) > CHUNK_SIZE:
            print(f"[AVISO] ORC não suporta escrita incremental. O arquivo será processado inteiro na memória.")
        import pyarrow as pa
        import pyarrow.orc as orc
        table = pa.Table.from_pandas(df)
        with open(arquivo, 'wb') as f:
            orc.write_table(table, f)

    async def converter_para_hdf(self, df: pd.DataFrame, arquivo: str, chunked: bool = True) -> None:
        """
        Converte DataFrame para formato HDF5, suportando escrita em chunks.
        """
        if not chunked or len(df) <= CHUNK_SIZE:
            df.to_hdf(
                arquivo,
                key='data',
                mode='w',
                format='table',
                complevel=9,
                complib='blosc'
            )
        else:
            store = pd.HDFStore(arquivo, mode='w', complevel=9, complib='blosc')
            for chunk in pd.read_csv(df.attrs['csv_path'], chunksize=CHUNK_SIZE, low_memory=False):
                store.append('data', chunk, format='table', data_columns=True)
            store.close()

    async def converter_para_pickle(self, df: pd.DataFrame, arquivo: str, chunked: bool = True) -> None:
        if len(df) > CHUNK_SIZE:
            print(f"[AVISO] Pickle não suporta escrita incremental. O arquivo será processado inteiro na memória.")
        with open(arquivo, 'wb') as f:
            df.to_pickle(f, protocol=4)

    async def converter_para_json(self, df: pd.DataFrame, arquivo: str, chunked: bool = True) -> None:
        """
        Converte DataFrame para formato JSON, suportando escrita em chunks.
        """
        if not chunked or len(df) <= CHUNK_SIZE:
            df.to_json(arquivo, orient='records', lines=True)
        else:
            with open(arquivo, 'w', encoding='utf-8') as f:
                for chunk in pd.read_csv(df.attrs['csv_path'], chunksize=CHUNK_SIZE, low_memory=False):
                    chunk.to_json(f, orient='records', lines=True)

class CSVConverterApp(ctk.CTk):
    """Aplicação principal para conversão de arquivos CSV."""
    def __init__(self) -> None:
        super().__init__()
        self._configurar_janela()
        self._inicializar_atributos()
        self._configurar_formatos()
        self.criar_widgets()
        logging.info("Aplicação iniciada com sucesso")

    def _configurar_janela(self) -> None:
        """Configura as propriedades da janela principal."""
        self.title("Conversor CSV Inteligente")
        self.geometry("800x600")
        self.resizable(True, True)

    def _inicializar_atributos(self) -> None:
        """Inicializa os atributos da aplicação."""
        self.arquivos_selecionados: List[str] = []
        self.conversor = ConversorCSV()

    def _configurar_formatos(self) -> None:
        """Configura os formatos de conversão disponíveis."""
        self.formatos_disponiveis: List[FormatoConversao] = [
            FormatoConversao(
                "Parquet",
                self.conversor.converter_para_parquet,
                "Formato compacto, ideal para Big Data e análise com Spark.",
                "parquet"
            ),
            FormatoConversao(
                "Feather",
                self.conversor.converter_para_feather,
                "Formato rápido para leitura/escrita em memória.",
                "feather"
            ),
            FormatoConversao(
                "ORC",
                self.conversor.converter_para_orc,
                "Similar ao Parquet, eficiente para leitura em Hive/Spark.",
                "orc"
            ),
            FormatoConversao(
                "HDF5",
                self.conversor.converter_para_hdf,
                "Bom para grandes quantidades de dados estruturados.",
                "h5"
            ),
            FormatoConversao(
                "Pickle",
                self.conversor.converter_para_pickle,
                "Serializa DataFrames para uso exclusivo em Python.",
                "pkl"
            ),
            FormatoConversao(
                "JSON",
                self.conversor.converter_para_json,
                "Formato universal para troca de dados entre sistemas.",
                "json"
            ),
        ]

    def criar_widgets(self) -> None:
        """Cria e configura todos os widgets da interface gráfica."""
        self._criar_frame_principal()
        self._criar_frame_arquivos()
        self._criar_frame_lista()
        self._criar_frame_formatos()
        self._criar_frame_status()
        self._criar_frame_creditos()

    def _criar_frame_principal(self) -> None:
        """Cria o frame principal da aplicação."""
        self.main_frame = ctk.CTkFrame(self)
        self.main_frame.pack(fill="both", expand=True, padx=20, pady=20)

    def _criar_frame_arquivos(self) -> None:
        """Cria o frame para seleção de arquivos."""
        frame_arquivos = ctk.CTkFrame(self.main_frame)
        frame_arquivos.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(
            frame_arquivos,
            text="Arquivos CSV:",
            font=("Arial", 16)
        ).pack(side="left", padx=5)
        
        btn_selecionar = ctk.CTkButton(
            frame_arquivos,
            text="Selecionar Arquivos",
            command=self.selecionar_arquivos
        )
        btn_selecionar.pack(side="left", padx=5)

    def _criar_frame_lista(self) -> None:
        """Cria o frame para exibição da lista de arquivos."""
        frame_lista = ctk.CTkFrame(self.main_frame)
        frame_lista.pack(fill="both", expand=True, pady=(0, 10))
        
        self.lista_arquivos = ctk.CTkTextbox(frame_lista, height=100)
        self.lista_arquivos.pack(fill="both", expand=True, padx=5, pady=5)

    def _criar_frame_formatos(self) -> None:
        """Cria o frame para seleção de formatos de conversão."""
        frame_formatos = ctk.CTkFrame(self.main_frame)
        frame_formatos.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(
            frame_formatos,
            text="Escolha o formato de conversão:",
            font=("Arial", 14)
        ).pack(pady=5)

        frame_botoes = ctk.CTkScrollableFrame(frame_formatos)
        frame_botoes.pack(fill="both", expand=True, padx=5, pady=5)

        for formato in self.formatos_disponiveis:
            self._criar_botao_conversao(frame_botoes, formato)

    def _criar_botao_conversao(self, frame: ctk.CTkFrame, formato: FormatoConversao) -> None:
        """
        Cria um botão de conversão para um formato específico.
        
        Args:
            frame: Frame onde o botão será criado
            formato: Formato de conversão associado ao botão
        """
        frame_botao = ctk.CTkFrame(frame)
        frame_botao.pack(fill="x", pady=2)
        
        botao = ctk.CTkButton(
            frame_botao,
            text=f"Converter para {formato.nome}",
            command=lambda f=formato.funcao: self.iniciar_conversao(f)
        )
        botao.pack(side="left", padx=5, pady=2)
        
        label = ctk.CTkLabel(frame_botao, text="ℹ️", font=("Arial", 12))
        label.pack(side="left", padx=5)
        
        ToolTip(label, formato.descricao)

    def _criar_frame_status(self) -> None:
        """Cria o frame para exibição de status e progresso."""
        frame_status = ctk.CTkFrame(self.main_frame)
        frame_status.pack(fill="x", pady=(0, 10))

        self.progresso = ctk.CTkProgressBar(frame_status)
        self.progresso.pack(fill="x", padx=5, pady=5)
        self.progresso.set(0)

        self.status_label = ctk.CTkLabel(
            frame_status,
            text="",
            text_color="lightgreen",
            wraplength=700
        )
        self.status_label.pack(fill="x", padx=5, pady=5)

    def _criar_frame_creditos(self) -> None:
        """Cria o frame de créditos."""
        frame_creditos = ctk.CTkFrame(
            self.main_frame,
            fg_color="#2b2b2b",
            corner_radius=10
        )
        frame_creditos.pack(fill="x", pady=(0, 5))
        
        creditos = ctk.CTkLabel(
            frame_creditos,
            text="Desenvolvido por Isllan Toso / VIXPar / Globalsys",
            font=("Arial", 11, "bold"),
            text_color="#1f538d",
            pady=10
        )
        creditos.pack(pady=5)

    def selecionar_arquivos(self) -> None:
        """Abre diálogo para seleção de arquivos CSV."""
        try:
            arquivos = filedialog.askopenfilenames(
                title="Selecione os arquivos CSV",
                filetypes=[("Arquivos CSV", "*.csv"), ("Todos os arquivos", "*.*")]
            )
            if arquivos:
                # Verificar recursos
                ok, mensagem = self.conversor._verificar_recursos(list(arquivos))
                if not ok:
                    messagebox.showerror("Erro", mensagem)
                    return

                self.arquivos_selecionados = list(arquivos)
                self.atualizar_lista_arquivos()
                logging.info(f"Arquivos selecionados: {len(arquivos)}")
                
                # Analisar arquivos
                print("\n=== ANÁLISE DOS ARQUIVOS SELECIONADOS ===")
                for arquivo in arquivos:
                    info = asyncio.run(self.conversor.analisar_arquivo(arquivo))
                    if info:
                        self.conversor._imprimir_info_arquivo(info)
                print("="*50 + "\n")
        except Exception as e:
            logging.error(f"Erro ao selecionar arquivos: {str(e)}")
            messagebox.showerror("Erro", "Erro ao selecionar arquivos")

    def atualizar_lista_arquivos(self) -> None:
        """Atualiza a lista visual de arquivos selecionados."""
        self.lista_arquivos.delete("1.0", "end")
        for arquivo in self.arquivos_selecionados:
            self.lista_arquivos.insert("end", f"{Path(arquivo).name}\n")

    def iniciar_conversao(self, funcao_conversao: Callable) -> None:
        """
        Inicia o processo de conversão dos arquivos selecionados.
        
        Args:
            funcao_conversao: Função que será usada para converter os arquivos
        """
        if not self.arquivos_selecionados:
            messagebox.showwarning("Aviso", "Selecione pelo menos um arquivo CSV!")
            return

        self.progresso.set(0)
        self.status_label.configure(text="🔄 Iniciando conversão...", text_color="white")
        
        async def processar_arquivos():
            total_arquivos = len(self.arquivos_selecionados)
            for i, arquivo in enumerate(self.arquivos_selecionados):
                try:
                    await self.converter_async(arquivo, funcao_conversao.__name__.split('_')[-1], funcao_conversao)
                    progresso = (i + 1) / total_arquivos
                    self.progresso.set(progresso)
                except Exception as e:
                    logging.error(f"Erro ao converter arquivo {arquivo}: {str(e)}")
                    self.status_label.configure(
                        text=f"❌ Erro ao converter {Path(arquivo).name}: {str(e)}",
                        text_color="red"
                    )
            
            self.progresso.set(1.0)
            self.status_label.configure(text="✅ Conversão concluída!", text_color="lightgreen")

        asyncio.run(processar_arquivos())

    async def converter_async(self, arquivo: str, extensao: str, funcao_conversao: Callable) -> None:
        """
        Realiza a conversão do arquivo de forma assíncrona.
        
        Args:
            arquivo: Caminho do arquivo a ser convertido
            extensao: Extensão do arquivo de saída
            funcao_conversao: Função que realiza a conversão
        """
        try:
            info = await self.conversor.analisar_arquivo(arquivo)
            if info:
                print(f"\n=== CONVERTENDO: {info.nome} ===")
                print(f"Tamanho original: {info.tamanho / (1024*1024):.2f} MB")
                print(f"Linhas: {info.linhas:,}")
                print(f"Colunas: {info.colunas}")
                print(f"Memória estimada: {info.memoria_estimada / (1024*1024):.2f} MB")

            df = await self.conversor.ler_csv_async(arquivo)
            if df is not None:
                df.attrs['csv_path'] = arquivo
                nome_arquivo = Path(arquivo).stem
                nome_saida = os.path.join(self.conversor.pasta_saida, f"{nome_arquivo}.{extensao}")

                if not self.verificar_sobrescrita(nome_saida):
                    return

                try:
                    inicio = datetime.now()
                    await funcao_conversao(df, nome_saida, True)
                    fim = datetime.now()
                    tempo = (fim - inicio).total_seconds()
                    
                    if not os.path.exists(nome_saida):
                        raise Exception(f"Arquivo não foi criado: {nome_saida}")

                    tamanho_saida = os.path.getsize(nome_saida)
                    print(f"\n=== ARQUIVO CONVERTIDO: {Path(nome_saida).name} ===")
                    print(f"Tamanho do arquivo convertido: {tamanho_saida / (1024*1024):.2f} MB")
                    print(f"Tempo de conversão: {tempo:.2f} segundos")
                    print("="*50 + "\n")

                    self.status_label.configure(
                        text=f"✅ Convertido: {Path(nome_saida).name}",
                        text_color="lightgreen"
                    )
                    logging.info(f"Arquivo convertido com sucesso: {Path(nome_saida).name}")
                except Exception as e:
                    erro_msg = f"Erro ao converter para {extensao.upper()}: {str(e)}"
                    logging.error(erro_msg)
                    self.status_label.configure(text=f"❌ {erro_msg}", text_color="red")
                    raise
        except Exception as e:
            erro_msg = f"Falha ao processar {Path(arquivo).name}: {str(e)}"
            logging.error(erro_msg)
            self.status_label.configure(text=f"❌ {erro_msg}", text_color="red")
            raise

    def verificar_sobrescrita(self, destino: str) -> bool:
        """
        Verifica se o arquivo de destino já existe e pergunta se deve sobrescrever.
        
        Args:
            destino: Caminho do arquivo de destino
            
        Returns:
            True se deve sobrescrever, False caso contrário
        """
        if os.path.exists(destino):
            return messagebox.askyesno(
                "Sobrescrever?",
                f"O arquivo '{Path(destino).name}' já existe. Deseja sobrescrever?"
            )
        return True

if __name__ == "__main__":
    app = CSVConverterApp()
    app.mainloop()
