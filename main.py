import customtkinter as ctk
import pandas as pd
import os
from tkinter import messagebox, Label, Toplevel, filedialog
from concurrent.futures import ThreadPoolExecutor
from typing import List, Callable, Optional
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import psutil
import threading
import logging
import gc
from tqdm import tqdm

# --- Configurações Globais ---
PASTA_CONVERTIDOS = "CSV_Convertidos"
CODIFICACOES = ['utf-8', 'ISO-8859-1']
TAMANHO_CHUNK = 100_000
MAX_ARQUIVOS = 100
MAX_TAMANHO_ARQUIVO = 2 * 1024 * 1024 * 1024
MAX_TENTATIVAS = 3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Dataclasses ---
@dataclass
class FormatoConversao:
    nome: str
    funcao: Callable
    descricao: str
    extensao: str

# --- Tooltip ---
class DicaTooltip:
    def __init__(self, widget, texto):
        self.widget = widget
        self.texto = texto
        self.tooltip = None
        widget.bind('<Enter>', self.entrar)
        widget.bind('<Leave>', self.sair)

    def entrar(self, evento=None):
        x, y, _, _ = self.widget.bbox("insert")
        x += self.widget.winfo_rootx() + 25
        y += self.widget.winfo_rooty() + 25
        self.tooltip = Toplevel(self.widget)
        self.tooltip.wm_overrideredirect(True)
        self.tooltip.wm_geometry(f"+{x}+{y}")
        label = Label(self.tooltip, text=self.texto, background="#ffffe0", relief='solid', borderwidth=1)
        label.pack()

    def sair(self, evento=None):
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

# --- Conversor Principal ---
class ConversorCSV:
    def __init__(self):
        if not os.path.exists(PASTA_CONVERTIDOS):
            os.makedirs(PASTA_CONVERTIDOS)
        self.baixo_consumo = psutil.virtual_memory().available < (3 * 1024 * 1024 * 1024)
        self._limpar_memoria()

    def _limpar_memoria(self):
        gc.collect()

    def verificar_recursos(self, arquivos: List[str]) -> Optional[str]:
        if len(arquivos) > MAX_ARQUIVOS:
            return f"Limite de {MAX_ARQUIVOS} arquivos excedido."
        total = sum(os.path.getsize(arq) for arq in arquivos)
        if total > MAX_TAMANHO_ARQUIVO:
            return "Arquivos muito grandes."
        return None

    def ler_csv(self, caminho: str) -> Optional[pd.DataFrame]:
        for tentativa in range(MAX_TENTATIVAS):
            for cod in CODIFICACOES:
                try:
                    df = pd.read_csv(caminho, low_memory=False, encoding=cod)
                    if df.empty:
                        logging.warning(f"Arquivo {caminho} está vazio")
                        return None
                    return df
                except Exception as e:
                    logging.error(f"Erro ao ler arquivo {caminho} com codificação {cod}: {str(e)}")
                    continue
            if tentativa < MAX_TENTATIVAS - 1:
                logging.info(f"Tentativa {tentativa + 1} falhou, tentando novamente...")
        return None

    def salvar_em_chunks(self, caminho_csv, caminho_saida, formato):
        total_chunks = sum(1 for _ in pd.read_csv(caminho_csv, chunksize=TAMANHO_CHUNK))
        with tqdm(total=total_chunks, desc="Processando chunks") as pbar:
            for chunk in pd.read_csv(caminho_csv, chunksize=TAMANHO_CHUNK):
                try:
                    if formato == "parquet":
                        nome = caminho_saida.replace('.parquet', f'_chunk{chunk.index[0]}.parquet')
                        chunk.to_parquet(nome, index=False, compression="snappy")
                    elif formato == "h5":
                        with pd.HDFStore(caminho_saida, mode='a', complevel=9, complib='blosc') as store:
                            store.append('dados', chunk, format='table', data_columns=True)
                    elif formato == "json":
                        with open(caminho_saida, 'a', encoding='utf-8') as f:
                            chunk.to_json(f, orient='records', lines=True)
                    pbar.update(1)
                    self._limpar_memoria()
                except Exception as e:
                    logging.error(f"Erro ao processar chunk: {str(e)}")
                    raise

    def salvar(self, df: pd.DataFrame, caminho: str, formato: str, caminho_csv: str = None):
        if df is None or df.empty:
            raise ValueError("DataFrame vazio ou inválido")

        if self.baixo_consumo and formato in ['parquet', 'h5', 'json'] and caminho_csv:
            self.salvar_em_chunks(caminho_csv, caminho, formato)
            return

        try:
            if formato == "parquet":
                df.to_parquet(caminho, index=False, compression="snappy")
            elif formato == "feather":
                df.to_feather(caminho)
            elif formato == "h5":
                df.to_hdf(caminho, key='dados', mode='w', format='table', complevel=9, complib='blosc')
            elif formato == "json":
                df.to_json(caminho, orient='records', lines=True)
            elif formato == "pkl":
                df.to_pickle(caminho)
            elif formato == "orc":
                import pyarrow as pa, pyarrow.orc as orc
                with open(caminho, 'wb') as f:
                    orc.write_table(pa.Table.from_pandas(df), f)
        except Exception as e:
            logging.error(f"Erro ao salvar arquivo {caminho}: {str(e)}")
            raise

# --- Aplicação CustomTkinter ---
class AplicacaoConversor(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Conversor CSV Inteligente")
        self.geometry("800x600")
        self.arquivos = []
        self.conversor = ConversorCSV()
        self.formatos = [
            FormatoConversao("Parquet", lambda df, p, c: self.conversor.salvar(df, p, "parquet", c), "Compactado para Big Data", "parquet"),
            FormatoConversao("Feather", lambda df, p, c: self.conversor.salvar(df, p, "feather"), "Leitura rápida", "feather"),
            FormatoConversao("ORC", lambda df, p, c: self.conversor.salvar(df, p, "orc"), "Compatível Hive/Spark", "orc"),
            FormatoConversao("HDF5", lambda df, p, c: self.conversor.salvar(df, p, "h5", c), "Formato binário estruturado", "h5"),
            FormatoConversao("Pickle", lambda df, p, c: self.conversor.salvar(df, p, "pkl"), "Serialização Python", "pkl"),
            FormatoConversao("JSON", lambda df, p, c: self.conversor.salvar(df, p, "json", c), "Formato universal", "json"),
        ]
        self.progresso = None
        self._criar_widgets()

    def _criar_widgets(self):
        # Frame principal para organizar os elementos
        frame_principal = ctk.CTkFrame(self)
        frame_principal.pack(fill="both", expand=True, padx=10, pady=10)

        # Frame superior para o botão de seleção e lista
        frame_superior = ctk.CTkFrame(frame_principal)
        frame_superior.pack(fill="both", expand=True, pady=(0, 10))

        botao = ctk.CTkButton(frame_superior, text="Selecionar Arquivos", command=self.selecionar_arquivos)
        botao.pack(pady=10)
        self.lista = ctk.CTkTextbox(frame_superior, height=100)
        self.lista.pack(pady=5, fill="both", expand=True)

        # Frame para os botões de conversão
        frame_conversao = ctk.CTkFrame(frame_principal)
        frame_conversao.pack(fill="x", pady=(0, 10))

        for formato in self.formatos:
            frame = ctk.CTkFrame(frame_conversao)
            frame.pack(pady=2, padx=5, fill="x")
            botao = ctk.CTkButton(frame, text=f"Converter para {formato.nome}", command=lambda f=formato: self.converter_em_thread(f))
            botao.pack(side="left")
            label = ctk.CTkLabel(frame, text="ℹ️")
            label.pack(side="left", padx=5)
            DicaTooltip(label, formato.descricao)

        # Frame inferior para status e progresso
        frame_inferior = ctk.CTkFrame(frame_principal)
        frame_inferior.pack(fill="x", pady=(0, 10))

        self.status = ctk.CTkLabel(frame_inferior, text="")
        self.status.pack(pady=10)

        self.progresso = ctk.CTkProgressBar(frame_inferior)
        self.progresso.pack(pady=5, fill="x", padx=10)
        self.progresso.set(0)

        # Frame para o botão de créditos
        frame_creditos = ctk.CTkFrame(frame_principal)
        frame_creditos.pack(fill="x", side="bottom", pady=(10, 0))
        
        # Botão de créditos alinhado à direita
        botao_creditos = ctk.CTkButton(frame_creditos, text="Desenvolvido por Isllan Toso", width=150)
        botao_creditos.pack(side="right", padx=10, pady=5)

    def selecionar_arquivos(self):
        arquivos = filedialog.askopenfilenames(filetypes=[("CSV", "*.csv")])
        if arquivos:
            erro = self.conversor.verificar_recursos(list(arquivos))
            if erro:
                messagebox.showerror("Erro", erro)
                return
            self.arquivos = list(arquivos)
            self.lista.delete("1.0", "end")
            for a in self.arquivos:
                self.lista.insert("end", f"{Path(a).name}\n")
            if self.conversor.baixo_consumo:
                self.status.configure(text="⚠️ Modo de Baixo Consumo de RAM ativado.")

    def converter_em_thread(self, formato: FormatoConversao):
        if not self.arquivos:
            messagebox.showwarning("Aviso", "Por favor, selecione arquivos primeiro.")
            return
        threading.Thread(target=self.converter, args=(formato,), daemon=True).start()

    def converter(self, formato: FormatoConversao):
        try:
            self.status.configure(text=f"Convertendo para {formato.nome}...")
            self.progresso.set(0)
            total_arquivos = len(self.arquivos)
            
            for i, arquivo in enumerate(self.arquivos):
                try:
                    df = self.conversor.ler_csv(arquivo)
                    if df is not None:
                        saida = os.path.join(PASTA_CONVERTIDOS, f"{Path(arquivo).stem}.{formato.extensao}")
                        formato.funcao(df, saida, arquivo)
                        logging.info(f"Convertido: {saida}")
                    self.progresso.set((i + 1) / total_arquivos)
                except Exception as e:
                    logging.error(f"Erro ao converter {arquivo}: {str(e)}")
                    messagebox.showerror("Erro", f"Erro ao converter {Path(arquivo).name}: {str(e)}")
            
            self.status.configure(text=f"✅ Conversão para {formato.nome} finalizada.")
            self.progresso.set(1)
        except Exception as e:
            logging.error(f"Erro durante a conversão: {str(e)}")
            messagebox.showerror("Erro", f"Erro durante a conversão: {str(e)}")

if __name__ == '__main__':
    app = AplicacaoConversor()
    app.mainloop()
