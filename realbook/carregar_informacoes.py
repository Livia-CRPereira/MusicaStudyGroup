import pandas as pd
import os
from unidecode import unidecode

# --- Caminhos ---
INPUT_CSV = 'MusicaStudyGroup/realbook/musicas_realbook_completo.csv'
PASTA_TITULO = 'busca_por_titulo'
PASTA_COMPLETA = 'busca_completa'
OUTPUT_CSV = 'musicas_com_status.csv'

# --- Carrega CSV ---
df = pd.read_csv(INPUT_CSV, names=["titulo", "autor", "ano"])

# --- Função de normalização robusta ---
def normalizar_nome(nome):
    return unidecode(nome.lower().strip())

# --- Corrige extração de arquivos .mp3.mp3 ---
def extrair_nomes_musicas(pasta):
    nomes = set()
    for nome_arquivo in os.listdir(pasta):
        if nome_arquivo.startswith("temp_") and nome_arquivo.endswith(".mp3.mp3"):
            nome = nome_arquivo[len("temp_"):-len(".mp3.mp3")]
            nomes.add(normalizar_nome(nome))
    return nomes

baixadas_por_titulo = extrair_nomes_musicas(PASTA_TITULO)
baixadas_completas = extrair_nomes_musicas(PASTA_COMPLETA)

# --- Monta status e busca ---
status_list = []
busca_list = []

for _, row in df.iterrows():
    titulo = str(row["titulo"]).strip()
    autor = str(row["autor"]).strip() if not pd.isna(row["autor"]) else ""
    nome_completo = f"{titulo} - {autor.split('/')[0].strip()}" if autor else titulo

    norm_titulo = normalizar_nome(titulo)
    norm_completo = normalizar_nome(nome_completo)

    if norm_completo in baixadas_completas:
        status_list.append(1)
        busca_list.append("completa")
    elif norm_titulo in baixadas_por_titulo:
        status_list.append(1)
        busca_list.append("titulo")
    else:
        status_list.append(0)
        busca_list.append("")

# --- Salva resultado ---
df["status"] = status_list
df["busca"] = busca_list
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ CSV salvo com sucesso como '{OUTPUT_CSV}'")


