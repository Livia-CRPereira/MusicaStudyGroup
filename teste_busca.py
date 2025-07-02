# teste_busca.py
import pandas as pd
import os
import time
import re
import yt_dlp
from moviepy.editor import AudioFileClip
from youtubesearchpython import VideosSearch
import concurrent.futures
from tqdm import tqdm

# --- CONFIGURAÇÃO ---
INPUT_CSV_FILE = 'musicas_realbook_completo.csv'
BUSCA_COMPLETA_FOLDER = 'busca_completa'
BUSCA_POR_TITULO_FOLDER = 'busca_por_titulo'
MAX_WORKERS = 5 # Número de downloads simultâneos

def sanitize_filename(filename):
    """Remove caracteres inválidos para nomes de arquivo."""
    return re.sub(r'[\\/*?:"<>|]', "", filename)

def download_audio(video_url, output_path, filename):
    """Faz o download do áudio, converte para MP3 e corta para 40 segundos."""
    try:
        # Garante que o nome do arquivo seja seguro para o sistema de arquivos
        sanitized_filename = sanitize_filename(filename)
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'outtmpl': os.path.join(output_path, f'{sanitized_filename}.%(ext)s'),
            'max_filesize': 50 * 1024 * 1024,  # Limite de 50 MB
            'quiet': True, # Suprime a saída do yt-dlp no console
            'no_warnings': True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        # Corta os primeiros 40 segundos do áudio
        mp3_path = os.path.join(output_path, f'{sanitized_filename}.mp3')
        
        # Verifica se o arquivo foi realmente criado antes de tentar cortá-lo
        if not os.path.exists(mp3_path):
            # print(f"AVISO: O arquivo MP3 '{mp3_path}' não foi encontrado após o download. Pulando o corte.")
            return False

        audio = AudioFileClip(mp3_path)
        # Garante que o clipe não seja mais longo que o áudio original
        end_duration = min(audio.duration, 40)
        final_clip = audio.subclip(0, end_duration)
        final_clip.write_audiofile(mp3_path, logger=None)
        
        # Fecha os clips para liberar memória
        audio.close()
        final_clip.close()

        return True
    except Exception as e:
        # print(f"Erro durante o download ou processamento para '{filename}': {e}")
        return False

def search_youtube_link(query):
    """Busca no YouTube e retorna o link do primeiro resultado."""
    try:
        videos_search = VideosSearch(query, limit=1)
        results = videos_search.result()
        if results and 'result' in results and len(results['result']) > 0:
            return results['result'][0]['link']
    except Exception as e:
        # print(f"Erro ao buscar por '{query}': {e}")
        return None
    return None

def process_song(row_tuple):
    """Função 'worker' que processa uma única linha do DataFrame."""
    index, row = row_tuple
    title = row['Titulo']
    author = row['Autor']

    # 1. Decide qual busca fazer e onde salvar
    if pd.isna(author) or str(author).strip() == '':
        # Busca por Título
        query = title
        output_folder = BUSCA_POR_TITULO_FOLDER
        filename = title
    else:
        # Busca Completa (Titulo-Autor sem espaços)
        title_no_space = str(title).replace(' ', '')
        author_no_space = str(author).replace(' ', '')
        query = f"{title_no_space}-{author_no_space}"
        output_folder = BUSCA_COMPLETA_FOLDER
        filename = f"{title} - {author}" # Nome de arquivo mais legível

    # 2. Busca o link no YouTube
    video_url = search_youtube_link(query)
    
    # 3. Faz o download se o link for encontrado
    if video_url:
        download_audio(video_url, output_folder, filename)
        return f"Sucesso: {filename}"
    else:
        return f"Falha (vídeo não encontrado): {query}"

def main():
    """Função principal que orquestra todo o processo."""
    print("Iniciando o processo de download de áudios...")

    # Verifica se o arquivo CSV de entrada existe
    if not os.path.exists(INPUT_CSV_FILE):
        print(f"ERRO: Arquivo de entrada '{INPUT_CSV_FILE}' não encontrado!")
        print("Por favor, execute o script de scraping primeiro.")
        return

    # Cria as pastas de destino
    os.makedirs(BUSCA_COMPLETA_FOLDER, exist_ok=True)
    os.makedirs(BUSCA_POR_TITULO_FOLDER, exist_ok=True)

    # Carrega o DataFrame a partir do arquivo CSV
    df = pd.read_csv(INPUT_CSV_FILE)
    print(f"Encontradas {len(df)} músicas no arquivo CSV para processar.")

    # Usa ThreadPoolExecutor para downloads concorrentes
    tasks = list(df.iterrows())
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Usando tqdm para criar uma barra de progresso
        results = list(tqdm(executor.map(process_song, tasks), total=len(tasks), desc="Baixando músicas"))

    print("\n--- Processo de download concluído! ---")
    
    # Contagem final
    sucessos = sum(1 for r in results if r.startswith("Sucesso"))
    falhas = len(results) - sucessos
    
    print(f"✅ Áudios baixados com sucesso: {sucessos}")
    print(f"❌ Falhas (vídeos não encontrados ou erro no download): {falhas}")
    print(f"Verifique as pastas '{BUSCA_COMPLETA_FOLDER}' e '{BUSCA_POR_TITULO_FOLDER}'.")


if __name__ == "__main__":
    main()