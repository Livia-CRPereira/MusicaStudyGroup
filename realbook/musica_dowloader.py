# musica_downloader.py
# musica_downloader_DEBUG.py
import pandas as pd
import os
import time
import re
import yt_dlp
import logging
from moviepy.editor import AudioFileClip
from youtubesearchpython import VideosSearch
import concurrent.futures
from tqdm import tqdm

# --- CONFIGURAÇÃO ---
INPUT_CSV_FILE = 'MusicaStudyGroup/realbook/musicas_realbook_completo.csv'
BUSCA_COMPLETA_FOLDER = 'busca_completa'
BUSCA_POR_TITULO_FOLDER = 'busca_por_titulo'
LOG_FILE = 'erros.log'
MAX_WORKERS = 4
CLIP_DURATION = 40

# --- CONFIGURAÇÃO DO LOGGING ---
logging.basicConfig(level=logging.ERROR, 
                    filename=LOG_FILE, 
                    filemode='w', 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def sanitize_filename(filename):
    sanitized = re.sub(r'[\\/*?:"<>|]', "", filename)
    return sanitized[:150]

def download_and_process_audio(video_url, output_path, filename):
    sanitized_filename = sanitize_filename(filename)
    temp_audio_path = os.path.join(output_path, f"temp_{sanitized_filename}.mp3")
    final_audio_path = os.path.join(output_path, f"{sanitized_filename}.mp3")

    try:
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': temp_audio_path,
            'max_filesize': 50 * 1024 * 1024,
            'quiet': True,
            'no_warnings': True,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        if not os.path.exists(temp_audio_path):
             logging.error(f"yt-dlp não conseguiu criar o arquivo para '{filename}'")
             return False

        with AudioFileClip(temp_audio_path) as audio:
            end_duration = min(audio.duration, CLIP_DURATION)
            if end_duration > 0:
                with audio.subclip(0, end_duration) as final_clip:
                    final_clip.write_audiofile(final_audio_path, logger=None, codec='libmp3lame')
            else:
                logging.error(f"Áudio com duração zero para '{filename}'")
                return False
        return True
    except Exception as e:
        print(f"ERRO NO DOWNLOAD para '{filename}': {e}") # LINHA DE DEBUG ADICIONADA
        logging.error(f"Erro no download/processamento para '{filename}': {e}")
        return False
    finally:
        if os.path.exists(temp_audio_path):
            os.remove(temp_audio_path)

def search_youtube_link(query):
    try:
        search_query = f"{query} audio"
        videos_search = VideosSearch(search_query, limit=1)
        results = videos_search.result()
        
        if results and results.get('result') and len(results['result']) > 0:
            return results['result'][0]['link']
        else:
            videos_search = VideosSearch(query, limit=1)
            results = videos_search.result()
            if results and results.get('result') and len(results['result']) > 0:
                 return results['result'][0]['link']
    except Exception as e:
        print(f"ERRO NA BUSCA por '{query}': {e}") # LINHA DE DEBUG ADICIONADA
        logging.error(f"Erro ao buscar por '{query}': {e}")
        return None
    return None

def process_song(row_tuple):
    index, row = row_tuple
    title = str(row.get('Titulo', '')).strip()
    author = str(row.get('Autor', '')).strip()

    if not title:
        return "Falha (Título vazio)"
    
    if pd.isna(row.get('Autor')) or author == '':
        query = title
        output_folder = BUSCA_POR_TITULO_FOLDER
        filename = title
    else:
        main_author = author.split('/')[0].strip()
        query = f"{title} {main_author}"
        output_folder = BUSCA_COMPLETA_FOLDER
        filename = f"{title} - {main_author}"

    time.sleep(0.1)
    
    video_url = search_youtube_link(query)
    
    if video_url:
        success = download_and_process_audio(video_url, output_folder, filename)
        if success:
            return f"Sucesso: {filename}"
        else:
            return f"Falha (erro no download): {filename}"
    else:
        return f"Falha (vídeo não encontrado): {query}"

def main():
    print("Iniciando o processo de download de áudios...")
    print(f"Erros detalhados serão salvos em '{LOG_FILE}'")

    if not os.path.exists(INPUT_CSV_FILE):
        print(f"ERRO: Arquivo de entrada '{INPUT_CSV_FILE}' não encontrado!")
        return

    os.makedirs(BUSCA_COMPLETA_FOLDER, exist_ok=True)
    os.makedirs(BUSCA_POR_TITULO_FOLDER, exist_ok=True)

    try:
        df = pd.read_csv(INPUT_CSV_FILE, encoding='utf-8')
    except Exception as e:
        print(f"ERRO ao ler o CSV: {e}. Verifique se o arquivo '{INPUT_CSV_FILE}' está salvo com a codificação correta (UTF-8).")
        return

    print(f"Encontradas {len(df)} músicas no arquivo CSV para processar.")

    tasks = list(df.iterrows())
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        with tqdm(total=len(tasks), desc="Baixando músicas") as pbar:
            futures = [executor.submit(process_song, task) for task in tasks]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())
                pbar.update(1)

    print("\n--- Processo de download concluído! ---")
    
    sucessos = sum(1 for r in results if r.startswith("Sucesso"))
    falhas = len(results) - sucessos
    
    print(f"✅ Áudios baixados com sucesso: {sucessos}")
    print(f"❌ Falhas: {falhas}")
    print(f"Verifique as pastas '{BUSCA_COMPLETA_FOLDER}' e '{BUSCA_POR_TITULO_FOLDER}'.")
    if falhas > 0:
        print(f"👉 Detalhes sobre os erros foram salvos no arquivo '{LOG_FILE}'.")


if __name__ == "__main__":
    main()