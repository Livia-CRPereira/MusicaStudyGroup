import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import time
import os

def get_all_song_links(base_url):
    """
    Varre a página principal para encontrar todos os links de músicas,
    procurando dentro das seções alfabéticas 'letter-section'.
    """
    try:
        print(f"Acessando a página principal: {base_url}")
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        letter_sections = soup.find_all('div', class_='letter-section')
        if not letter_sections:
            print("ERRO: Não foi possível encontrar nenhuma 'div' com a classe 'letter-section'.")
            return []

        song_links = []
        for section in letter_sections:
            links_in_section = section.find_all('a', href=True)
            for link in links_in_section:
                href = link['href']
                if href.startswith(base_url) or href.startswith('/'):
                    full_url = href if href.startswith('http') else f"{base_url.rstrip('/')}{href}"
                    song_links.append(full_url)

        song_links = sorted(list(set(song_links)))
        print(f"Encontrados {len(song_links)} links de músicas únicos.")
        return song_links

    except requests.exceptions.RequestException as e:
        print(f"Erro crítico ao acessar a página de índice {base_url}: {e}")
        return []

def get_song_details(song_url):
    """
    Extrai o título, ano e autor. É tolerante a falhas: salva a música
    se pelo menos o título for encontrado.
    """
    try:
        response = requests.get(song_url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find('h1', class_='entry-title')
        title = title_tag.text.strip() if title_tag else None

        # Se não conseguirmos nem encontrar o título, desistimos desta página.
        if not title:
            return None

        # Agora tentamos encontrar o autor e o ano, mas não descartamos a música se falharmos.
        author = None
        year = None
        content_div = soup.find('div', class_='entry-content')
        
        if content_div:
            author_tag = content_div.find('div', align='right')
            if author_tag:
                full_text = author_tag.text.strip()
                separator = ' – ' if ' – ' in full_text else (' - ' if ' - ' in full_text else None)
                
                if separator:
                    parts = full_text.split(separator, 1)
                    if len(parts) == 2:
                        year = parts[0].strip()
                        author = parts[1].strip()
                    else:
                        author = full_text
                else:
                    author = full_text
        
        # MODIFICAÇÃO CHAVE: Retorna o dicionário mesmo que autor ou ano sejam nulos.
        return {'Titulo': title, 'Ano': year, 'Autor': author}

    except requests.exceptions.RequestException:
        # Erro de rede ou timeout, retorna None silenciosamente. O loop principal irá reportar.
        return None

def main():
    """Função principal para orquestrar o scraping."""
    BASE_URL = "https://realbook.site"
    OUTPUT_FILENAME = 'musicas_realbook_completo.csv'
    
    print("--- Iniciando o scraping do Real Book Site (Modo Robusto) ---")
    
    all_song_links = get_all_song_links(BASE_URL)
    
    if not all_song_links:
        print("\nNenhum link de música encontrado.")
        return

    all_songs_data = []
    failed_links = []
    
    for link in tqdm(all_song_links, desc="Extraindo dados das músicas"):
        details = get_song_details(link)
        if details:
            all_songs_data.append(details)
        else:
            # MODIFICAÇÃO: Guarda os links que falharam para análise.
            failed_links.append(link)
        
        time.sleep(0.05)

    if not all_songs_data:
        print("\nERRO: Nenhum dado de música foi extraído.")
        return

    df = pd.DataFrame(all_songs_data)
    
    # MODIFICAÇÃO: Não vamos mais usar dropna(), para manter músicas sem autor/ano.
    # df.dropna(subset=['Titulo', 'Autor'], inplace=True) # REMOVIDO
    
    # Manter a remoção de duplicatas exatas é uma boa prática.
    df.drop_duplicates(subset=['Titulo', 'Ano', 'Autor'], inplace=True)
    
    df = df[['Titulo', 'Autor', 'Ano']]
    
    df.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8')
    
    print(f"\n--- Scraping concluído! ---")
    print(f"-> {len(df)} músicas salvas com sucesso em '{OUTPUT_FILENAME}'.")
    
    # Imprime um relatório sobre os links que falharam.
    if failed_links:
        print(f"-> {len(failed_links)} links não puderam ser processados (nenhum título encontrado ou erro de acesso).")
        # Se quiser ver os links que falharam, descomente a linha abaixo:
        # print("Links com falha:", failed_links)

if __name__ == "__main__":
    main()
