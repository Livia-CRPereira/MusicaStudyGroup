import pandas as pd
import os
import time
import yt_dlp
from moviepy.editor import AudioFileClip
from youtubesearchpython import VideosSearch
import concurrent.futures

def download_audio(video_url, output_path, song_artist):
    try:
        ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'outtmpl': os.path.join(output_path, f'{song_artist}.%(ext)s'),
            'max_filesize': 50 * 1024 * 1024,  # 50 MB limit
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        # Cut the first 40 seconds of audio
        mp3_path = os.path.join(output_path, f'{song_artist}.mp3')
        audio = AudioFileClip(mp3_path).subclip(0, 40)
        audio.write_audiofile(mp3_path, logger=None)
        audio.close()

        return True
    except Exception as e:
        print(f"Error downloading {song_artist}: {e}")
        return False

def search_youtube_link(query):
    try:
        videos_search = VideosSearch(query, limit=1)
        results = videos_search.result()
        if results and 'result' in results and len(results['result']) > 0:
            return results['result'][0]['link']
    except Exception as e:
        print(f"Error searching for {query}: {e}")
    return None

def process_item(item, output_folder):
    video_url = search_youtube_link(item)
    if video_url:
        print(f"Downloading {item} from {video_url}")
        success = download_audio(video_url, output_folder, item)
        if success:
            print(f"Successfully downloaded {item}")
        else:
            print(f"Failed to download {item}")
    else:
        print(f"Video not found for {item}")
    time.sleep(2)  # Add a delay to avoid rate limiting

def main():
    # Load the DataFrame from the CSV file
    df = pd.read_csv('songs_and_artists_updated.csv')

    # Create a list of "SongName-ArtistName" without spaces
    music_artist_list = (df['Song'] + '-' + df['Artist']).str.replace(' ', '').tolist()


    # Folder to store downloaded audios
    output_folder = 'downloaded_audios'
    os.makedirs(output_folder, exist_ok=True)

    # Use ThreadPoolExecutor for concurrent downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_item, item, output_folder) for item in music_artist_list]
        concurrent.futures.wait(futures)

    print("Download complete.")

if __name__ == "__main__":
    main()
