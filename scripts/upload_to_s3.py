import requests
import boto3
import json
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(dotenv_path="/opt/airflow/.env")

GENIUS_ACCESS_TOKEN = os.getenv("GENIUS_ACCESS_TOKEN")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Initialisation du client S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

BASE_URL = "https://api.genius.com"

def request_genius(endpoint, params=None):
    """Effectue une requête à l'API Genius et gère les erreurs."""
    headers = {"Authorization": f"Bearer {GENIUS_ACCESS_TOKEN}"}
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erreur API ({response.status_code}) : {response.text}")
        return None

def get_artist_id(artist_name):
    """Recherche l'ID d'un artiste sur Genius."""
    data = request_genius("/search", {"q": artist_name})
    if not data:
        return None

    for hit in data["response"]["hits"]:
        if hit["result"]["primary_artist"]["name"].lower() == artist_name.lower():
            return hit["result"]["primary_artist"]["id"]

    print(f"Aucun match exact pour {artist_name}.")
    return None

def get_artist_details(artist_id):
    """Récupère les informations d'un artiste."""
    data = request_genius(f"/artists/{artist_id}")
    if not data:
        return None

    artist = data["response"]["artist"]
    
    bio_text = ""
    if "description" in artist and "dom" in artist["description"]:
        for item in artist["description"]["dom"].get("children", []):
            if isinstance(item, dict) and "children" in item:
                bio_text += "".join([c if isinstance(c, str) else "" for c in item["children"]])

    return {
        "name": artist["name"],
        "image_url": artist["image_url"],
        "bio": bio_text.strip(),
    }

def get_popular_songs_by_artist(artist_id):
    """Récupère les chansons populaires d'un artiste."""
    data = request_genius(f"/artists/{artist_id}/songs", {"sort": "popularity", "per_page": 10})
    if not data:
        return []

    songs = []
    for song in data["response"]["songs"]:
        songs.append({
            "id": song["id"],
            "title": song["title"],
            "url": song["url"],
            "song_art_image_url": song["song_art_image_url"]
        })
    return songs

def get_song_details(song_id):
    """Récupère les détails d'une chanson spécifique."""
    data = request_genius(f"/songs/{song_id}")
    if not data:
        return None

    song = data["response"]["song"]
    
    return {
        "title": song["title"],
        "url": song["url"],
        "image_url": song["song_art_image_url"],
        "language": song.get("language", "unknown"),
        "release_date": song.get("release_date", "unknown"),
        "pageviews": song["stats"].get("pageviews", 0)
    }

def get_song_lyrics(song_url):
    """Scrape les paroles d'une chanson depuis Genius."""
    response = requests.get(song_url)
    if response.status_code != 200:
        return f"Erreur lors du scraping : {response.status_code}"

    soup = BeautifulSoup(response.text, "html.parser")
    lyrics_divs = soup.find_all("div", {"data-lyrics-container": "true"})
    lyrics = "\n".join(div.get_text(separator="\n") for div in lyrics_divs)

    return lyrics.strip() if lyrics else "Paroles indisponibles"

def upload_to_s3(key, content):
    """Téléverse du contenu JSON sur S3."""
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=key, 
            Body=json.dumps(content, ensure_ascii=False, indent=4).encode("utf-8"),
            ContentType="application/json"
        )
        print(f"Téléversement réussi : {key}")
    except Exception as e:
        print(f"Erreur lors du téléversement sur S3 : {str(e)}")

def process_artists(file_path):
    """Traite une liste d'artistes et enregistre les données sur S3."""
    if not os.path.exists(file_path):
        print(f"Le fichier {file_path} est introuvable.")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        artists_data = json.load(f)

    for artist_name in artists_data.get("artists", []):
        print(f"\nTraitement de l'artiste : {artist_name}")

        artist_id = get_artist_id(artist_name)
        if not artist_id:
            continue

        artist_details = get_artist_details(artist_id)
        if not artist_details:
            continue

        songs = get_popular_songs_by_artist(artist_id)
        song_details = []
        for song in songs:
            details = get_song_details(song["id"])
            if details:
                details["lyrics"] = get_song_lyrics(details["url"])
                song_details.append(details)

        artist_data = {
            "artist": artist_details,
            "songs": song_details
        }

        s3_key = f"raw/{artist_name.replace(' ', '_').lower()}.json"
        upload_to_s3(s3_key, artist_data)

    print("Données enregistrées sur S3")

if __name__ == "__main__":
    process_artists("/opt/airflow/scripts/artists.json")
