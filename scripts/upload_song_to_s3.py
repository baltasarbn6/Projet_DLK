import requests
import boto3
import json
import os
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from unidecode import unidecode
import sys

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
    """Effectue une requête à l'API Genius."""
    headers = {"Authorization": f"Bearer {GENIUS_ACCESS_TOKEN}"}
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params)
    return response.json() if response.status_code == 200 else None


def get_song_details_by_title(song_title):
    """Recherche une chanson par son titre et récupère ses détails."""
    data = request_genius("/search", {"q": song_title})
    if not data:
        return None

    for hit in data["response"]["hits"]:
        if unidecode(hit["result"]["title"].lower()) == unidecode(song_title.lower()):
            song_id = hit["result"]["id"]
            return request_genius(f"/songs/{song_id}")["response"]["song"]
    return None


def get_artist_details(artist_id):
    """Récupère les informations de l'artiste."""
    data = request_genius(f"/artists/{artist_id}")
    if not data:
        return {"name": "Inconnu", "image_url": "", "bio": "Biographie non disponible"}

    artist = data["response"]["artist"]
    bio_text = ""
    if "description" in artist and "dom" in artist["description"]:
        for item in artist["description"]["dom"].get("children", []):
            if isinstance(item, dict) and "children" in item:
                bio_text += "".join([c if isinstance(c, str) else "" for c in item["children"]])

    return {
        "name": artist["name"],
        "image_url": artist["image_url"],
        "bio": bio_text.strip() or "Biographie non disponible",
    }


def get_song_lyrics(song_url):
    """Scrape les paroles d'une chanson depuis Genius."""
    try:
        response = requests.get(song_url, timeout=10)
        if response.status_code != 200:
            return "Paroles indisponibles"

        soup = BeautifulSoup(response.text, "html.parser")
        lyrics_divs = soup.find_all("div", {"data-lyrics-container": "true"})
        lyrics = "\n".join(div.get_text(separator="\n") for div in lyrics_divs)
        return lyrics.strip() if lyrics else "Paroles indisponibles"
    except Exception as e:
        print(f"Erreur lors du scraping des paroles : {e}")
        return "Paroles indisponibles"


def upload_song_to_s3(song_title):
    """Récupère les détails d'une chanson et les téléverse sur S3."""
    song_data = get_song_details_by_title(song_title)
    if not song_data:
        print(f"Chanson introuvable : {song_title}")
        return

    # Récupérer les informations de l'artiste
    artist_id = song_data["primary_artist"]["id"]
    artist_info = get_artist_details(artist_id)

    # Récupérer les paroles et traduction si nécessaire
    lyrics = get_song_lyrics(song_data["url"])
    french_lyrics = ""
    if song_data.get("language") != "fr":
        fr_translation = next((t for t in song_data.get("translation_songs", []) if t["language"] == "fr"), None)
        if fr_translation:
            french_lyrics = get_song_lyrics(fr_translation["url"])

    # Créer le contenu JSON à téléverser
    song_info = {
        "title": song_data["title"],
        "artist": {
            "name": artist_info["name"],
            "image_url": artist_info["image_url"],
            "bio": artist_info["bio"]
        },
        "url": song_data["url"],
        "image_url": song_data["song_art_image_url"],
        "language": song_data.get("language", "unknown"),
        "release_date": song_data.get("release_date", "unknown"),
        "pageviews": song_data["stats"].get("pageviews", 0),
        "lyrics": lyrics,
        "french_lyrics": french_lyrics
    }

    # Envoi vers S3
    s3_key = f"raw/{unidecode(song_title).replace(' ', '_').lower()}.json"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=json.dumps(song_info, ensure_ascii=False, indent=4).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Téléversé sur S3 : {song_title}")


if __name__ == "__main__":
    songs = sys.argv[1:]
    for song in songs:
        upload_song_to_s3(song)
