import requests
import boto3
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup

# Charger les variables d'environnement
load_dotenv()

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


def get_artist_id(artist_name):
    """
    Recherche l'ID d'un artiste sur Genius.
    """
    url = f"{BASE_URL}/search"
    headers = {"Authorization": f"Bearer {GENIUS_ACCESS_TOKEN}"}
    params = {"q": artist_name}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        for hit in data["response"]["hits"]:
            if hit["result"]["primary_artist"]["name"].lower() == artist_name.lower():
                return hit["result"]["primary_artist"]["id"]
        print(f"Aucun ID trouvé pour l'artiste : {artist_name}")
    else:
        print(f"Erreur lors de la recherche de l'artiste {artist_name}: {response.status_code}")
    return None


def get_artist_details(artist_id):
    """
    Récupère les informations d'un artiste.
    """
    url = f"{BASE_URL}/artists/{artist_id}"
    headers = {"Authorization": f"Bearer {GENIUS_ACCESS_TOKEN}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()["response"]["artist"]
        return {
            "name": data["name"],
            "image_url": data["image_url"],
            "bio": data.get("description", {}).get("plain", None),
        }
    else:
        print(f"Erreur lors de la récupération des informations de l'artiste : {response.status_code}")
        return None


def download_image(image_url, key):
    """
    Télécharge une image depuis une URL et la téléverse sur S3.
    """
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            s3_client.put_object(
                Bucket=BUCKET_NAME, Key=key, Body=response.content, ContentType="image/jpeg"
            )
            print(f"Téléversement réussi pour l'image sous la clé {key}")
            return key
        else:
            print(f"Erreur lors du téléchargement de l'image : {response.status_code}")
            return None
    except Exception as e:
        print(f"Erreur lors du téléchargement de l'image : {str(e)}")
        return None


def get_popular_songs_by_artist(artist_id):
    """
    Récupère les chansons populaires d'un artiste.
    """
    url = f"{BASE_URL}/artists/{artist_id}/songs"
    headers = {"Authorization": f"Bearer {GENIUS_ACCESS_TOKEN}"}
    params = {"sort": "popularity", "per_page": 10}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()["response"]["songs"]
    else:
        print(f"Erreur lors de la récupération des chansons populaires : {response.status_code}")
        return []


def get_song_lyrics(song_url):
    """
    Scrape les paroles depuis une URL Genius.
    """
    response = requests.get(song_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        lyrics_divs = soup.find_all("div", {"data-lyrics-container": "true"})
        lyrics = "\n".join(div.get_text(separator="\n") for div in lyrics_divs)
        return lyrics.strip() if lyrics else "Paroles indisponibles"
    else:
        return f"Erreur lors de la récupération des paroles : {response.status_code}"


def upload_artist_data_to_s3(artist_name, artist_details, songs, artist_image_key):
    """
    Téléverse les informations de l'artiste et de ses chansons populaires sur S3.
    """
    content = f"Informations sur {artist_name} :\n"
    content += f"Nom : {artist_details['name']}\n"
    content += f"Biographie : {artist_details['bio'] if artist_details['bio'] else 'Biographie non disponible'}\n"
    content += f"Image S3 Key : {artist_image_key}\n\n"
    content += "Chansons populaires :\n"

    for song in songs:
        # Téléverser l'image de la chanson sur S3
        song_image_key = f"raw/{artist_name.replace(' ', '_').lower()}_{song['title'].replace(' ', '_').lower()}_image.jpg"
        download_image(song["song_art_image_url"], song_image_key)

        # Récupérer les paroles
        lyrics = get_song_lyrics(song["url"])

        # Ajouter les informations de la chanson au contenu
        content += f"- {song['title']} (URL : {song['url']})\n"
        content += f"  Image S3 Key : {song_image_key}\n"
        content += f"  Paroles :\n{lyrics}\n\n"

    # Téléverser les données de l'artiste et des chansons sur S3
    s3_key = f"raw/{artist_name.replace(' ', '_').lower()}_popular_songs.txt"
    try:
        s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=content)
        print(f"Téléversement réussi pour {artist_name} sous la clé {s3_key}")
    except Exception as e:
        print(f"Erreur lors du téléversement des données de {artist_name} : {str(e)}")


def process_artists(file_path):
    """
    Récupère les données pour une liste d'artistes à partir d'un fichier.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        artists = [line.strip() for line in f.readlines()]

    for artist_name in artists:
        print(f"Traitement de l'artiste : {artist_name}")

        # Récupérer l'ID et les détails de l'artiste
        artist_id = get_artist_id(artist_name)
        if not artist_id:
            continue

        artist_details = get_artist_details(artist_id)
        if not artist_details:
            continue

        # Télécharger l'image de l'artiste
        artist_image_key = f"raw/{artist_name.replace(' ', '_').lower()}_image.jpg"
        download_image(artist_details["image_url"], artist_image_key)

        # Récupérer les chansons populaires
        songs = get_popular_songs_by_artist(artist_id)
        if not songs:
            continue

        # Téléverser les données sur S3
        upload_artist_data_to_s3(artist_name, artist_details, songs, artist_image_key)


# Exemple d'utilisation
if __name__ == "__main__":
    process_artists("artists.txt")
