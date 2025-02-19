import boto3
import pymysql
import os
import json
import re
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/.env")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))

# Initialisation du client S3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# Connexion MySQL
connection = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    port=MYSQL_PORT,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

def clean_lyrics(raw_lyrics):
    """Nettoie les paroles en supprimant les balises et les espaces inutiles."""
    if not raw_lyrics or raw_lyrics == "Paroles indisponibles":
        return "Paroles indisponibles"
    raw_lyrics = re.sub(r"^Paroles\s*:", "", raw_lyrics)
    raw_lyrics = re.sub(r"\[.*?\]", "", raw_lyrics)  # Retire les titres comme [Refrain], [Couplet 1]...
    return "\n".join(line.strip() for line in raw_lyrics.split("\n") if line.strip())

def artist_exists(artist_name):
    """Vérifie si un artiste existe dans la base."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
        return cursor.fetchone()

def insert_artist(artist_name, bio, image_url):
    """Ajoute un artiste dans la base s'il n'existe pas encore et retourne son ID."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO artists (name, bio, image_url) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE bio=VALUES(bio), image_url=VALUES(image_url)
            """,
            (artist_name, bio, image_url),
        )
        connection.commit()
        
        cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
        return cursor.fetchone()["id"]

def list_song_files():
    """Liste uniquement les fichiers de chansons depuis S3."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    files = response.get("Contents", [])
    song_files = []

    for obj in files:
        key = obj["Key"]
        if key.endswith(".json"):
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            content = json.loads(response["Body"].read().decode("utf-8"))

            if "title" in content:
                song_files.append(key)
    return song_files

def process_song_file(file_key):
    """Lit et insère un fichier JSON de chanson dans MySQL."""
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    data = json.loads(response["Body"].read().decode("utf-8"))

    artist_name = data["artist"]["name"]
    artist_bio = data["artist"].get("bio", "Biographie non disponible")
    artist_image_url = data["artist"].get("image_url", "")

    artist = artist_exists(artist_name)
    if not artist:
        artist_id = insert_artist(artist_name, artist_bio, artist_image_url)
        print(f"Artiste ajouté : {artist_name}")
    else:
        artist_id = artist["id"]

    title = data.get("title", "Titre inconnu")
    url = data.get("url", "")
    image_url = data.get("image_url", "")
    language = data.get("language", "unknown")
    release_date = data.get("release_date", None)
    pageviews = data.get("pageviews", 0)

    # Nettoyer les paroles avant insertion dans MySQL
    lyrics = clean_lyrics(data.get("lyrics", ""))
    french_lyrics = clean_lyrics(data.get("french_lyrics", ""))

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO songs (artist_id, title, url, image_url, language, release_date, pageviews, lyrics, french_lyrics)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                url=VALUES(url), 
                image_url=VALUES(image_url), 
                pageviews=VALUES(pageviews), 
                lyrics=VALUES(lyrics),
                french_lyrics=VALUES(french_lyrics)
            """,
            (artist_id, title, url, image_url, language, release_date, pageviews, lyrics, french_lyrics),
        )
        connection.commit()

    print(f"Chanson insérée : {title} ({artist_name})")

def process_all_songs():
    """Parcourt tous les fichiers de chansons et les insère dans MySQL."""
    files = list_song_files()
    for file_key in files:
        print(f"Traitement de {file_key}...")
        process_song_file(file_key)

if __name__ == "__main__":
    process_all_songs()
