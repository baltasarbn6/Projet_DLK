import boto3
import pymysql
from dotenv import load_dotenv
import os
import re
import json

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

# Connexion à MySQL
connection = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    port=MYSQL_PORT,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

def list_raw_files():
    """Liste les fichiers JSON présents dans le bucket S3."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

def clean_lyrics(raw_lyrics):
    """Nettoie les paroles en supprimant les balises et les espaces inutiles."""
    if not raw_lyrics:
        return "Paroles indisponibles"
    raw_lyrics = re.sub(r"^Paroles\s*:", "", raw_lyrics)
    raw_lyrics = re.sub(r"\[.*?\]", "", raw_lyrics)  # Retire les titres comme [Refrain], [Couplet 1]...
    return "\n".join(line.strip() for line in raw_lyrics.split("\n") if line.strip())

def artist_exists(artist_name):
    """Vérifie si un artiste existe dans la base."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
        return cursor.fetchone()

def process_file(file_key):
    """Traite un fichier JSON depuis S3 et insère les données dans MySQL."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
    except Exception as e:
        print(f"Erreur lors de la récupération du fichier S3 {file_key} : {e}")
        return

    # Vérification des données de l'artiste
    if "artist" not in data or "name" not in data["artist"]:
        print(f"Données invalides dans {file_key}, champ 'artist' manquant")
        return

    artist_name = data["artist"]["name"]
    bio = data["artist"].get("bio", "Biographie non disponible")
    artist_image_url = data["artist"].get("image_url", "")

    try:
        with connection.cursor() as cursor:
            # Vérifier si l'artiste existe déjà
            cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
            artist = cursor.fetchone()

            if not artist:
                cursor.execute(
                    """
                    INSERT INTO artists (name, bio, image_url) 
                    VALUES (%s, %s, %s)
                    """,
                    (artist_name, bio, artist_image_url),
                )
                connection.commit()
                print(f"Artiste inséré : {artist_name}")

                # Récupérer l'ID nouvellement inséré
                cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
                artist_id = cursor.fetchone()["id"]
            else:
                artist_id = artist["id"]
                print(f"Artiste déjà en base : {artist_name} (ID: {artist_id})")

            # Insertion des chansons
            for song in data.get("songs", []):
                title = song.get("title", "Titre inconnu")
                url = song.get("url", "")
                image_url = song.get("image_url", "")
                language = song.get("language", "unknown")
                release_date = song.get("release_date", None)
                pageviews = song.get("pageviews", 0)
                lyrics = clean_lyrics(song.get("lyrics", "Paroles indisponibles"))
                french_lyrics = clean_lyrics(song.get("french_lyrics", ""))

                cursor.execute(
                    """
                    INSERT INTO songs (artist_id, title, url, image_url, language, release_date, pageviews, lyrics, french_lyrics)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        url = VALUES(url),
                        image_url = VALUES(image_url),
                        pageviews = VALUES(pageviews),
                        lyrics = VALUES(lyrics),
                        french_lyrics = VALUES(french_lyrics)
                    """,
                    (artist_id, title, url, image_url, language, release_date, pageviews, lyrics, french_lyrics)
                )
            connection.commit()

        print(f"Données insérées/actualisées pour l'artiste : {artist_name}")

    except Exception as e:
        connection.rollback()
        print(f"Erreur MySQL pour {artist_name} : {e}")

def process_all_files():
    """Parcourt et traite tous les fichiers JSON présents sur S3."""
    files = list_raw_files()
    print(f"Fichiers trouvés : {len(files)}")
    for file_key in files:
        print(f"Traitement du fichier : {file_key}")
        process_file(file_key)

if __name__ == "__main__":
    process_all_files()
