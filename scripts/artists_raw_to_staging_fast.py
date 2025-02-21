import boto3
import pymysql
from dotenv import load_dotenv
import os
import re
import json
from datetime import datetime
from dateutil import parser
from concurrent.futures import ThreadPoolExecutor, as_completed

# Chargement des variables d'environnement
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

def create_mysql_connection():
    """Crée une nouvelle connexion MySQL pour chaque thread."""
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        port=MYSQL_PORT,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

def format_release_date(release_date: str) -> str:
    """Formate la date de sortie dans un format compatible MySQL (YYYY-MM-DD)."""
    if not release_date or release_date.lower() == "unknown":
        return None

    try:
        parsed_date = parser.parse(release_date, default=datetime(1900, 1, 1))
        return parsed_date.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"Erreur de formatage de la date complète : {release_date} - Erreur : {e}")
    
    try:
        if re.match(r"^[A-Za-z]+\s\d{4}$", release_date):
            parsed_date = parser.parse(f"1 {release_date}")
            return parsed_date.strftime("%Y-%m-%d")
        elif re.match(r"^\d{4}$", release_date):
            return f"{release_date}-01-01"
    except Exception as e:
        print(f"Erreur de formatage pour date partielle : {release_date} - Erreur : {e}")
    
    return None

def list_raw_files():
    """Liste les fichiers JSON présents dans le bucket S3."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

def clean_lyrics(raw_lyrics):
    """Nettoie les paroles en supprimant les balises et les espaces inutiles."""
    if not raw_lyrics:
        return "Paroles indisponibles"
    raw_lyrics = re.sub(r"^Paroles\s*:", "", raw_lyrics)
    raw_lyrics = re.sub(r"\[.*?\]", "", raw_lyrics)
    return "\n".join(line.strip() for line in raw_lyrics.split("\n") if line.strip())

def process_file(file_key: str):
    """Traite un fichier JSON depuis S3 et insère les données dans MySQL."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
    except Exception as e:
        print(f"Erreur lors de la récupération du fichier S3 {file_key} : {e}")
        return

    artist_name = data["artist"]["name"]
    bio = data["artist"].get("bio", "Biographie non disponible")
    artist_image_url = data["artist"].get("image_url", "")

    connection = create_mysql_connection()

    try:
        with connection.cursor() as cursor:
            # Insertion ou mise à jour de l'artiste
            cursor.execute(
                """
                INSERT INTO artists (name, bio, image_url) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    bio = VALUES(bio),
                    image_url = VALUES(image_url)
                """,
                (artist_name, bio, artist_image_url),
            )
            connection.commit()

            cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
            artist_id = cursor.fetchone()["id"]

            # Insertion des chansons
            for song in data.get("songs", []):
                title = song.get("title", "Titre inconnu")
                url = song.get("url", "")
                image_url = song.get("image_url", "")
                language = song.get("language", "unknown")
                release_date_raw = song.get("release_date", None)
                release_date = format_release_date(release_date_raw)
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
    finally:
        connection.close()

def process_all_files():
    """Parcourt et traite tous les fichiers JSON présents sur S3 en parallèle."""
    files = list_raw_files()
    print(f"Fichiers trouvés : {len(files)}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_file, file_key): file_key for file_key in files}

        for future in as_completed(futures):
            file_key = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Erreur lors du traitement de {file_key} : {e}")

if __name__ == "__main__":
    process_all_files()
