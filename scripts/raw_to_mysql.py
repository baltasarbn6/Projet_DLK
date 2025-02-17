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

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

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
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

def clean_lyrics(raw_lyrics):
    raw_lyrics = re.sub(r"^Paroles\s*:", "", raw_lyrics)
    raw_lyrics = re.sub(r"\[.*?\]", "", raw_lyrics)
    return "\n".join(line.strip() for line in raw_lyrics.split("\n") if line.strip())

def process_file(file_key):
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
    except Exception as e:
        print(f"Erreur lors de la récupération du fichier S3 {file_key} : {e}")
        return

    artist_name = data["artist"]["name"]
    bio = data["artist"].get("bio", "Biographie non disponible")
    artist_image_url = data["artist"]["image_url"]

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO artists (name, bio, image_url) 
                VALUES (%s, %s, %s) 
                ON DUPLICATE KEY UPDATE bio=VALUES(bio), image_url=VALUES(image_url)
                """,
                (artist_name, bio, artist_image_url)
            )
            connection.commit()

            cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
            artist_id = cursor.fetchone()["id"]

            for song in data["songs"]:
                title = song.get("title", "Titre inconnu")
                url = song.get("url", "")
                image_url = song.get("image_url", "")
                language = song.get("language", "unknown")
                release_date = song.get("release_date", None)
                pageviews = song.get("pageviews", 0)
                lyrics = clean_lyrics(song.get("lyrics", "Paroles indisponibles"))

                cursor.execute(
                    """
                    INSERT INTO songs (artist_id, title, url, image_url, language, release_date, pageviews, lyrics)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE url=VALUES(url), image_url=VALUES(image_url), pageviews=VALUES(pageviews), lyrics=VALUES(lyrics)
                    """,
                    (artist_id, title, url, image_url, language, release_date, pageviews, lyrics)
                )
            connection.commit()
        print(f"Données insérées/actualisées pour l'artiste : {artist_name}")

    except Exception as e:
        print(f"Erreur lors de l'insertion en base MySQL pour {artist_name} : {e}")

def process_all_files():
    files = list_raw_files()
    print(f"Fichiers trouvés : {len(files)}")
    for file_key in files:
        print(f"Traitement du fichier : {file_key}")
        process_file(file_key)

if __name__ == "__main__":
    process_all_files()
