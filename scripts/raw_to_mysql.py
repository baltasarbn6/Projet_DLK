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
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = response["Body"].read().decode("utf-8")
    data = json.loads(content)

    artist_name = data["artist"]["name"]
    bio = data["artist"].get("bio", "Biographie non disponible")
    artist_image_url = data["artist"]["image_url"]

    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO artists (name, bio, image_url) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE bio=%s, image_url=%s",
            (artist_name, bio, artist_image_url, bio, artist_image_url),
        )
        cursor.execute("SELECT id FROM artists WHERE name = %s", (artist_name,))
        artist_id = cursor.fetchone()["id"]

    for song in data["songs"]:
        title = song.get("title", "Titre inconnu")
        url = song.get("url", None)
        image_url = song.get("image_url", "https://default-image-url.com")
        language = song.get("language", "unknown")
        release_date = song.get("release_date", None)
        pageviews = song.get("pageviews", 0)

        raw_lyrics = song.get("lyrics", "Paroles indisponibles")
        cleaned_lyrics = clean_lyrics(raw_lyrics)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO songs (artist_id, title, url, image_url, language, release_date, pageviews, lyrics)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE url=%s, image_url=%s, language=%s, release_date=%s, pageviews=%s, lyrics=%s
                """,
                (artist_id, title, url, image_url, language, release_date, pageviews, cleaned_lyrics,
                url, image_url, language, release_date, pageviews, cleaned_lyrics),
            )

    connection.commit()
    print(f"Données insérées pour l'artiste : {artist_name}")

def process_all_files():
    files = list_raw_files()
    print(f"Fichiers trouvés : {len(files)}")
    for file_key in files:
        print(f"Traitement du fichier : {file_key}")
        process_file(file_key)

if __name__ == "__main__":
    process_all_files()
