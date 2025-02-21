import pymysql
import pymongo
import os
from dotenv import load_dotenv
from random import seed
from datetime import date
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import nltk
from nltk.corpus import stopwords
from pymongo import ReplaceOne

# Configuration aléatoire et chargement des stopwords
seed(42)
nltk.download("stopwords")
STOP_WORDS = set(stopwords.words("french"))

# Charger les variables d'environnement
load_dotenv(dotenv_path="/opt/airflow/.env")

# Configuration MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Configuration MongoDB
MONGO_URI = "mongodb://mongodb-dlk:27017/"
MONGO_DATABASE = "datalakes_curated"

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

def create_mongo_client():
    """Crée une nouvelle connexion MongoDB pour chaque thread."""
    return pymongo.MongoClient(MONGO_URI)[MONGO_DATABASE]

def mask_words(line, percentage, stop_words):
    """Masque un pourcentage de mots dans une ligne de texte."""
    words = line.split()
    maskable_indices = [i for i, w in enumerate(words) if w.lower() not in stop_words]
    num_to_mask = max(1, int(len(maskable_indices) * percentage / 100)) if maskable_indices else 0
    mask_indices = np.random.choice(maskable_indices, min(len(maskable_indices), num_to_mask), replace=False)
    return " ".join("____" if i in mask_indices else word for i, word in enumerate(words))

def generate_difficulty_versions(lyrics, easy_pct=10, medium_pct=25, hard_pct=40):
    """Génère trois versions de paroles en masquant un pourcentage de mots."""
    if not lyrics:
        return {"easy": "Paroles indisponibles", "medium": "Paroles indisponibles", "hard": "Paroles indisponibles"}

    return {
        "easy": "\n".join(mask_words(line, easy_pct, STOP_WORDS) for line in lyrics.splitlines()),
        "medium": "\n".join(mask_words(line, medium_pct, STOP_WORDS) for line in lyrics.splitlines()),
        "hard": "\n".join(mask_words(line, hard_pct, STOP_WORDS) for line in lyrics.splitlines()),
    }

def process_song(song, artist):
    """Prépare un document MongoDB à partir d'une chanson MySQL."""
    difficulty_versions = generate_difficulty_versions(song.get("lyrics", ""))

    return {
        "title": song["title"],
        "artist": {
            "name": artist["name"],
            "bio": artist.get("bio", "Biographie non disponible"),
            "image_url": artist.get("image_url", ""),
        },
        "url": song["url"],
        "image_url": song["image_url"],
        "language": song["language"],
        "release_date": str(song["release_date"]) if isinstance(song["release_date"], date) else song["release_date"],
        "pageviews": song["pageviews"],
        "lyrics": song.get("lyrics", ""),
        "difficulty_versions": difficulty_versions,
        "french_lyrics": song.get("french_lyrics", ""),
    }

def fetch_songs_from_mysql(batch_size=500):
    """Récupère les données de MySQL par lot pour éviter la surcharge mémoire."""
    connection = create_mysql_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as total FROM songs")
            total_songs = cursor.fetchone()["total"]
            print(f"Nombre total de chansons : {total_songs}")

            for offset in range(0, total_songs, batch_size):
                cursor.execute(f"""
                    SELECT songs.*, artists.name AS artist_name, artists.bio AS artist_bio, artists.image_url AS artist_image_url
                    FROM songs 
                    LEFT JOIN artists ON songs.artist_id = artists.id
                    LIMIT {batch_size} OFFSET {offset}
                """)
                yield cursor.fetchall()
    finally:
        connection.close()

def insert_into_mongodb(documents, mongo_db):
    """Insertion optimisée en masse dans MongoDB avec gestion des duplications."""
    if not documents:
        return

    try:
        bulk_operations = [
            ReplaceOne(
                {"title": doc["title"], "artist.name": doc["artist"]["name"]},
                doc,
                upsert=True
            ) for doc in documents
        ]

        result = mongo_db.songs.bulk_write(bulk_operations, ordered=False)
        print(f"Insertion MongoDB : {len(documents)} documents (insertés : {result.upserted_count}, mis à jour : {result.modified_count})")
    except Exception as e:
        print(f"Erreur lors de l'insertion dans MongoDB : {e}")

def migrate_to_mongodb(batch_size=500):
    """Migre les données depuis MySQL vers MongoDB en parallèle avec optimisation."""
    mongo_db = create_mongo_client()

    for batch in fetch_songs_from_mysql(batch_size):
        print(f"Traitement d'un lot de {len(batch)} chansons")
        documents = []

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(process_song, song, {
                    "name": song["artist_name"],
                    "bio": song["artist_bio"],
                    "image_url": song["artist_image_url"]
                }): song for song in batch
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    documents.append(result)

        insert_into_mongodb(documents, mongo_db)

    print("Migration complète vers MongoDB !")

if __name__ == "__main__":
    migrate_to_mongodb(batch_size=500)
