import pymysql
import pymongo
import os
from dotenv import load_dotenv
from random import seed, sample
from datetime import date

# Charger les variables d'environnement
load_dotenv(dotenv_path="/opt/airflow/.env")
seed(42)  # Fixer une graine pour des résultats reproductibles

# Configuration MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Configuration MongoDB
MONGO_URI = "mongodb://mongodb-dlk:27017/"
MONGO_DATABASE = "datalakes_curated"

# Connexion à MySQL
mysql_connection = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    port=MYSQL_PORT,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

# Connexion à MongoDB
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DATABASE]

# Liste de mots courants à ne pas masquer
STOP_WORDS = {"le", "la", "les", "un", "une", "des", "et", "mais", "ou", "donc", "or", "ni", "car", 
              "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles", "de", "du", "en", 
              "à", "au", "aux", "par", "pour", "avec", "sans", "sous", "sur", "dans", "chez", "vers", 
              "entre", "comme"}


def generate_difficulty_versions(lyrics, easy_pct=10, medium_pct=25, hard_pct=40):
    """
    Génère trois versions de paroles (facile, intermédiaire, difficile) en masquant un pourcentage de mots.
    """
    if not lyrics:
        return {"easy": "Paroles indisponibles", "medium": "Paroles indisponibles", "hard": "Paroles indisponibles"}

    def mask_words(line, percentage):
        words = line.split()
        maskable_indices = [i for i, w in enumerate(words) if w.lower() not in STOP_WORDS]
        num_to_mask = max(1, int(len(maskable_indices) * percentage / 100)) if maskable_indices else 0
        mask_indices = sample(maskable_indices, min(len(maskable_indices), num_to_mask))
        return " ".join("____" if i in mask_indices else word for i, word in enumerate(words))

    easy = "\n".join(mask_words(line, easy_pct) for line in lyrics.splitlines())
    medium = "\n".join(mask_words(line, medium_pct) for line in lyrics.splitlines())
    hard = "\n".join(mask_words(line, hard_pct) for line in lyrics.splitlines())

    return {"easy": easy, "medium": medium, "hard": hard}


def migrate_to_mongodb():
    """
    Migre les données depuis MySQL vers MongoDB avec un document par chanson,
    incluant les données de l'artiste et les paroles traduites en français si disponibles.
    """
    with mysql_connection.cursor() as cursor:
        # Récupération des artistes
        cursor.execute("SELECT * FROM artists")
        artists = {artist["id"]: artist for artist in cursor.fetchall()}

        # Récupération des chansons
        cursor.execute("SELECT * FROM songs")
        songs = cursor.fetchall()

        for song in songs:
            artist = artists.get(song["artist_id"])
            if not artist:
                continue

            # Récupération des données de l'artiste
            artist_name = artist["name"]
            artist_bio = artist["bio"] if artist["bio"] else "Biographie non disponible"
            artist_image_url = artist["image_url"] if artist["image_url"] else ""

            # Récupération des données de la chanson
            lyrics = song.get("lyrics", "")
            french_lyrics = song.get("french_lyrics", "")
            difficulty_versions = generate_difficulty_versions(lyrics)

            # Création du document complet
            song_doc = {
                "title": song["title"],
                "artist": {
                    "name": artist_name,
                    "bio": artist_bio,
                    "image_url": artist_image_url
                },
                "url": song["url"],
                "image_url": song["image_url"],
                "language": song["language"],
                "release_date": str(song["release_date"]) if isinstance(song["release_date"], date) else song["release_date"],
                "pageviews": song["pageviews"],
                "lyrics": lyrics,
                "difficulty_versions": difficulty_versions,
                "french_lyrics": french_lyrics if french_lyrics else ""
            }

            mongo_db.songs.replace_one(
                {"title": song["title"], "artist.name": artist_name},
                song_doc,
                upsert=True
            )
            print(f"Chanson insérée : {song['title']} ({artist_name})")

    print("Migration complète vers MongoDB !")


if __name__ == "__main__":
    migrate_to_mongodb()
