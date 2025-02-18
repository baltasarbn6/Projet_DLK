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
STOP_WORDS = {"le", "la", "les", "un", "une", "des", "et", "mais", "ou", "donc", "or", "ni", "car", "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles", "de", "du", "en", "à", "au", "aux", "par", "pour", "avec", "sans", "sous", "sur", "dans", "chez", "vers", "entre", "comme"}


def generate_difficulty_versions(lyrics, easy_pct=10, medium_pct=25, hard_pct=40):
    """
    Génère trois versions de paroles (facile, intermédiaire, difficile) en masquant un pourcentage de mots,
    tout en excluant les mots courants définis dans STOP_WORDS.
    """
    if not lyrics:
        return {"easy": "Paroles indisponibles", "medium": "Paroles indisponibles", "hard": "Paroles indisponibles"}

    def mask_words_in_line(line, percentage):
        words = line.split()
        if not words:
            return line
        maskable_indices = [i for i, w in enumerate(words) if w.lower() not in STOP_WORDS]
        num_to_mask = max(1, int(len(maskable_indices) * percentage / 100)) if maskable_indices else 0
        mask_indices = sample(maskable_indices, min(len(maskable_indices), num_to_mask))
        return " ".join("____" if i in mask_indices else word for i, word in enumerate(words))

    easy = "\n".join(mask_words_in_line(line, easy_pct) for line in lyrics.splitlines())
    medium = "\n".join(mask_words_in_line(line, medium_pct) for line in lyrics.splitlines())
    hard = "\n".join(mask_words_in_line(line, hard_pct) for line in lyrics.splitlines())

    return {"easy": easy, "medium": medium, "hard": hard}


def migrate_to_mongodb():
    """
    Migre les données depuis MySQL vers MongoDB avec un document par chanson,
    en incluant les paroles traduites en français si disponibles.
    """
    with mysql_connection.cursor() as cursor:
        # Récupérer les artistes
        cursor.execute("SELECT * FROM artists")
        artists = {artist["id"]: artist for artist in cursor.fetchall()}

        # Récupérer les chansons avec leurs paroles originales et traduites
        cursor.execute("SELECT * FROM songs")
        songs = cursor.fetchall()

        for song in songs:
            artist = artists.get(song["artist_id"])
            if not artist:
                continue

            artist_name = artist["name"]
            lyrics = song.get("lyrics", "")
            french_lyrics = song.get("french_lyrics", "")

            # Générer les versions de difficulté pour les paroles originales
            difficulty_versions = generate_difficulty_versions(lyrics)

            # Créer le document MongoDB
            song_doc = {
                "title": song["title"],
                "artist": artist_name,
                "url": song["url"],
                "image_url": song["image_url"],
                "language": song["language"],
                "release_date": str(song["release_date"]) if isinstance(song["release_date"], date) else song["release_date"],
                "pageviews": song["pageviews"],
                "lyrics": lyrics,
                "difficulty_versions": difficulty_versions,
                "french_lyrics": french_lyrics if french_lyrics else ""
            }

            # Insérer ou mettre à jour la chanson dans MongoDB
            mongo_db.songs.replace_one({"title": song["title"], "artist": artist_name}, song_doc, upsert=True)
            print(f"Chanson insérée : {song['title']} ({artist_name})")

    print("Migration vers MongoDB terminée !")


if __name__ == "__main__":
    migrate_to_mongodb()
