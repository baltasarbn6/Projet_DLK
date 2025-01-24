import pymysql
import pymongo
import os
from dotenv import load_dotenv
from random import seed, sample

# Charger les variables d'environnement
load_dotenv()
seed(42)  # Fixer une graine pour des résultats reproductibles

# Configuration MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Configuration MongoDB
MONGO_URI = "mongodb://localhost:27017/"
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

def generate_difficulty_versions(lyrics, easy_pct=10, medium_pct=25, hard_pct=40):
    """
    Génère trois versions de paroles (facile, intermédiaire, difficile) en masquant un pourcentage de mots.
    """
    def mask_words(words, percentage):
        total_words = len(words)
        num_to_mask = max(1, int(total_words * percentage / 100))
        mask_indices = sample(range(total_words), num_to_mask)
        return [
            "____" if i in mask_indices else word for i, word in enumerate(words)
        ]

    # Diviser les paroles en mots
    words = lyrics.split()

    # Générer les versions pour chaque niveau
    return {
        "easy": " ".join(mask_words(words, easy_pct)),
        "medium": " ".join(mask_words(words, medium_pct)),
        "hard": " ".join(mask_words(words, hard_pct)),
    }

def migrate_to_mongodb():
    """
    Migre les données depuis MySQL vers MongoDB avec un document par chanson.
    """
    with mysql_connection.cursor() as cursor:
        # Récupérer tous les artistes
        cursor.execute("SELECT * FROM artists")
        artists = cursor.fetchall()

        for artist in artists:
            artist_id = artist["id"]
            artist_name = artist["name"]

            # Récupérer toutes les chansons de l'artiste
            cursor.execute("SELECT * FROM songs WHERE artist_id = %s", (artist_id,))
            songs = cursor.fetchall()

            for song in songs:
                lyrics = song["lyrics"]
                difficulty_versions = generate_difficulty_versions(lyrics)

                song_doc = {
                    "title": song["title"],
                    "artist": artist_name,
                    "url": song["url"],
                    "image_key": song["s3_image_key"],
                    "lyrics": lyrics,
                    "difficulty_versions": difficulty_versions,
                }

                # Insérer la chanson comme un document dans MongoDB
                mongo_db.songs.replace_one({"title": song["title"], "artist": artist_name}, song_doc, upsert=True)
                print(f"✅ Chanson insérée : {song['title']} ({artist_name})")

if __name__ == "__main__":
    migrate_to_mongodb()
