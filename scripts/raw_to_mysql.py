import boto3
import pymysql
from dotenv import load_dotenv
import os
import re

# Charger les variables d'environnement
load_dotenv()

# Configuration S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Configuration MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))

# Initialisation des clients S3 et MySQL
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
    """
    Liste tous les fichiers dans le dossier 'raw/' du bucket S3.
    """
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith("_popular_songs.txt")]
    return files


def clean_lyrics(raw_lyrics):
    """
    Nettoie les paroles :
    - Supprime le préfixe 'Paroles :' s'il est présent.
    - Supprime les balises comme [Couplet 1], [Refrain], etc.
    - Combine les lignes qui appartiennent à la même phrase.
    - Conserve les dialogues avec les tirets.
    """
    # Supprimer le préfixe 'Paroles :'
    raw_lyrics = re.sub(r"^Paroles\s*:", "", raw_lyrics)

    # Supprimer les balises comme [Couplet 1], [Refrain], etc.
    raw_lyrics = re.sub(r"\[.*?\]", "", raw_lyrics)

    cleaned_lyrics = []
    temp_line = ""

    for line in raw_lyrics.split("\n"):
        line = line.strip()  # Supprimer les espaces en début et fin de ligne

        if not line:
            # Ajouter une séparation de paragraphe si une ligne vide est rencontrée
            if temp_line:
                cleaned_lyrics.append(temp_line.strip())
                temp_line = ""
            continue

        # Si la ligne commence par un tiret (dialogue) ou une majuscule, elle doit être séparée
        if line.startswith("-") or (temp_line and line[0].isupper() and not temp_line.endswith((".", ":", "?"))):
            cleaned_lyrics.append(temp_line.strip())
            temp_line = line
        else:
            # Combiner la ligne avec la précédente
            temp_line += " " + line

    # Ajouter la dernière ligne
    if temp_line:
        cleaned_lyrics.append(temp_line.strip())

    return "\n".join(cleaned_lyrics)


def process_file(file_key):
    """
    Traite un fichier Raw depuis S3 et insère les données dans MySQL.
    """
    # Télécharger le fichier depuis S3
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = response["Body"].read().decode("utf-8")

    # Extraire les données du fichier
    lines = content.split("\n")
    artist_name = lines[1].split(": ", 1)[1].strip()
    bio = lines[2].split(": ", 1)[1].strip()
    artist_image_key = lines[3].split(": ", 1)[1].strip()

    # Insérer l'artiste dans MySQL
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO artists (name, bio, s3_image_key) VALUES (%s, %s, %s)",
            (artist_name, bio, artist_image_key),
        )
        artist_id = cursor.lastrowid

    # Extraire et insérer les chansons
    i = 5  # Commencer après "Chansons populaires :"
    while i < len(lines):
        try:
            # Vérifier si la ligne contient une chanson
            if lines[i].startswith("- ") and "(URL :" in lines[i]:
                # Titre de la chanson
                title = lines[i].split("(URL")[0].replace("- ", "").strip()

                # URL de la chanson
                url = lines[i].split("URL : ")[1].split(")")[0].strip()

                # Clé S3 de l'image de la chanson
                image_key = lines[i + 1].split(": ", 1)[1].strip()

                # Paroles brutes
                lyrics_start = i + 2
                raw_lyrics = []
                while lyrics_start < len(lines) and not lines[lyrics_start].startswith("- "):
                    raw_lyrics.append(lines[lyrics_start])
                    lyrics_start += 1

                raw_lyrics = "\n".join(raw_lyrics).strip()
                cleaned_lyrics = clean_lyrics(raw_lyrics)  # Nettoyer les paroles

                # Insérer la chanson dans MySQL
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO songs (artist_id, title, url, s3_image_key, lyrics) VALUES (%s, %s, %s, %s, %s)",
                        (artist_id, title, url, image_key, cleaned_lyrics),
                    )

                i = lyrics_start  # Passer à la ligne suivante
            else:
                # Ignorer les lignes mal formatées
                print(f"⚠️ Ligne ignorée : {lines[i]}")
                i += 1
        except Exception as e:
            print(f"❌ Erreur lors du traitement de la chanson à la ligne {i}: {lines[i]}")
            print(e)
            i += 1

    # Valider les transactions
    connection.commit()
    print(f"✅ Données insérées pour l'artiste : {artist_name}")


def process_all_files():
    """
    Traite tous les fichiers Raw depuis S3 et insère les données dans MySQL.
    """
    files = list_raw_files()
    print(f"Fichiers trouvés : {len(files)}")
    for file_key in files:
        print(f"Traitement du fichier : {file_key}")
        process_file(file_key)


if __name__ == "__main__":
    process_all_files()


