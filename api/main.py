from fastapi import FastAPI
import pymysql
import pymongo
import boto3
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv(dotenv_path="/opt/api/.env")

# Configuration MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

# Configuration MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb-dlk:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "datalakes_curated")

# Configuration S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Initialiser les clients
mysql_connection = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    port=MYSQL_PORT,
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)
mongo_client = pymongo.MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DATABASE]
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

# Initialiser l'application FastAPI
app = FastAPI()


@app.get("/")
async def root():
    """Route par défaut."""
    return {"message": "Bienvenue sur l'API Gateway du projet DLK"}

@app.get("/raw")
async def get_raw_data():
    """Accès aux données brutes depuis S3"""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    files = [obj["Key"] for obj in response.get("Contents", [])]
    return {"raw_files": files}


@app.get("/staging")
async def get_staging_data():
    """Accès aux données intermédiaires depuis MySQL"""
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT * FROM datalakes_staging.songs")
        songs = cursor.fetchall()
    return {"staging_data": songs}


@app.get("/curated")
async def get_curated_data():
    """Accès aux données finales depuis MongoDB"""
    songs = list(mongo_db.songs.find({}, {"_id": 0}))  # Exclure `_id`
    return {"curated_data": songs}


@app.get("/health")
async def get_health_status():
    """Vérification de l'état des services (MySQL, MongoDB, S3)."""
    service_status = {
        "mysql": {"status": "unhealthy", "details": None},
        "mongodb": {"status": "unhealthy", "details": None},
        "s3": {"status": "unhealthy", "details": None},
    }

    try:
        # Vérification MySQL
        with mysql_connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        service_status["mysql"]["status"] = "healthy"
    except Exception as e:
        service_status["mysql"]["details"] = str(e)

    try:
        # Vérification MongoDB
        mongo_db.command("ping")
        service_status["mongodb"]["status"] = "healthy"
    except Exception as e:
        service_status["mongodb"]["details"] = str(e)

    try:
        # Vérification S3
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        service_status["s3"]["status"] = "healthy"
    except Exception as e:
        service_status["s3"]["details"] = str(e)

    # Déterminer l'état général
    overall_status = "healthy" if all(
        service["status"] == "healthy" for service in service_status.values()
    ) else "unhealthy"

    return {"status": overall_status, "services": service_status}

@app.get("/stats")
async def get_stats():
    """Métriques sur le remplissage des buckets et bases de données"""
    # Stats S3
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    s3_file_count = len(response.get("Contents", [])) if "Contents" in response else 0

    # Stats MySQL
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) as count FROM datalakes_staging.songs")
        mysql_count = cursor.fetchone()["count"]

    # Stats MongoDB
    mongo_count = mongo_db.songs.count_documents({})

    return {
        "s3_file_count": s3_file_count,
        "mysql_song_count": mysql_count,
        "mongo_song_count": mongo_count,
    }
