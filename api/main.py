from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pymysql
import pymongo
import boto3
from dotenv import load_dotenv
import os
from pydantic import BaseModel
import requests
from typing import List

load_dotenv(dotenv_path="/opt/api/.env")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb-dlk:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "datalakes_curated")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

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

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL")
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ArtistRequest(BaseModel):
    artists: List[str]

@app.get("/")
async def root():
    return {"message": "Bienvenue sur l'API Gateway du projet DLK"}

@app.get("/raw")
async def get_raw_data():
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    files = [obj["Key"] for obj in response.get("Contents", [])]
    return {"raw_files": files}

@app.get("/raw/{artist_name}")
async def get_artist_data(artist_name: str):
    key = f"raw/{artist_name.replace(' ', '_').lower()}.json"
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        artist_data = response["Body"].read().decode("utf-8")
        return {"artist_data": artist_data}
    except Exception as e:
        return {"error": f"Artiste non trouvé ou erreur S3: {str(e)}"}

@app.get("/staging")
async def get_staging_data():
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT * FROM artists")
        artists = cursor.fetchall()
        cursor.execute("SELECT * FROM songs")
        songs = cursor.fetchall()
    return {"artists": artists, "songs": songs}

@app.get("/staging/artists")
async def get_staging_artists():
    """Liste les artistes stockés en MySQL."""
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT id, name, bio FROM artists")
        artists = cursor.fetchall()
    return {"artists": artists}

@app.get("/staging/songs")
async def get_staging_songs():
    """Liste les chansons stockées en MySQL."""
    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT id, artist_id, title, url, image_url, lyrics FROM songs")
        songs = cursor.fetchall()
    return {"songs": songs}

@app.get("/curated")
async def get_curated_data():
    songs = list(mongo_db.songs.find({}, {"_id": 0}))
    return {"curated_data": songs}

@app.get("/health")
async def get_health_status():
    service_status = {
        "mysql": {"status": "unhealthy", "details": None},
        "mongodb": {"status": "unhealthy", "details": None},
        "s3": {"status": "unhealthy", "details": None},
    }

    try:
        with mysql_connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        service_status["mysql"]["status"] = "healthy"
    except Exception as e:
        service_status["mysql"]["details"] = str(e)

    try:
        mongo_db.command("ping")
        service_status["mongodb"]["status"] = "healthy"
    except Exception as e:
        service_status["mongodb"]["details"] = str(e)

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        service_status["s3"]["status"] = "healthy"
    except Exception as e:
        service_status["s3"]["details"] = str(e)

    overall_status = "healthy" if all(service["status"] == "healthy" for service in service_status.values()) else "unhealthy"
    return {"status": overall_status, "services": service_status}

@app.get("/stats")
async def get_stats():
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix="raw/")
    s3_file_count = len(response.get("Contents", [])) if "Contents" in response else 0

    with mysql_connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) as count FROM artists")
        mysql_artists_count = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) as count FROM songs")
        mysql_songs_count = cursor.fetchone()["count"]

    mongo_songs_count = mongo_db.songs.count_documents({})
    mongo_artists_count = mongo_db.songs.distinct("artist")

    return {
        "s3_file_count": s3_file_count,
        "mysql": {"artists_count": mysql_artists_count, "songs_count": mysql_songs_count},
        "mongodb": {"artists_count": len(mongo_artists_count), "songs_count": mongo_songs_count},
    }

@app.post("/ingest")
async def ingest_data(request: ArtistRequest):
    if not request.artists:
        raise HTTPException(status_code=400, detail="Liste d'artistes vide")

    payload = {
        "conf": {"artists": request.artists}
    }
    auth = (AIRFLOW_USER, AIRFLOW_PASSWORD)
    headers = {"Content-Type": "application/json"}

    response = requests.post(AIRFLOW_API_URL, auth=auth, headers=headers, json=payload)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail=f"Erreur lors du déclenchement du DAG : {response.text}")

    return {"message": f"Pipeline déclenché avec succès pour {len(request.artists)} artistes."}
