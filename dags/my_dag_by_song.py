from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
import subprocess

# Chemin des scripts dans le conteneur
SCRIPT_PATH = "/opt/airflow/scripts"

def run_upload_song_to_s3(**kwargs):
    """Appelle le script upload_song_to_s3.py en passant les titres depuis la config."""
    songs = kwargs.get('dag_run').conf.get('songs', [])
    if not songs:
        raise ValueError("Aucune chanson fournie pour l'ingestion")
    subprocess.run(["python", f"{SCRIPT_PATH}/upload_song_to_s3.py"] + songs, check=True)

def run_raw_song_to_mysql():
    """Appelle le script raw_song_to_mysql.py"""
    subprocess.run(["python", f"{SCRIPT_PATH}/raw_song_to_mysql.py"], check=True)

def run_staging_to_curated():
    """Appelle le script staging_to_curated.py"""
    subprocess.run(["python", f"{SCRIPT_PATH}/staging_to_curated.py"], check=True)

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag_by_song',
    default_args=default_args,
    description='DAG pour orchestrer les scripts S3, MySQL et MongoDB par musique',
    schedule_interval=None,
    catchup=False,
)

# Définition des tâches
task_song_s3 = PythonOperator(
    task_id='upload_song_to_s3',
    python_callable=run_upload_song_to_s3,
    provide_context=True,
    dag=dag,
)

task_song_mysql = PythonOperator(
    task_id='raw_song_to_mysql',
    python_callable=run_raw_song_to_mysql,
    dag=dag,
)

task_song_mongo = PythonOperator(
    task_id='staging_to_curated',
    python_callable=run_staging_to_curated,
    dag=dag,
)

# Ordre d'exécution des tâches
task_song_s3 >> task_song_mysql >> task_song_mongo
