from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import subprocess

# Configuration des arguments par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG optimisé
dag = DAG(
    'dag_songs_fast',
    default_args=default_args,
    description="DAG optimisé pour l'ingestion rapide avec Airflow",
    schedule_interval=None,
    catchup=False,
)

def run_upload_songs_to_s3_fast(**kwargs):
    """Appelle le script optimisé upload_songs_to_s3_fast.py"""
    songs = kwargs.get('dag_run').conf.get('songs', [])

    if not songs:
        raise ValueError("Aucune chanson fournie pour l'ingestion")

    song_args = [item for song in songs for item in [song["title"], song["artist"]]]

    subprocess.run(["python", "/opt/airflow/scripts/upload_songs_to_s3_fast.py"] + song_args, check=True)

def run_songs_raw_to_staging_fast():
    """Appelle le script optimisé songs_raw_to_staging_fast.py"""
    subprocess.run(["python", "/opt/airflow/scripts/songs_raw_to_staging_fast.py"], check=True)

def run_staging_to_curated_fast():
    """Appelle le script optimisé staging_to_curated_fast.py"""
    subprocess.run(["python", "/opt/airflow/scripts/staging_to_curated_fast.py"], check=True)

# Définition des tâches optimisées
task1 = PythonOperator(
    task_id='upload_songs_to_s3_fast',
    python_callable=run_upload_songs_to_s3_fast,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='songs_raw_to_staging_fast',
    python_callable=run_songs_raw_to_staging_fast,
    dag=dag,
)

task3 = PythonOperator(
    task_id='staging_to_curated_fast',
    python_callable=run_staging_to_curated_fast,
    dag=dag,
)

# Ordre d'exécution des tâches optimisées
task1 >> task2 >> task3
