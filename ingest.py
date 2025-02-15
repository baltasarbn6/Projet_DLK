import requests
from requests.auth import HTTPBasicAuth

# URL de l'API Airflow
url = "http://localhost:8080/api/v1/dags/my_dag/dagRuns"

# Données à envoyer
data = {
    "conf": {
        "artists": ["Daniel Balavoine", "Jean-Jacques Goldman", "Christophe Maé"]
    }
}

# Authentification avec Basic Auth
auth = HTTPBasicAuth("airflow", "airflow")

# Envoi de la requête POST
response = requests.post(url, json=data, auth=auth)

# Affichage de la réponse
print(f"Statut HTTP: {response.status_code}")
print("Réponse:", response.json())
