#!/bin/bash

echo "Attente que la base de données Airflow soit prête..."

# Attendre que la DB soit accessible
until airflow db check; do
    echo "Base non prête. Nouvelle vérification dans 5s..."
    sleep 5
done

# Modifier le fichier airflow.cfg uniquement pour le webserver
echo "Modification de airflow.cfg pour activer basic_auth..."
sed -i 's/auth_backends = airflow.api.auth.backend.session/auth_backends = airflow.api.auth.backend.basic_auth/' /opt/airflow/airflow.cfg

echo "Lancement de l'Airflow Webserver..."
exec airflow webserver
