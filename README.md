# Projet_DLK

## Description

**Projet_DLK** est une solution complète d'ingestion, de transformation et de stockage de données pour les artistes et chansons à partir de l'API Genius. L'architecture repose sur plusieurs étapes de traitement orchestrées par Apache Airflow, avec un stockage des données dans S3, MySQL et MongoDB.

---

## Architecture

Le projet utilise trois principales couches de stockage :

- **S3 (Raw Layer)** : Stockage brut des données JSON récupérées depuis l'API Genius.
- **MySQL (Staging Layer)** : Stockage intermédiaire permettant des transformations et nettoyages avant la finalisation.
- **MongoDB (Curated Layer)** : Stockage final optimisé pour la consommation de données.

### Flux de données

1. **Ingestion des données brutes** : Les chansons et artistes spécifiés sont téléchargés depuis l'API Genius et stockés sous forme de fichiers JSON dans S3.
2. **Transformation vers MySQL** : Les données brutes sont extraites de S3, transformées et chargées dans la base de données MySQL.
3. **Enrichissement et migration vers MongoDB** : Les données depuis MySQL sont enrichies (paroles avec différents niveaux de difficulté, métadonnées) puis migrées vers MongoDB.

---

## Lancement du projet

### Prérequis

- **Docker & Docker Compose** : Assurez-vous que Docker est installé.
- **Node.js & npm** : Pour lancer le frontend React.

### Étapes

1. **Cloner le dépôt :**
```bash
git clone https://github.com/baltasarbn6/Projet_DLK.git
cd Projet_DLK
```
2. **Copier les variables d'environnement :**

Assurez-vous que le fichier `.env` est présent à la **racine** du projet ainsi que dans le dossier **/api**.

3. **Démarrer les services Docker :**
```bash
docker-compose up --build
```

4. **Installer les dépendances du frontend :**
```bash
cd frontend
npm install
```

5. **Lancer le frontend en mode développement :**
```bash
npm run dev
```

### Accès aux services

- **Airflow UI :** `http://localhost:8080`
- **API Gateway :** `http://localhost:8000`
- **Frontend :** `http://localhost:5173`

---

## Endpoints de l'API

### **Récupération des données brutes depuis S3 :**
```http
GET http://localhost:8000/raw

GET http://localhost:8000/raw/artist/{artist_name}

GET http://localhost:8000/raw/song/{artist_name}/{song_title}
```

### **Récupération des données en staging depuis MySQL :**
```http
GET http://localhost:8000/staging

GET http://localhost:8000/staging/artists

GET http://localhost:8000/staging/songs
```

### **Récupération des données en curated depuis MongoDB :**
```http
GET http://localhost:8000/curated
```

### **Vérification de la santé des services :**
```http
GET http://localhost:8000/health
```

### **Statistiques sur les données stockées :**
```http
GET http://localhost:8000/stats
```

---

## Ingestion des données via Airflow

### **Ingestion par artistes :**
```bash
curl -X POST http://localhost:8000/ingest -H 'Content-Type: application/json' -d '{"artists": ["artist1", "artist2", ...]}'
```

### **Ingestion par chansons :**
```bash
curl -X POST http://localhost:8000/ingest_songs -H 'Content-Type: application/json' -d '{"songs": [{"title": "title1", "artist": "artist1"}, {"title": "title2", "artist": "artist2"}, ...]}'
```

### **Ingestion optimisée par artistes :**
```bash
curl -X POST http://localhost:8000/ingest_fast -H 'Content-Type: application/json' -d '{"artists": ["artist1", "artist2", ...]}'
```

### **Ingestion optimisée par chansons :**
```bash
curl -X POST http://localhost:8000/ingest_songs_fast -H 'Content-Type: application/json' -d '{"songs": [{"title": "title1", "artist": "artist1"}, {"title": "title2", "artist": "artist2"}, ...]}'
```

---

## Explication des DAGs dans Airflow

### **DAGs classiques :**
- **dag_artists** : Ingestion des artistes spécifiés via `/ingest`, avec un traitement séquentiel entre les étapes S3, MySQL et MongoDB.
- **dag_songs** : Ingestion des chansons spécifiées via `/ingest_songs`, en suivant le même flux séquentiel.

### **DAGs optimisés :**
- **dag_artists_fast** : Utilise des techniques de parallélisation (`ThreadPoolExecutor`), traitement par lot (`batching`) et optimisation des connexions pour accélérer l'ingestion des artistes.
- **dag_songs_fast** : Applique les mêmes techniques d'optimisation pour l'ingestion rapide des chansons, avec un gain de performance attendu de plus de 30%.

---

## Remarques
- Assurez-vous de remplir le fichier `.env` avec les bonnes informations (clés API, accès aux bases de données).
- Avant chaque nouvelle exécution, vérifiez que tous les conteneurs Docker sont bien opérationnels avec `docker ps`.
- Il faut attendre la fin de démarrage du webserver de Airflow avant de démarrer les ingestions.
- Pour voir ce qui n'a pas été trouvé dans Genius (peut-être un problème d'écriture d'un artiste ou d'une chanson), il faut remonter les logs du Scheduler.
