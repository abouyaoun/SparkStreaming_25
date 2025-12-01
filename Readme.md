# SparkStreaming_25

Projet complet de **streaming temps réel** combinant **Kafka**, **Spark Structured Streaming**, **Scala**, **Docker**, et une interface de visualisation **Streamlit** connectée à PostgreSQL.
Ce projet illustre la création d’un pipeline data moderne et robuste, de la génération de données jusqu’à la visualisation finale.

---

## 🎯 Objectifs du projet

* Mettre en place un **producer Kafka** en Scala capable d’envoyer un batch de données toutes les X secondes.
* Développer un **consumer Spark Structured Streaming** récupérant les messages Kafka en temps réel.
* Stocker les données transformées dans une base **PostgreSQL** via un sink géré par Spark.
* Créer une interface de visualisation **Streamlit** connectée à PostgreSQL.
* Packager l’ensemble dans une architecture **Docker** reproductible et facilement déployable.
* Illustrer un pipeline de data engineering opérationnel de bout en bout.

---

## 📦 Architecture globale

```
Producer (Scala) ──> Kafka ──> Spark Structured Streaming (Scala)
                                   │
                                   ▼
                           PostgreSQL (DB)
                                   │
                                   ▼
                          Streamlit (Python)
```

Les différents modules du dépôt :

* `producer/` : producer Kafka écrit en Scala (SBT + jar).
* `consumer/` : consumer Spark Structured Streaming avec parsing + batch processing.
* `streamlit/` : tableau de bord pour visualiser les données entrantes.
* `init/` : initialisation de la base PostgreSQL + scripts.
* `docker-compose.yml` : orchestration complète (Kafka, Zookeeper, Spark, PostgreSQL, Streamlit).

---

## 🧰 Compétences mobilisées

### 🔹 **Data Engineering**

* Traitement temps réel (Spark Structured Streaming)
* Gestion de flux Kafka (topics, partitionnement, offsets)
* Orchestration multi-conteneur avec Docker
* Stockage et ingestion de données dans une base relationnelle
* Architecture distribuée et scalabilité

### 🔹 **Développement Scala**

* Structure de projet SBT
* Compilation, assembly et génération de JAR
* Manipulation d’objets JSON, parsing, sérialisation
* Création de modules séparés : `model`, `utils`, `processing`, `ConsumerApp`

### 🔹 **DevOps / Conteneurisation**

* Dockerfile, volumes, networks
* docker-compose pour orchestrer plusieurs services
* Débogage des conteneurs, logs, connexions inter-services

### 🔹 **Visualisation & Backend Python**

* Interface Streamlit responsive
* Connexion à PostgreSQL via SQLAlchemy / psycopg2
* Affichage dynamique de données temps réel

### 🔹 **Compétences transverses**

* Architecture logicielle modulaire
* Gestion de projet (versioning, branches, merges)
* Collaboration sur GitHub

---

## 📘 Attentes du projet

* Construire un pipeline **fonctionnel, stable et modulaire**.
* Écrire un code clair, découpé, et maintenable.
* Produire un consumer Spark propre, réparti en 4 modules :

  * `model/StockData.scala`
  * `processing/BatchProcessor.scala`
  * `utils/MessageParser.scala`
  * `ConsumerApp.scala`
* Utiliser Kafka comme couche de streaming fiable.
* Fournir une interface utilisateur permettant de **visualiser et analyser les données en temps réel**.
* Démontrer une compréhension complète de la chaîne Data Engineering moderne.

---

## 🚀 Installation & Exécution

### 1. Builder le producer et consumer

```bash
cd producer
sbt clean compile assembly

cd ../consumer
sbt clean compile assembly
```

### 2. Lancer l’infrastructure Docker

```bash
cd ..
docker compose build
docker compose up
```

> Si certains services ne démarrent pas :
> ouvrez l’interface Docker Desktop → sélectionnez le service → **Start**.

---

## 🔄 Rebuild complet (en cas de modifs Consumer / Producer)

```bash
docker volume prune --all

cd consumer
sbt clean compile assembly

cd ../producer
sbt clean compile assembly

docker compose up --build
```

---

## 🎨 Lancer l'interface Streamlit

```bash
cd streamlit
source venv/bin/activate
python -m streamlit run app.py
```

Accessible sur :
👉 [http://localhost:8501/](http://localhost:8501/)

---

## 👥 Contributeurs

* **Zameloth – Théo ELOY**
* **abouyaoun – Ayman**
* **MatyDiop – Maty Diop**

---

## 📄 Licence

Projet éducatif / démonstration.
Réutilisation libre avec attribution.

---

## 📫 Contact

Pour toute question, amélioration ou suggestion :
👉 Ouvrir une *Issue* ou *Pull Request* sur GitHub.

























cd ../producer
sbt clean compile assembly

cd ..
docker compose build
docker compose up

(services - selectionner element n'ayant pas start et appuyer sur start)


--modif consumer

docker volume prune --all
cd consumer
sbt clean compile assembly
cd ../producer
sbt clean compile assembly
docker compose up --build

--streamlit


cd streamlit
source venv/bin/activate
python -m streamlit run app.py

http://localhost:8501
