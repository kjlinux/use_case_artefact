# Challenge Data Engineer - Artefact CI

Pipeline de données pour les ventes d'un site e-commerce de mode. Le projet couvre l'analyse exploratoire, la modélisation relationnelle (3FN/DKNF), l'implémentation SQL, le déploiement Docker (PostgreSQL + Minio), un script d'ingestion Python idempotent, et l'orchestration Airflow 3.

## Architecture

```
CSV (Minio) --> Script Python --> PostgreSQL (11 tables DKNF) --> Vue étoile
                    |
              Airflow DAG (meme logique)
```

Le fichier CSV source est stocké dans un bucket Minio (`folder-source`). Le script d'ingestion lit le CSV, filtre par date, éclate les données en tables normalisées, et les charge dans PostgreSQL.

## Structure du projet

```
use_case_artefact/
├── README.md
├── .env.example
├── .gitignore
├── requirements.txt
├── data/                          Données sources
│   └── fashion_store_sales.csv
├── notebooks/                     Analyse exploratoire
│   └── 01_analyse_exploratoire.ipynb
├── docs/                          Documentation modélisation
│   └── modelisation.md
├── sql/                           Scripts SQL
│   ├── 01_create_dknf_tables.sql
│   └── 02_create_star_schema_view.sql
├── docker/                        Infrastructure
│   ├── docker-compose.yml
│   ├── postgres/init/             Init automatique des tables PG
│   └── airflow/                   Image Airflow
├── src/                           Code d'ingestion
│   ├── main.py
│   ├── ingestion/
│   │   ├── minio_client.py
│   │   ├── transformer.py
│   │   └── postgres_loader.py
│   └── utils/
│       └── logger.py
├── dags/                          DAG Airflow
│   └── dag_ingestion.py
```

## Pre-requis

- Docker et Docker Compose
- Python 3.10+
- Git

## Lancement rapide

### 1. Configurer l'environnement

```bash
cp .env.example .env
```

### 2. Lancer les services

```bash
cd docker
docker compose up -d
```

Cela demarre :

- **PostgreSQL** (port 5432) avec les tables DKNF créées automatiquement
- **Minio** (API port 9000, console port 9001) avec le CSV uploade dans le bucket `folder-source`
- **Airflow 3** (UI port 8080) avec les connexions et variables pré-configurées

### 3. Verifier le déploiement

```bash
# Tables PostgreSQL créées
docker compose exec postgres psql -U fashion -d fashion_store -c "\dt"

Ouvrir http://localhost:9001 (minioadmin / minioadmin123)
```

### 4. Exécuter l'ingestion (script standalone)

```bash
docker compose exec airflow-scheduler python -m src.main 20250616
```

Le script :

- Lit le CSV depuis Minio
- Filtre les lignes du 2025-06-16
- Charge les données dans les 11 tables DKNF
- Est idempotent (relancable sans créer de doublons)

### 5. Executer via Airflow

1. Ouvrir http://localhost:8080 (admin / admin)
2. Activer le DAG `fashion_store_ingestion`
3. Trigger avec les parametres : `{"ingestion_date": "20250617"}`

### 6. Verifier les données

```bash
docker compose exec postgres psql -U fashion -d fashion_store -c \
  "SELECT count(*) FROM sale_items;"

docker compose exec postgres psql -U fashion -d fashion_store -c \
  "SELECT * FROM v_star_schema LIMIT 5;"
```

### 7. Arrêter

```bash
docker compose down
docker compose down -v    # reset complet
```

## Modèle de données

### DKNF (11 tables)

Tables de référence : `countries`, `categories`, `brands`, `colors`, `sizes`, `age_ranges`, `channels`

Tables entités : `customers`, `products`, `sales`, `sale_items`

Les champs dérivés (unit_price, item_total, discount_percent, discounted, total_amount) ne sont pas stockés. Ils sont recalculés à la volée par la vue `v_star_schema`.

Voir [docs/modelisation.md](docs/modelisation.md) pour la justification complète des choix de normalisation.

## Technologies

- PostgreSQL 16
- Minio (S3-compatible)
- Docker / Docker Compose
- Python 3, pandas, boto3, psycopg2
- Apache Airflow 3
