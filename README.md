# Challenge Data Engineer - Artefact CI

Pipeline de donnees pour les ventes d'un site e-commerce de mode. Le projet couvre l'analyse exploratoire, la modelisation relationnelle (3FN/DKNF), l'implementation SQL, le deploiement Docker (PostgreSQL + Minio), un script d'ingestion Python idempotent, et l'orchestration Airflow 3.x.

## Architecture

```
CSV (Minio) --> Script Python --> PostgreSQL (11 tables DKNF) --> Vue etoile
                    |
              Airflow DAG (meme logique)
```

Le fichier CSV source est stocke dans un bucket Minio (`folder-source`). Le script d'ingestion lit le CSV, filtre par date, eclate les donnees en tables normalisees, et les charge dans PostgreSQL avec des upserts idempotents.

## Structure du projet

```
use_case_artefact/
├── README.md
├── .env.example
├── .gitignore
├── requirements.txt
├── data/                          Donnees sources
│   └── fashion_store_sales.csv
├── notebooks/                     Analyse exploratoire
│   └── 01_analyse_exploratoire.ipynb
├── docs/                          Documentation modelisation
│   └── modelisation.md
├── sql/                           Scripts SQL
│   ├── 01_create_dknf_tables.sql
│   └── 02_create_star_schema_view.sql
├── docker/                        Infrastructure
│   ├── docker-compose.yml
│   ├── postgres/init/             Init auto des tables PG
│   └── airflow/                   Image Airflow custom
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
└── tests/                         Tests (bonus)
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
- **PostgreSQL** (port 5432) avec les tables DKNF creees automatiquement
- **Minio** (API port 9000, console port 9001) avec le CSV uploade dans le bucket `folder-source`
- **Airflow 3.x** (UI port 8080) avec les connexions et variables pre-configurees

### 3. Verifier le deploiement

```bash
# Tables PostgreSQL creees
docker compose exec postgres psql -U fashion -d fashion_store -c "\dt"

# CSV present dans Minio
# Ouvrir http://localhost:9001 (minioadmin / minioadmin123)
```

### 4. Executer l'ingestion (script standalone)

```bash
docker compose exec airflow-scheduler python -m src.main 20250616
```

Le script :
- Lit le CSV depuis Minio
- Filtre les lignes du 2025-06-16
- Charge les donnees dans les 11 tables DKNF
- Est idempotent (relancable sans creer de doublons)

### 5. Executer via Airflow

1. Ouvrir http://localhost:8080 (admin / admin)
2. Activer le DAG `fashion_store_ingestion`
3. Trigger avec les parametres : `{"ingestion_date": "20250617"}`

### 6. Verifier les donnees

```bash
docker compose exec postgres psql -U fashion -d fashion_store -c \
  "SELECT count(*) FROM sale_items;"

docker compose exec postgres psql -U fashion -d fashion_store -c \
  "SELECT * FROM v_star_schema LIMIT 5;"
```

### 7. Arreter

```bash
docker compose down       # conserve les donnees
docker compose down -v    # reset complet
```

## Modele de donnees

### DKNF (11 tables)

Tables de reference : `countries`, `categories`, `brands`, `colors`, `sizes`, `age_ranges`, `channels`

Tables entites : `customers`, `products`, `sales`, `sale_items`

Les champs derives (unit_price, item_total, discount_percent, discounted, total_amount) ne sont pas stockes. Ils sont recalcules a la volee par la vue `v_star_schema`.

Voir [docs/modelisation.md](docs/modelisation.md) pour la justification complete des choix de normalisation.

## Choix techniques

| Choix | Justification |
|-------|--------------|
| 2 instances PostgreSQL | Separation donnees metier / metadata Airflow |
| DKNF plutot que 3FN seulement | Elimine les champs derives, enforce la qualite via le schema |
| ON CONFLICT pour l'idempotence | Relancable sans doublons ni erreurs |
| Transaction unique | Rollback atomique si une etape echoue |
| Minio pour le stockage | Simule S3 en local, realiste pour la production |
| Airflow Connections/Variables | Pas de credentials en dur dans le code du DAG |

## Technologies

- PostgreSQL 16
- Minio (S3-compatible)
- Docker / Docker Compose
- Python 3, pandas, boto3, psycopg2
- Apache Airflow 3.x
