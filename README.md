# 🚀 MySQL to PostgreSQL ETL Pipeline with Apache Airflow

---

The pipeline is orchestrated by **Apache Airflow DAGs**, where extraction, transformation, and loading are split into modular tasks.

---

# 🛠️ Tech Stack

* **Python 3**
* **Apache Airflow**
* **MySQL**
* **PostgreSQL**
* **Docker / Docker Compose**
* **Faker**
* **SQL**

---

# 📂 Project Structure

```bash
.
├── dags/
│   └── mysql_to_postgres_dag.py      # Main Airflow DAG
│
├── plugins/
│   └── helpers/
│       ├── constants.py              # MySQL → PostgreSQL type map
│       ├── transformation.py         # Schema + row transformation logic
│       └── checks.py                 # Validation utilities
│
├── generator/
│   ├── models/
│   │   └── person.py                 # Fake source data model
│   └── ...                           # Source data generator service
│
├── scripts/
│   └── sql/
│       └── mysql_init.sql            # MySQL bootstrap SQL
│
├── docker-compose.yaml               # Full local infrastructure
└── README.md
```

---

## DAG stages

```text
Read Schema
    ↓
Create Target Table
    ↓
Evaluate Batches
    ↓
Extract Batches (dynamic mapping)
    ↓
Transform Rows (dynamic mapping)
    ↓
Load Batches (dynamic mapping)
```

# 🐳 Run Locally

## 1) Clone repository

```bash
git clone https://github.com/JDidyk19/pet_project_mysql_to_postgres_etl.git
cd pet_project_mysql_to_postgres_etl
```

## 2) Start infrastructure

```bash
docker compose up --build
```

This starts:

* Airflow webserver
* Airflow scheduler
* MySQL
* PostgreSQL
* fake data generator service

---

## 3) Open Airflow UI

```text
http://localhost:8080
```
