from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient
import logging

# -------------------------
# Mongo Connection
# -------------------------
def get_mongo_client():
    return MongoClient("mongodb://localhost:27017/")

# -------------------------
# Task 1: Read from MongoDB
# -------------------------
def read_from_mongo(**context):
    client = get_mongo_client()
    db = client["airflow_db"]
    collection = db["employees"]

    data = list(collection.find({}, {"_id": 0}))

    logging.info(f"Fetched {len(data)} records from MongoDB")

    context['ti'].xcom_push(key="mongo_data", value=data)

# -------------------------
# Task 2: Write to PostgreSQL
# -------------------------
def write_to_postgres(**context):
    data = context['ti'].xcom_pull(key="mongo_data")

    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Create table
    hook.run("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            name TEXT,
            department TEXT,
            created_date TIMESTAMP
        );
    """)

    # Insert data
    for row in data:
        hook.run("""
            INSERT INTO employees (name, department, created_date)
            VALUES (%s, %s, %s)
        """, parameters=(
            row.get("name"),
            row.get("department"),
            row.get("created_date")
        ))

    logging.info("Data inserted into PostgreSQL")

# -------------------------
# Task 3: Validate Data
# -------------------------
def validate_data(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")

    pg_count = hook.get_first("SELECT COUNT(*) FROM employees;")[0]

    client = get_mongo_client()
    mongo_count = client["airflow_db"]["employees"].count_documents({})

    result = {
        "mongo_count": mongo_count,
        "postgres_count": pg_count,
        "status": "MATCH" if mongo_count == pg_count else "MISMATCH"
    }

    logging.info(f"Validation Result: {result}")

    context['ti'].xcom_push(key="validation_result", value=result)

# -------------------------
# Task 4: Save Result in BOTH DBs
# -------------------------
def save_result(**context):
    result = context['ti'].xcom_pull(key="validation_result")

    # ---------- PostgreSQL ----------
    hook = PostgresHook(postgres_conn_id="postgres_default")

    hook.run("""
        CREATE TABLE IF NOT EXISTS validation_log (
            id SERIAL PRIMARY KEY,
            source_count INT,
            target_count INT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    hook.run("""
        INSERT INTO validation_log (source_count, target_count, status)
        VALUES (%s, %s, %s)
    """, parameters=(
        result['mongo_count'],
        result['postgres_count'],
        result['status']
    ))

    # ---------- MongoDB ----------
    client = get_mongo_client()
    db = client["airflow_db"]

    db["validation_log"].insert_one({
        "source_count": result['mongo_count'],
        "target_count": result['postgres_count'],
        "status": result['status'],
        "created_at": datetime.now()
    })

    logging.info("Validation result stored in BOTH MongoDB and PostgreSQL")

# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id="mongo_postgres_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mongo", "postgres", "etl", "validation"]
) as dag:

    t1 = PythonOperator(
        task_id="read_from_mongo",
        python_callable=read_from_mongo
    )

    t2 = PythonOperator(
        task_id="write_to_postgres",
        python_callable=write_to_postgres
    )

    t3 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    t4 = PythonOperator(
        task_id="save_result",
        python_callable=save_result
    )

    # Flow
    t1 >> t2 >> t3 >> t4