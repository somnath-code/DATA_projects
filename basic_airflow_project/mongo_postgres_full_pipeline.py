from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

default_args = {
    "owner": "somnath",
    "retries": 1
}

# -------------------------
# Task 1: Read from MongoDB
# -------------------------
def read_from_mongo(**context):
    from pymongo import MongoClient

    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["airflow_db"]
        collection = db["employees"]

        data = list(collection.find({}, {"_id": 0}))

        if not data:
            raise ValueError("MongoDB collection is empty")

        # Convert datetime → string (XCom safe)
        for row in data:
            if "created_date" in row:
                row["created_date"] = str(row["created_date"])

        logging.info(f"Fetched {len(data)} records from MongoDB")

        context['ti'].xcom_push(key="mongo_data", value=data)

    except Exception as e:
        logging.error(f"Mongo Read Failed: {e}")
        raise


# -------------------------
# Task 2: Write to PostgreSQL
# -------------------------
def write_to_postgres(**context):
    data = context['ti'].xcom_pull(
        key="mongo_data",
        task_ids="read_mongo"   # ✅ FIXED
    )

    if not data:
        raise ValueError("No data received from MongoDB")

    hook = PostgresHook(postgres_conn_id="postgres_default")

    try:
        hook.run("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name TEXT,
                department TEXT,
                created_date TEXT
            );
        """)

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

    except Exception as e:
        logging.error(f"Postgres Write Failed: {e}")
        raise


# -------------------------
# Task 3: Validate Data
# -------------------------
def validate_data(**context):
    from pymongo import MongoClient

    hook = PostgresHook(postgres_conn_id="postgres_default")

    try:
        pg_count = hook.get_first("SELECT COUNT(*) FROM employees;")[0]

        client = MongoClient("mongodb://localhost:27017/")
        mongo_count = client["airflow_db"]["employees"].count_documents({})

        result = {
            "mongo_count": mongo_count,
            "postgres_count": pg_count,
            "status": "MATCH" if mongo_count == pg_count else "MISMATCH"
        }

        logging.info(f"Validation Result: {result}")

        context['ti'].xcom_push(key="validation_result", value=result)

    except Exception as e:
        logging.error(f"Validation Failed: {e}")
        raise


# -------------------------
# Task 4: Save Result in BOTH DBs
# -------------------------
def save_result(**context):
    from pymongo import MongoClient

    result = context['ti'].xcom_pull(
        key="validation_result",
        task_ids="validate"
    )

    if not result:
        raise ValueError("No validation result found")

    hook = PostgresHook(postgres_conn_id="postgres_default")

    try:
        # PostgreSQL table
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
            result["mongo_count"],
            result["postgres_count"],
            result["status"]
        ))

        # MongoDB collection
        client = MongoClient("mongodb://localhost:27017/")
        db = client["airflow_db"]

        db["validation_log"].insert_one({
            "source_count": result["mongo_count"],
            "target_count": result["postgres_count"],
            "status": result["status"],
            "created_at": datetime.now()
        })

        logging.info("Validation result saved in BOTH MongoDB and PostgreSQL")

    except Exception as e:
        logging.error(f"Save Result Failed: {e}")
        raise


# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="mongo_postgres_pipeline_final",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["mongo", "postgres", "etl"]
) as dag:

    t1 = PythonOperator(
        task_id="read_mongo",
        python_callable=read_from_mongo
    )

    t2 = PythonOperator(
        task_id="write_postgres",
        python_callable=write_to_postgres
    )

    t3 = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )

    t4 = PythonOperator(
        task_id="save_result",
        python_callable=save_result
    )

    # DAG FLOW
    t1 >> t2 >> t3 >> t4