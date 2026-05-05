from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logging.basicConfig(level=logging.INFO)

def create_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            name TEXT,
            department TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logging.info("Table created")

def insert_data():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run("""
        INSERT INTO employees (name, department)
        VALUES ('Somnath', 'Data Engineering');
    """)
    logging.info("Data inserted")

def read_data():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    records = hook.get_records("SELECT * FROM employees;")

    for row in records:
        logging.info(f"Row: {row}")

with DAG(
    dag_id="psql_hook_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["postgres"]
) as dag:

    task1 = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    task2 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    task3 = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )

    task1 >> task2 >> task3