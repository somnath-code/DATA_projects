from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import psycopg2  # Switched from sqlite3
import os

# PostgreSQL connection details
# Replace these with your actual local PostgreSQL credentials
DB_CONFIG = {
    "host": "localhost",
    "database": "airflow_db",
    "user": "airflow_user",
    "password": "110897",
    "port": "5432"
}

# Task 1: Create Table
def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Note: PostgreSQL uses SERIAL or BIGSERIAL for auto-increment instead of AUTOINCREMENT
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            name TEXT,
            department TEXT,
            created_date TIMESTAMP
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("Table created successfully in PostgreSQL")

# Task 2: Insert Data
def insert_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # PostgreSQL uses %s as a placeholder instead of ?
    cursor.execute("""
        INSERT INTO employees (name, department, created_date)
        VALUES (%s, %s, %s)
    """, ("Somnath", "Data Engineering", datetime.now()))

    conn.commit()
    cursor.close()
    conn.close()
    print("Data inserted successfully")

# Task 3: Read Data
def read_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    cursor.close()
    conn.close()

default_args = {
    "owner": "somnath"
}

with DAG(
    dag_id="psql_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["postgres", "demo"]
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