from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sqlite3
import os

# SQLite database path
DB_PATH = "/home/somnath/coding/pyspark/sqlite_demo.db"


# Task 1: Create Table
def create_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            department TEXT,
            created_date TEXT
        )
    """)

    conn.commit()
    conn.close()

    print("Table created successfully")


# Task 2: Insert Data
def insert_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO employees (name, department, created_date)
        VALUES (?, ?, ?)
    """, ("Somnath", "Data Engineer", str(datetime.now())))

    conn.commit()
    conn.close()

    print("Data inserted successfully")


# Task 3: Read Data
def read_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM employees")
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    conn.close()


default_args = {
    "owner": "somnath"
}

with DAG(
    dag_id="sqlite_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["sqlite", "demo"]
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