from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pymongo import MongoClient
import logging

# Mongo connection (local)
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "airflow_db"
COLLECTION = "employees"

def insert_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]

    doc = {
        "name": "Somnath",
        "department": "Data Engineering",
        "created_date": datetime.now()
    }

    col.insert_one(doc)
    logging.info("Inserted document")

def read_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION]

    docs = col.find()

    for d in docs:
        logging.info(d)

default_args = {
    "owner": "somnath"
}

with DAG(
    dag_id="mongo_demo_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["mongodb"]
) as dag:

    task1 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data
    )

    task2 = PythonOperator(
        task_id="read_data",
        python_callable=read_data
    )

    task1 >> task2