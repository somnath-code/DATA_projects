import sqlite3
import psycopg2
from datetime import datetime

# Source and Destination configs
SQLITE_PATH = "/home/somnath/coding/pyspark/sqlite_demo.db"
POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "airflow_db",
    "user": "airflow_user",
    "password": "110897",
    "port": "5432"
}

def migrate_data():
    try:
        # 1. Connect to SQLite and fetch data
        s_conn = sqlite3.connect(SQLITE_PATH)
        s_cursor = s_conn.cursor()
        s_cursor.execute("SELECT name, department, created_date FROM employees")
        rows = s_cursor.fetchall()
        s_conn.close()

        # 2. Connect to PostgreSQL and insert data
        p_conn = psycopg2.connect(**POSTGRES_CONFIG)
        p_cursor = p_conn.cursor()
        
        # Using executemany for efficiency
        insert_query = """
            INSERT INTO employees (name, department, created_date) 
            VALUES (%s, %s, %s)
        """
        p_cursor.executemany(insert_query, rows)
        
        p_conn.commit()
        print(f"Successfully migrated {len(rows)} rows to PostgreSQL.")
        
        p_cursor.close()
        p_conn.close()

    except Exception as e:
        print(f"Error during migration: {e}")

if __name__ == "__main__":
    migrate_data()