import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime

# --- Configuration ---
# NOTE: These sensitive values are passed directly to the script via spark-submit,
# or better yet, read from a secure source like Airflow Connections/Vault/etc.
DB_URL = "jdbc:postgresql://localhost:5432/spark_db"
DB_TABLE = "integration_results"
DB_USER = "spark_user"
DB_PASSWORD = os.getenv("PG_PASS", "your_default_password_if_not_set") # Secure way to get password

# --- Create Spark Session ---
# Spark will automatically find the PostgreSQL driver because we pass the 
# --jars or --packages flag during spark-submit.
spark = SparkSession.builder.appName("PostgresSparkETL").getOrCreate()

# Set log level to reduce console clutter
spark.sparkContext.setLogLevel("WARN")

try:
    print(f"--- Running PySpark Job at {datetime.now()} ---")

    # 1. Define Schema and Data
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("data_source", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])

    data = [
        (1, "Spark-Airflow Run 1", str(datetime.now())),
        (2, "Spark-Airflow Run 2", str(datetime.now()))
    ]

    # 2. Create DataFrame
    df = spark.createDataFrame(data, schema)
    print("DataFrame to be written:")
    df.show()

    # 3. Write Data to PostgreSQL
    df.write.format("jdbc").options(
        url=DB_URL,
        dbtable=DB_TABLE,
        user=DB_USER,
        password=DB_PASSWORD,
        driver="org.postgresql.Driver"
    ).mode("append").save()

    print(f"\n--- Successfully Wrote {df.count()} records to {DB_TABLE} ---")

except Exception as e:
    print(f"\n!!! PySpark Job Failed: {e} !!!")
    raise e
    
finally:
    spark.stop()
    print("Spark Session stopped.")

    #`--- End of PySpark Job ---    
    # Note: In a production environment, consider adding more robust error handling, logging, and possibly retry logic depending on the use case.`