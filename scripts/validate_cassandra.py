from pyspark.sql import SparkSession
from datetime import datetime
import os

def validate_cassandra_load(keyspace="railway", table="train_summary", log_path=None):
    """Validate that Cassandra table has records and log the count."""
    print("🔍 Starting Cassandra data validation...")

    # Initialize Spark session with Cassandra connector
    spark = (
        SparkSession.builder.appName("ValidateCassandraLoad")
        .config("spark.cassandra.connection.host", "127.0.0.1")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
        .getOrCreate()
    )

    # Read from Cassandra
    print(f"📂 Reading table {table} from keyspace {keyspace} ...")
    df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load()
    )

    count = df.count()
    print(f"✅ Cassandra Validation Successful — Total records: {count}")

    # Logging
    if not log_path:
        log_path = "/home/jerin-nadar/Train_availability/logs/validation_log.txt"

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    with open(log_path, "a") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Record count in {table}: {count}\n")

    spark.stop()
    print(f"📝 Validation log updated at {log_path}")


def main():
    """Entry point for standalone or Airflow PythonOperator execution."""
    validate_cassandra_load()


if __name__ == "__main__":
    main()
