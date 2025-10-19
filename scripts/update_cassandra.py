from pyspark.sql import SparkSession

def update_to_cassandra(input_path, keyspace="railway", table="train_summary"):
    """Write processed data to Cassandra."""
    print("🚀 Starting Cassandra update...")

    # Initialize Spark session with Cassandra connection configs
    spark = (
        SparkSession.builder.appName("UpdateCassandra")
        .config("spark.cassandra.connection.host", "127.0.0.1")
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
        .getOrCreate()
    )

    print(f"📂 Reading processed data from {input_path} ...")
    final_df = spark.read.parquet(input_path)

    print(f"💾 Writing data to Cassandra keyspace={keyspace}, table={table} ...")
    (
        final_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=table, keyspace=keyspace)
        .save()
    )

    print("✅ Data successfully updated to Cassandra!")
    spark.stop()


def main():
    """Entry point for standalone execution."""
    input_path = "/home/jerin-nadar/Train_availability/processed/train_summary.parquet"
    update_to_cassandra(input_path)


if __name__ == "__main__":
    main()
