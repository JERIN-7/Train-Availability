from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, coalesce

def ingest_data():
    spark = SparkSession.builder.appName("TrainIngestion").getOrCreate()

    # Read train data
    train_df = (
        spark.read.json("/home/jerin-nadar/Train_availability/Data_sets/trains.json")
        .selectExpr("explode(features) as feature")
        .select(
            col("feature.properties.name").alias("name"),
            col("feature.properties.number").cast("string").alias("number"),
            col("feature.properties.from_station_name").alias("from_stn"),
            col("feature.properties.to_station_name").alias("to_stn"),
            col("feature.properties.distance").alias("dist")
        )
    )

    # Read ticket data
    ticket_df = (
        spark.read.option("header", True).option("inferSchema", False)
        .csv("/home/jerin-nadar/Train_availability/Data_sets/Railway_Ticket_Confirmation_12M.csv")
        .select(
            col("PNR Number").alias("PNR"),
            col("Train Number").alias("Train_No"),
            coalesce(
                to_date(col("Date of Journey"), "yyyy-MM-dd"),
                to_date(col("Date of Journey"), "dd-MM-yyyy"),
                to_date(col("Date of Journey"), "MM/dd/yyyy")
            ).alias("Journey_Date"),
            col("Class of Travel").alias("Class"),
            col("Confirmation Status").alias("Final_Status")
        )
        .filter(col("Journey_Date").isNotNull())
    )

    # Write to staging
    train_df.write.mode("overwrite").parquet("/home/jerin-nadar/Train_availability/staging/train.parquet")
    ticket_df.write.mode("overwrite").parquet("/home/jerin-nadar/Train_availability/staging/ticket.parquet")

    print("✅ Ingestion completed: data stored in staging/")

default_args = {"owner": "airflow", "start_date": datetime(2025, 10, 15)}

with DAG("train_ingestion_dag", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data
    )
