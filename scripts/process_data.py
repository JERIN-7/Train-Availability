from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, count, sum as _sum

def get_spark_session(app_name="TrainProcessing"):
    """Create and return a Spark session."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def mark_ac_nonac(df):
    """Add flags for AC and Non-AC based on class."""
    df = df.withColumn("Class_up", upper(col("Class")))
    df = df.withColumn("Is_AC", when(col("Class_up").rlike("(?i)(1AC|2AC|3AC|1A|2A|3A)"), 1).otherwise(0))
    df = df.withColumn("Is_NonAC", when(col("Is_AC") == 0, 1).otherwise(0))
    df = df.withColumn("Final_Status_up", upper(col("Final_Status")))
    return df


def compute_status_columns(df):
    """Add columns for confirmation and waitlist status."""
    return (
        df.withColumn("AC_Confirmed", when((col("Is_AC") == 1) & col("Final_Status_up").like("CONFIRMED%"), 1).otherwise(0))
          .withColumn("AC_Waitlisted", when((col("Is_AC") == 1) & col("Final_Status_up").like("WAITLISTED%"), 1).otherwise(0))
          .withColumn("NonAC_Confirmed", when((col("Is_NonAC") == 1) & col("Final_Status_up").like("CONFIRMED%"), 1).otherwise(0))
          .withColumn("NonAC_Waitlisted", when((col("Is_NonAC") == 1) & col("Final_Status_up").like("WAITLISTED%"), 1).otherwise(0))
    )


def aggregate_ticket_data(df):
    """Aggregate ticket data by train and journey date."""
    return df.groupBy("Train_No", "Journey_Date").agg(
        _sum("AC_Confirmed").alias("ac_conf"),
        _sum("AC_Waitlisted").alias("ac_wl"),
        _sum("NonAC_Confirmed").alias("nac_conf"),
        _sum("NonAC_Waitlisted").alias("nac_wl"),
        count("*").alias("tot_tkt")
    )


def process_data(train_path, ticket_path, output_path):
    """Main processing function."""
    spark = get_spark_session()
    print("🚄 Reading staging data...")

    train_df = spark.read.parquet(train_path)
    ticket_df = spark.read.parquet(ticket_path)

    print("🧩 Processing ticket data...")
    ticket_df = mark_ac_nonac(ticket_df)
    ticket_df = compute_status_columns(ticket_df)

    print("📊 Aggregating ticket data...")
    agg_df = aggregate_ticket_data(ticket_df)

    print("🔗 Joining with train details...")
    joined = train_df.join(agg_df, train_df.number == agg_df.Train_No, "inner").select(
        col("name").alias("train_name"),
        col("number").alias("train_no"),
        col("Journey_Date").alias("journey_dt"),
        col("from_stn"),
        col("to_stn"),
        col("dist"),
        col("tot_tkt"),
        col("ac_conf"),
        col("ac_wl"),
        col("nac_conf"),
        col("nac_wl")
    )

    print("💾 Writing processed data...")
    joined.write.mode("overwrite").parquet(output_path)
    print(f"✅ Processing completed: results stored in {output_path}")


def main():
    """Entry point for CLI execution."""
    train_path = "/home/jerin-nadar/Train_availability/staging/train.parquet"
    ticket_path = "/home/jerin-nadar/Train_availability/staging/ticket.parquet"
    output_path = "/home/jerin-nadar/Train_availability/processed/train_summary.parquet"

    process_data(train_path, ticket_path, output_path)


if __name__ == "__main__":
    main()
