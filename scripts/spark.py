from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, to_date, upper, when, count, sum as _sum, round as _round
)
from pyspark.sql.functions import year
from cassandra.cluster import Cluster

# ---------- Spark session with Cassandra connector ----------
spark = (
    SparkSession.builder
    .appName("TrainDataToCassandra")
    .config(
        "spark.jars.packages",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
    )
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

# ---------- Read train JSON ----------
df = spark.read.json("/home/jerin-nadar/Train_availability/Data_sets/trains.json")
features_df = df.selectExpr("explode(features) as feature")
train_df = features_df.select(
    col("feature.properties.name").alias("name"),
    col("feature.properties.number").alias("number"),
    col("feature.properties.from_station_name").alias("from_station_name"),
    col("feature.properties.to_station_name").alias("to_station_name"),
    col("feature.properties.distance").alias("distance")
)
train_df = train_df.withColumn("number", col("number").cast("string"))

# ---------- Read ticket CSV ----------
ticket_raw = spark.read.option("header", True).option("inferSchema", False) \
    .csv("/home/jerin-nadar/Train_availability/Data_sets/Railway_Ticket_Confirmation_12M.csv")

ticket_df = ticket_raw.select(
    col("PNR Number").alias("PNR"),
    col("Train Number").alias("Train_No"),
    col("Date of Journey").alias("Journey_Date"),
    col("Class of Travel").alias("Class"),
    col("Quota").alias("Quota"),
    col("Booking Date").alias("Booking_Date"),
    col("Current Status").alias("Status"),
    col("Number of Passengers").alias("Passenger_Count"),
    col("Seat Availability").alias("Seat_Availability"),
    col("Holiday or Peak Season").alias("Peak_Season"),
    col("Waitlist Position").alias("WL_Position"),
    col("Confirmation Status").alias("Final_Status")
)

# ---------- Robust date parsing ----------
ticket_df = ticket_df.withColumn(
    "Journey_Date",
    coalesce(
        to_date(col("Journey_Date"), "yyyy-MM-dd"),
        to_date(col("Journey_Date"), "dd-MM-yyyy"),
        to_date(col("Journey_Date"), "MM/dd/yyyy")
    )
).withColumn(
    "Booking_Date",
    coalesce(
        to_date(col("Booking_Date"), "yyyy-MM-dd"),
        to_date(col("Booking_Date"), "dd-MM-yyyy"),
        to_date(col("Booking_Date"), "MM/dd/yyyy")
    )
).filter(col("Journey_Date").isNotNull())

# ---------- AC / Non-AC detection ----------
ticket_flagged_df = ticket_df.withColumn("Class_up", upper(col("Class")))
ticket_flagged_df = ticket_flagged_df.withColumn(
    "Is_AC",
    when(col("Class_up").rlike("(?i)(^|\\W)(1AC|2AC|3AC|1A|2A|3A)(\\W|$)"), 1).otherwise(0)
).withColumn(
    "Is_NonAC",
    when(col("Is_AC") == 0, 1).otherwise(0)
)

# ---------- AC/NonAC confirmed/waitlisted ----------
ticket_flagged_df = ticket_flagged_df.withColumn("Final_Status_up", upper(col("Final_Status")))
ticket_flagged_df = ticket_flagged_df.withColumn(
    "AC_Confirmed", when((col("Is_AC") == 1) & col("Final_Status_up").like("CONFIRMED%"), 1).otherwise(0)
).withColumn(
    "AC_Waitlisted", when((col("Is_AC") == 1) & col("Final_Status_up").like("WAITLISTED%"), 1).otherwise(0)
).withColumn(
    "NonAC_Confirmed", when((col("Is_NonAC") == 1) & col("Final_Status_up").like("CONFIRMED%"), 1).otherwise(0)
).withColumn(
    "NonAC_Waitlisted", when((col("Is_NonAC") == 1) & col("Final_Status_up").like("WAITLISTED%"), 1).otherwise(0)
)

# ---------- Aggregate ----------
aggregated_df = ticket_flagged_df.groupBy("Train_No", "Journey_Date").agg(
    _sum("AC_Confirmed").alias("AC_Confirmed"),
    _sum("AC_Waitlisted").alias("AC_Waitlisted"),
    _sum("NonAC_Confirmed").alias("NonAC_Confirmed"),
    _sum("NonAC_Waitlisted").alias("NonAC_Waitlisted"),
    count("*").alias("Total_Tickets")
)

# ---------- Percentages ----------
aggregated_df = aggregated_df.withColumn(
    "AC_Confirmed_Percentage",
    when((col("AC_Confirmed") + col("AC_Waitlisted")) > 0,
         _round(col("AC_Confirmed") / (col("AC_Confirmed") + col("AC_Waitlisted")) * 100, 2)
    ).otherwise(None)
).withColumn(
    "AC_Waitlisted_Percentage",
    when((col("AC_Confirmed") + col("AC_Waitlisted")) > 0,
         _round(col("AC_Waitlisted") / (col("AC_Confirmed") + col("AC_Waitlisted")) * 100, 2)
    ).otherwise(None)
).withColumn(
    "NonAC_Confirmed_Percentage",
    when((col("NonAC_Confirmed") + col("NonAC_Waitlisted")) > 0,
         _round(col("NonAC_Confirmed") / (col("NonAC_Confirmed") + col("NonAC_Waitlisted")) * 100, 2)
    ).otherwise(None)
).withColumn(
    "NonAC_Waitlisted_Percentage",
    when((col("NonAC_Confirmed") + col("NonAC_Waitlisted")) > 0,
         _round(col("NonAC_Waitlisted") / (col("NonAC_Confirmed") + col("NonAC_Waitlisted")) * 100, 2)
    ).otherwise(None)
)

aggregated_df = aggregated_df.withColumn("Train_No", col("Train_No").cast("string"))

# ---------- Join with train master ----------
final_summary_df = train_df.join(
    aggregated_df,
    train_df.number == aggregated_df.Train_No,
    how="inner"
)

final_summary_df = final_summary_df.select(
    col("name").alias("train_name"),
    col("number").alias("train_no"),
    col("Journey_Date").alias("journey_dt"),
    col("from_station_name").alias("from_stn"),
    col("to_station_name").alias("to_stn"),
    col("distance").alias("dist"),
    col("Total_Tickets").alias("tot_tkt"),
    col("AC_Confirmed").alias("ac_conf"),
    col("AC_Waitlisted").alias("ac_wl"),
    col("NonAC_Confirmed").alias("nac_conf"),
    col("NonAC_Waitlisted").alias("nac_wl"),
    col("AC_Confirmed_Percentage").alias("ac_conf_pct"),
    col("AC_Waitlisted_Percentage").alias("ac_wl_pct"),
    col("NonAC_Confirmed_Percentage").alias("nac_conf_pct"),
    col("NonAC_Waitlisted_Percentage").alias("nac_wl_pct")
)

# ---------- Cassandra setup ----------
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("""
CREATE KEYSPACE IF NOT EXISTS railway
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
session.execute("""
CREATE TABLE IF NOT EXISTS railway.train_summary (
    train_no text,
    journey_dt date,
    train_name text,
    from_stn text,
    to_stn text,
    dist int,
    tot_tkt int,
    ac_conf int,
    ac_wl int,
    nac_conf int,
    nac_wl int,
    ac_conf_pct double,
    ac_wl_pct double,
    nac_conf_pct double,
    nac_wl_pct double,
    PRIMARY KEY (train_no, journey_dt)
);
""")
session.shutdown()
cluster.shutdown()

# ---------- Write to Cassandra ----------
final_summary_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table="train_summary", keyspace="railway") \
    .save()

print("Data successfully written to Cassandra!")
