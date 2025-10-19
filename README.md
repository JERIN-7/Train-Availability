# Train Availability Pipeline

## Overview
This project is a **batch streaming data pipeline** for processing railway ticket data. The pipeline orchestrates extraction, transformation, and loading (ETL) operations using **Apache Spark** and **Apache Airflow**, and stores processed data in **Cassandra**.  

It is designed to clean and structure messy PNR data and build a **train availability history dataset** that can be used for querying and analysis.

## Features

- **Data Ingestion:** Reads raw PNR and ticket data from CSV files.  
- **Data Cleaning & Transformation:**  
  - Handles messy and inconsistent data.  
  - Structures the data into meaningful columns.  
  - Aggregates daily train availability statistics.  
- **Data Storage:** Stores processed data in **Cassandra** for easy querying and analysis.  
- **Workflow Orchestration:** Uses **Airflow** to automate and schedule the pipeline.  
- **Scalable Processing:** Uses **Spark** for distributed computation to handle large datasets efficiently.

## Prerequisites

- Docker & Docker Compose  
- Python 3.10+  
- Apache Spark  
- Apache Cassandra  

## Usage

- Place raw CSV files in the `Data_sets/` directory (these files are ignored by Git).  
- Run the Airflow DAGs to process the data.  
- Spark ETL scripts will clean and structure the data.  
- The processed data will be stored in Cassandra for future queries.

## Data Processing Workflow

1. **Extract:**  
   Read raw CSVs containing PNR and ticket information.

2. **Transform:**  
   - Clean messy fields and normalize data.  
   - Aggregate availability statistics per train per day.  
   - Generate structured data for downstream analysis.

3. **Load:**  
   Save structured data into Cassandra tables for persistent storage.

## Key Technologies

- **Apache Spark**: Distributed data processing framework for handling large datasets efficiently.  
- **Apache Cassandra**: Scalable NoSQL database for high-performance storage.  
- **Apache Airflow**: Workflow orchestration and scheduling tool.  
- **Docker**: Containerization platform to ensure a reproducible environment.
