from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    "owner": "jerin",
    "start_date": datetime(2025, 10, 12),
    "retries": 1,
}

# --- Define helper function to run scripts ---
def run_script(script_path):
    """Run any Python script as a subprocess."""
    result = subprocess.run(
        ["python3", script_path],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed with error:\n{result.stderr}")

# --- DAG definition ---
with DAG(
    dag_id="train_data_pipeline_v3",
    default_args=default_args,
    description="Train ETL Pipeline: ingestion → processing → update → validation",
    schedule_interval=None,
    catchup=False,
) as dag:

    ingest = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_script,
        op_args=["/opt/airflow/scripts/ingest_data.py"],
    )
    

    process = PythonOperator(
        task_id="process_data",
        python_callable=run_script,
        op_args=["/opt/airflow/scripts/process_data.py"],
    )

    update = PythonOperator(
        task_id="update_cassandra",
        python_callable=run_script,
        op_args=["/opt/airflow/scripts/update_cassandra.py"],
    )

    validate = PythonOperator(
        task_id="validate_cassandra_load",
        python_callable=run_script,
        op_args=["/opt/airflow/scripts/validate_cassandra.py"],
    )

    ingest >> process >> update >> validate
