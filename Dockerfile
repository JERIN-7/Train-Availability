FROM apache/airflow:2.6.0-python3.9

USER root

# Copy all project files
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /opt/airflow/requirements.txt
COPY scripts/entrypoint.sh /opt/airflow/script/entrypoint.sh

RUN chmod +x /opt/airflow/script/entrypoint.sh

USER airflow
WORKDIR /opt/airflow

ENTRYPOINT ["/opt/airflow/script/entrypoint.sh"]
