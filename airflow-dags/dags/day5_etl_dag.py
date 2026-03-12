from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG (
    dag_id="ecommerce_etl",
    start_date=datetime(2026,3,11),
    schedule="@daily",
    catchup=False
) as dag:
    ingest= BashOperator(
        task_id="ingest_data",
        bash_command="""
        kubectl delete job ingestion-job --ignore-not-found &&
        kubectl apply -f /opt/airflow/jobs/ingestion-job.yml &&
        kubectl wait --for=condition=complete job/ingestion-job
    """

    )

    transform= BashOperator(
        task_id="pyspark_transform",
        bash_command="""
            kubectl delete job pyspark-job --ignore-not-found &&
            kubectl apply -f /opt/airflow/jobs/pyspark-job.yml &&
            kubectl wait --for=condition=complete job/ingestion-job
        """
    )

    load= BashOperator(
        task_id="load_to_mongo",
        bash_command="""
            kubectl delete job load-mongo-job --ignore-not-found &&
            kubectl apply -f /opt/airflow/jobs/load-mongo-job.yml &&
            kubectl wait --for=condition=True job/load-mongo-job 
        """
    )

    ingest >> transform >> load