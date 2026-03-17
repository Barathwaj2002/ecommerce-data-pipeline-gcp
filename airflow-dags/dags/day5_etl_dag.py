from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta

def on_failure_callback(context):
    dag_id=context['dag'].dag_id
    task_id=context['task'].task_id
    logical_date=context['logical_date']
    print(f"Failed DAG {dag_id} of task {task_id} at {logical_date}")

default_args={
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': on_failure_callback
}

@dag(
    dag_id="ecommerce_etl",
    start_date=datetime(2026,3,11),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def ecommerce_etl():
    @task_group(group_id='ingestion')
    def ingestion():
        @task.bash
        def ingest():
            return """kubectl delete job ingestion-job --ignore-not-found &&
                      kubectl apply -f /opt/airflow/jobs/ingestion-job.yml &&
                      kubectl wait --for=condition=complete job/ingestion-job --timeout=300s"""
        return ingest()
    @task_group(group_id='transform')
    def transformation():
        @task.bash
        def transform():
            return """
            kubectl delete job pyspark-job --ignore-not-found &&
            kubectl apply -f /opt/airflow/jobs/pyspark-job.yml &&
            kubectl wait --for=condition=complete job/pyspark-job --timeout=300s
        """
        return transform()
    @task_group(group_id='load')
    def loading():
        @task.bash
        def load():
            return """
            kubectl delete job load-mongo-job --ignore-not-found &&
            kubectl apply -f /opt/airflow/jobs/load-mongo-job.yml &&
            kubectl wait --for=condition=complete job/load-mongo-job --timeout=300s
        """
        return load()
    @task
    def summary(**context):
        print(f"Pipeline complete — DAG: {context['dag'].dag_id}, Date: {context['logical_date']}")
    ingestion() >> transformation() >> loading() >> summary()
dag = ecommerce_etl()

