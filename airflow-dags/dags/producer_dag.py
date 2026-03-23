from airflow.decorators import dag, task
from airflow.datasets import Dataset
from datetime import datetime

my_dataset=Dataset("dummy-path")

@dag(dag_id="producer_dag", schedule=None, start_date=datetime(2026, 3, 18), catchup=False)
def producer_dag():
    @task(outlets=[my_dataset])
    def produce():
        print("Data produced and writter to S3")
    produce()
dag=producer_dag()
