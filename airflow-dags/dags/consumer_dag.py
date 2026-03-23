from airflow.decorators import task, dag
from airflow.datasets import Dataset
from datetime import datetime

my_dataset=Dataset("dummy-path")

@dag(dag_id="consumer_dag", start_date=datetime(2026, 3, 18), schedule=[my_dataset], catchup=False)
def consumer_dag():
    @task
    def consume():
        print("Consuming produced data")
    consume()
dag = consumer_dag()