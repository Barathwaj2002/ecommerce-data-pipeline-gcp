from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id="child_dag", schedule=None, start_date=datetime(2026, 3, 17), catchup=False)
def child_dag():
    @task
    def process():
        print("Child dag processing done")
    process()
dag = child_dag()