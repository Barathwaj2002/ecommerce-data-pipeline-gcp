from airflow.decorators import dag, task, task_group
from datetime import datetime

@dag(dag_id="task_groups", schedule=None, start_date=datetime(2026, 3, 15), catchup=False)
def task_group_dag():

    @task_group(group_id="ingestion")
    def ingestion_group():
        @task
        def extract():
            print("extracting")
        
        @task
        def validate():
            print("validating")
        
        extract() >> validate()
    @task_group(group_id="transformation")
    def transformation_group():
        @task
        def transform():
            print("transforming")
        
        @task
        def aggregate():
            print("aggregating")
        
        transform() >> aggregate()
    
    @task
    def load():
        print("loading")
    
    ingestion_group() >> transformation_group() >> load()

dag = task_group_dag()
        
    
        
