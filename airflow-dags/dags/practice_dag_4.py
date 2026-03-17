from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id="dynamic_tasks", schedule=None, start_date=datetime(2026, 3,10), catchup=False)
def dynamic_tasks():
    
    @task
    def get_files():
        return ["file1.parquet", "file2.parquet", "file3.parquet"]
    
    @task
    def process_file(filename):
        print(f"processing {filename}")
    
    files= get_files()
    process_file.expand(filename=files)
dag = dynamic_tasks()