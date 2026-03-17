from airflow.decorators import dag, task
from datetime import datetime
from airflow.models import Variable

@dag(dag_id="variables_test", schedule=None, start_date=datetime(2026, 3, 16), catchup=False)
def variables_test():
    @task
    def use_variable():
        mongo_uri=Variable.get("mongo_uri")
        batch_size=Variable.get("batch_size", default_var=500)
        print(f"Connecting to: {mongo_uri}")
        print(f"Batch size: {batch_size}")
    use_variable()
dag= variables_test()