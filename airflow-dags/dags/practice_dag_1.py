from airflow.decorators import dag, task
from datetime import datetime, timedelta

@dag(dag_id="py_op_seq", schedule="@daily",start_date=datetime(2026, 3, 15),catchup=False)
def python_operators():
    @task(task_id="print_1", retries=1,retry_delay=timedelta(minutes=2))
    def print1():
        print("print1")
    @task(task_id="print_2", retries=1,retry_delay=timedelta(minutes=2))
    def print2():
        print("print2")
    @task(task_id="print_3", retries=1,retry_delay=timedelta(minutes=2))
    def print3():
        print("print3")
    
    print1() >> print2() >> print3()
python_operators()
