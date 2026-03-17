from airflow.decorators import dag, task
from datetime import datetime, timedelta

def alert_on_failure(context):
    dag_id=context["dag"].dag_id
    task_id=context["task"].task_id
    logical_date=context["logical_date"]
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed at {execution_date}")

default_args={
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': alert_on_failure
}

@dag(dag_id="py_op_seq", schedule="@daily",start_date=datetime.now() - timedelta(days=7),catchup=False, default_args=default_args)
def python_operators():
    @task(task_id="print_1")
    def print1():
        print("print1")
    @task(task_id="print_2")
    def print2():
        raise Exception("Simulated Failure")
    @task(task_id="print_3")
    def print3():
        print("print3")
    
    print1() >> print2() >> print3()
python_operators()
