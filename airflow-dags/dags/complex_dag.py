from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.datasets import Dataset
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def custom_callback(context):
    dag_id=context["dag"].dag_id
    task_id=context["task"].task_id
    logical_date=context["logical_date"]
    print(f"ALERT: Task {task_id} of DAG {dag_id} failed at {logical_date}")

default_args={
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': custom_callback
}
my_dataset=Dataset("ds")
@dag(dag_id="complex_dag", start_date=datetime(2026, 3, 18), schedule="@daily", default_args=default_args, catchup=False, max_active_runs=1, max_active_tasks=2)
def complex_dag():
    @task_group(group_id="group1")
    def group1():
        @task(outlets=[my_dataset])
        def task1():
            print("task1 done")
        @task
        def task2():
            print("task2")
        t1 = task1()
        t2= task2()
    @task_group(group_id="group2")
    def group2():
        @task(outlets=[my_dataset])
        def task1():
            print("task1 done")
        @task
        def task2():
            print("task2")
        t1 = task1()
        t2 = task2()

    @task_group(group_id="group3")
    def group3():
        @task
        def task1():
            print("triggering child dag")
        
        trigger = TriggerDagRunOperator(
                task_id="triggering_child",
                trigger_dag_id="child_dag",
                wait_for_completion=False,
                poke_interval=10
            )
        
        @task
        def task2():
            print("done")

        task1() >> trigger >> task2()
    
    @task_group(group_id="group4")
    def group4():
        @task
        def task1(value):
            return (f"xcom_value: {value}")
        
        @task
        def task2():
            return 1
        task1(task2())
    group1() >> group2() >> group3() >> group4()    