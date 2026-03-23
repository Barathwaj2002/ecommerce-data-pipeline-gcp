from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

@dag(dag_id="parent_dag",schedule=None, start_date=datetime(2026, 3, 18), catchup=False)
def parent_dag():
    @task
    def prepare():
        print("Parent: preparation done")
    trigger_child=TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_dag",
        wait_for_completion=True,
        poke_interval=10
    )

    prepare() >> trigger_child
dag = parent_dag()
