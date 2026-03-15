from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor

@dag(dag_id="fs_practice", schedule=None, start_date=datetime(2026, 3, 15), catchup=False)
def fs_practice():
    sensor= FileSensor(
        task_id="file_wait",
        filepath="/opt/airflow/dags/data.txt",
        poke_interval=30,
        timeout=180,
        mode="reschedule",
        fs_conn_id="fs_default"
    )

    @task
    def on_condition():
        print("file found")
    
    sensor >> on_condition()

dag=fs_practice()
