from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime

@dag(dag_id='sensor_test', start_date=datetime(2026,3,14), schedule=None, catchup=False)
def file_sensor():
    wait_for_file=FileSensor(
            task_id='wait_for_file',
            filepath='/opt/airflow/dags/trigger.txt',
            poke_interval=10,
            timeout=300,
            # mode='poke'
            mode='reschedule'
        )

    @task
    def process():
        print('File found, processing!')
        
    wait_for_file >> process()
dag = file_sensor()
    