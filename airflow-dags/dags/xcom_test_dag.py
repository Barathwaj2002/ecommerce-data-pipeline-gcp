from datetime import datetime
from airflow.decorators import dag, task

@dag('xcom_test', start_date= datetime(2026, 3, 14), schedule=None, catchup=False)
def xcom_flow():
    @task
    def push_data():
        # args['ti'].xcom_push(key='row_count',value=39000)
        return 39000
    @task 
    def pull_data(count):
        # count=args['ti'].xcom_pull(task_id='push_data', key='row_count')
        print(f"row count:{count}")
    pull_data(push_data())    
dag= xcom_flow()

# Note: TaskFlow api uses @task decorator. when returned a value within this, it auto pushes it to xcom passing it as 
#       argument that is auto pulled from xcom. this is cleaner than manual ti.xcom_push/pull