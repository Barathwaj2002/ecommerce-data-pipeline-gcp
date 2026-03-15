from airflow.decorators import task, dag
from datetime import datetime
import random

@dag(dag_id="xcom_practice", schedule="@daily", start_date=datetime(2026, 3, 15), catchup=False)
def xcom_practice():
    @task
    def xcom_push():
        list=[]
        for i in range (0,5):
            list.append(random.randint(1,10))
        return list
    @task
    def xcom_pull(list):
        res=[sum(list), sum(list)/len(list)]
        print(res)
    xcom_pull(xcom_push())
dag=xcom_practice()