from airflow.decorators import dag, task
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
import time

@dag(
    dag_id="01_abrupt_worker_termination",
    schedule=None,
    start_date = timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["zombie_simulation"],
    max_active_tasks=2,
)
def abrupt_worker_termination():

    start = EmptyOperator(task_id="start")

    @task
    def interrupted_sleeper():
        print("Task started and sleeping for 20 seconds. Kill the worker container now to simulate a zombie task.")
        time.sleep(60)

    @task
    def comfortable_sleeper():
        print("My purpose in this task is to sleep without interruption.")
        time.sleep(60)

    start >> [interrupted_sleeper(), comfortable_sleeper()]

dag = abrupt_worker_termination()