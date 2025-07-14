from airflow.decorators import dag, task
from airflow.utils import timezone
import time

@dag(
    dag_id="01_abrupt_worker_termination",
    schedule=None,
    start_date = timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["zombie_simulation"],
)
def abrupt_worker_termination():

    @task
    def interrupted_sleeper():
        print("Task started and sleeping for 20 seconds. Kill the worker container now to simulate a zombie task.")
        time.sleep(20)

    def comfortable_sleeper():
        print("My purpose in this DAG is to sleep without interruption.")
        time.sleep(20)

    interrupted_sleeper()
    comfortable_sleeper()

dag = abrupt_worker_termination()