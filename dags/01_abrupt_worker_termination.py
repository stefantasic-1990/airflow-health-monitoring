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
    def terminate_me():
        print("Task started and sleeping for 60 seconds. Kill the worker container now to simulate a zombie task.")
        time.sleep(60)

    terminate_me()

dag = abrupt_worker_termination()