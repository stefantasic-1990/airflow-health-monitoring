from airflow.decorators import dag, task
from airflow.utils import timezone
import os
import signal
import time

@dag(
    dag_id="zombie_sim",
    schedule=None,
    start_date = timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["failure_simulation"],
)
def zombie_sim():

    @task
    def terminate_me():
        print("Task started and sleeping for 1 minute. Kill the worker container now to simulate a zombie task.")
        time.sleep(60)

    terminate_me()

dag = zombie_sim()