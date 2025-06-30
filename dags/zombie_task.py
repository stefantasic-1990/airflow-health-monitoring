# dags/zombie_self_kill_dag.py

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import os
import signal
import time

@dag(
    dag_id="zombie_task",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["failure_simulation"],
)
def zombie_task():

    @task
    def self_terminate():
        print("Task starting. Will kill its own process in 10 seconds...")
        time.sleep(10)  # Let logs flush, simulate processing
        pid = os.getpid()
        print(f"Killing process with ID: {pid}")
        os.kill(pid, signal.SIGKILL)

    self_terminate()

dag = zombie_task()