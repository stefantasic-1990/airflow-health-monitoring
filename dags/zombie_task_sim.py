from airflow.decorators import dag, task
from airflow.utils import timezone
import time

@dag(
    dag_id="zombie_task_sim",
    schedule=None,
    start_date = timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["failure_simulation"],
)
def zombie_task_sim():

    @task
    def terminate_me():
        print("Task started and sleeping for 30 seconds. Kill the worker container now to simulate a zombie task.")
        time.sleep(30)

    terminate_me()

dag = zombie_task_sim()