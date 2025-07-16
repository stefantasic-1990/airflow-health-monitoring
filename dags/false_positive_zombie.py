from airflow.decorators import dag, task
from airflow.utils import timezone
import time

@dag(
    dag_id="false_positive_zombie",
    schedule=None,
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["zombie_sim"],
)
def false_positive_zombie():

    @task
    def long_running_task():
        sleep_seconds=600
        print(f"Sleeping for {sleep_seconds} seconds")
        time.sleep(sleep_seconds)

    long_running_task()

dag=false_positive_zombie()