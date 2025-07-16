from airflow.decorators import dag, task
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator
import time

@dag(
    dag_id="abrupt_termination_zombie",
    schedule=None,
    start_date = timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["zombie_sim"],
    max_active_tasks=3,
)
def abrupt_termination_zombie():

    start = EmptyOperator(task_id="start")

    @task
    def sleeper_task_1():
        print("Task started and sleeping for 30 seconds. Kill the worker container now to simulate a zombie task.")
        time.sleep(30)

    @task
    def sleeper_task_2():
        print("Task started and sleeping for 30 seconds. Kill the worker container now to simulate a zombie task.")
        time.sleep(30)

    end = EmptyOperator(task_id="end")

    start >> [sleeper_task_1(), sleeper_task_2()] >> end

dag = abrupt_termination_zombie()