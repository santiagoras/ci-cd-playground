from datetime import datetime, timedelta
from airflow.decorators import task
from local_module.utils.adwords import get_message
from random import randint, random
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_TAGS = {
    "team": "data",
}

DAG_DESCRIPTION = "Eaxmple DAG"
DAG_DOC = """
# This is a an example DAG

Doc here. Test update on dags/example-dag/code
"""


def _random_command(max_duration: int) -> str:
    return f"sleep {randint(0, max_duration or 1) + random()}"


with DAG(
    "example-dag",
    default_args={
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(seconds=5),
    },
    description=DAG_DESCRIPTION,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    doc_md=dedent(DAG_DOC),
    tags=[f"{k}:{v}" for k, v in DAG_TAGS.items()],
) as dag:
    
    @task
    def print_message():
        print(get_message())

    task_extract = BashOperator(task_id="extract", bash_command=_random_command(2))
    task_load = BashOperator(task_id="load", bash_command=_random_command(2))
    task_transform = BashOperator(task_id="transform", bash_command=_random_command(4))

    print_message() >> task_extract >> task_load >> task_transform
