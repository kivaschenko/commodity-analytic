from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

with DAG(dag_id="example_dag_with_bash_operator",
         start_date=datetime(2024, 1, 1),
         schedule="0 0 * * *",) as dag:
    hello_task = BashOperator(task_id="hello_task",
                              bash_command="echo 'Hello, Airflow!'")
    @task()
    def print_date():
        """A simple task to print the current date and time."""
        print(f"Current date and time: {datetime.now()}")
        
    hello_task >> print_date()
