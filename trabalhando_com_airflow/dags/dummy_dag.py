# Importação das bibliotecas necessárias
from datetime import timedelta
# O objeto DAG; precisaremos dele para instanciar um DAG.
from airflow.models import DAG
# Operadores; vocês precisam disso para escrever tarefas!
from airflow.operators.bash import BashOperator

# Isso facilita o agendamento
from datetime import datetime


default_args = {
    'owner': 'Your name',
    'start_date': datetime(2026, 1, 1),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='Dummy DAG',
    schedule=timedelta(minutes=1),
)

# define the tasks

# define the first task

task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 1',
    dag=dag,
)

# define the second task
task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 2',
    dag=dag,
)

# define the third task
task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 3',
    dag=dag,
)

# task pipeline
task1 >> task2 >> task3