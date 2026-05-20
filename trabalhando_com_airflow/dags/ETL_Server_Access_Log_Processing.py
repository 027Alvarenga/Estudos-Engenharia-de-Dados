# Importação das bibliotecas necessárias
from datetime import timedelta
# O objeto DAG; precisaremos dele para instanciar um DAG.
from airflow.models import DAG
# Operadores; vocês precisam disso para escrever tarefas!
from airflow.operators.python import PythonOperator

# Isso facilita o agendamento
from datetime import datetime
import requests

input_file = 'web_server_access_log.txt'
extracted_file = 'extracted_data.txt'
transformed_file = 'transformed_data.txt'
output_file = 'capitalized.txt'

def download_file():
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt'
    
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
            # Abre o arquivo local para escrita em modo binário
        with open(input_file, 'wb') as file:
                # Escreve o conteúdo do arquivo em blocos
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    print(f"Arquivo baixado com sucesso: {input_file}")
    

def extract():
    global input_file
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
                for line in infile:
                    fields = line.split('#')
                    if len(fields) >= 4:
                        field_1 = fields[0]
                        field_4 = fields[3]
                        outfile.write(field_1 + "#" + field_4 + "\n")
                        
                        
def transform():
    global extracted_file, transformed_file
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
                for line in infile:
                    processed_line = line.upper()
                    outfile.write(processed_line + '\n')
                    
                    
def load():
    global transformed_file, output_file
    print("Na função load")
    # Salva o array em um arquivo CSV
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
                for line in infile:
                    outfile.write(line + '\n')
    
def check():
    global output_file
    print("Na função check")
    # Salva o array em um arquivo CSV
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)
            
default_args = {
    'owner': 'Your name',
    'start_date': datetime(2026, 1, 1),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl-server-access-log',
    default_args=default_args,
    description='etl-server-access-log',
    schedule=timedelta(days=1),
)

# Define the task named download to call the `download_file` function
download = PythonOperator(
    task_id='download',
    python_callable=download_file,
    dag=dag,
)

# Define the task named execute_extract to call the `extract` function
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

# Define the task named execute_transform to call the `transform` function
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

# Task pipeline
download >> execute_extract >> execute_transform >> execute_load >> execute_check           