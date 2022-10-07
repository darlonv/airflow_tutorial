# Original file from tutorial by:
# https://www.youtube.com/watch?v=4DGRqMoyrPk&t=127s
# Author: 
# Channel: Codifike

#Modified and commented by @darlonv

from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests, json

#Task function
def captura_conta_dados():
    url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
    # response = requests.get(url)
    # df = pd.DataFrame(json.loads(response.content))
    df = pd.read_json(url)
    qtd = len(df.index)
    return qtd

#Task code
# The return of a Branch task must be another task
def eh_valida(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados') #Get return from task captura_conta_dados
    if qtd > 100:      #Qtd is from another task
        return 'valido' #Go to task 'valido'
    return 'n_valido'   #Go to task 'n_valido'

with DAG('tutorial_dag_codifike',
        start_date = datetime(2022, 10, 7),
        schedule_interval = '30 * * * *',
        catchup = False
) as dag:

    #Task object
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',        #task name
        python_callable = captura_conta_dados   #task function
    )

    #Task object
    valido = BashOperator(
        task_id = 'valido',
        bash_command = 'echo "Quantidade OK"'
    )

    #Task object
    n_valido = BashOperator(
        task_id = 'n_valido',
        bash_command = 'echo "Quantidade não OK"'
    )

    #Task object
    eh_valida = BranchPythonOperator(
        task_id = 'eh_valida',
        python_callable = eh_valida
    )

    captura_conta_dados >> eh_valida >> [valido, n_valido]
