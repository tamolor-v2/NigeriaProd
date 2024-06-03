from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='StartApiDriver_Lea',
    default_args=args,
    schedule_interval='5,55 23,3,22 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

StartApiDriver_Lea = BashOperator(
     task_id='StartApiDriver_Lea' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/StartApiDriver_Lea.sh	',
     run_as_user = 'daasuser',
     dag=dag,
)

StartApiDriver_Lea
