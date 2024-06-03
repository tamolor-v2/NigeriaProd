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
    dag_id='LEA_DRIVER_TODAY',
    default_args=args,
#    schedule_interval='15 5,6,7,8,10,12,16,20,0 * * *',
    schedule_interval='15 5,9,11,15,19,21,23 * * *',
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)



LEA_DRIVER_TODAY = BashOperator(
     task_id='LEA_driver_today' ,
     bash_command='bash /nas/share05/tools/MSC_DAAS_LEA/LEA_driver_today.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)



LEA_DRIVER_TODAY
