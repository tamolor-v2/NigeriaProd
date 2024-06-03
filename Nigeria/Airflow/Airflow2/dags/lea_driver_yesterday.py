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
    'retries': 7,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='LEA_DRIVER_YESTERDAY',
    default_args=args,
    schedule_interval='15 03,14 * * *',
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)



LEA_DRIVER_YESTERDAY = BashOperator(
     task_id='LEA_driver_yesterday' ,
     bash_command='bash /nas/share05/tools/MSC_DAAS_LEA/LEA_driver_yesterday.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)



LEA_DRIVER_YESTERDAY
