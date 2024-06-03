from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


import airflow
from airflow.models import DAG
from airflow.models import Variable
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
    dag_id='HOURLY_RCH',
    default_args=args,
    schedule_interval='45 * * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

HOURLY_RCH = BashOperator(
     task_id='HOURLY_RCH' ,
     depends_on_past=False,
     bash_command='python3.6 /nas/share05/ops/mtnops/hourly_rch.py -p HOURLY_RECHARGES -l 2 ',
     dag=dag,
     run_as_user = 'daasuser'
)

#HOURLY_SMS_ACTIVATION_REQ = BashOperator(
#     task_id='SMS_ACTIVATION_REQUEST' ,
#     depends_on_past=False,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SMS_ACTIVATION_REQUEST -l 1 ',
     #bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SMS_ACTIVATION_REQUEST -l 1 -s 2020-06-11',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

HOURLY_RCH  
#HOURLY_SMS_ACTIVATION_REQ
