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

args  = {
    'owner': 'MTN Nigeria',
    'email': ['o.olanipekun@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success':True,      
    'retries': 5,
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(7),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
dag = DAG(
    'SOURCE_LAT',
    default_args=args,
    description='Source Latencies',
    schedule_interval='0 7 * * 1',
    catchup=False,
    concurrency=1,
    max_active_runs=1
    )
source_latency = BashOperator(
    task_id='SOURCE_LATENCY',
    bash_command= '/nas/share05/ops/mtnops/source_latency.py ',
    dag=dag,
    run_as_user='daasuser')
    
source_latency
