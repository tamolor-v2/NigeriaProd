from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import os

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': 'o.olanipekun@ligadata.com',
    'email_on_failure': 'o.olanipekun@ligadata.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 27),
}

dag = DAG(
    dag_id='ayoba_smsc',
    default_args=args,
    description = 'ayoba_smsc',
    schedule_interval='30 7 * * *',
    concurrency=1,
    catchup=True,
    max_active_runs=1
)

t2 = BashOperator(
     task_id='ayoba_smsc',
     #bash_command='python3.6 /nas/share05/ops/mtnops/data_ingestion.py -p sim_swop',
     bash_command='perl /nas/share05/ops/daily/ayoba_smsc.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)
    
t2
