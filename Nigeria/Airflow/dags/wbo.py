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
    dag_id='WBO',
    default_args=args,
    schedule_interval='0 12 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

CVM_WBO_BASE = BashOperator(
     task_id='CVM_WBO_BASE' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/wbo.py -l 1 -p CVM_WBO_BASE -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

DOLA30_DATASUB = BashOperator(
     task_id='DOLA30_DATASUB' , 
     bash_command='python3.6 /nas/share05/ops/mtnops/wbo.py -l 1 -p DOLA30_DATASUB -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

CVM_WBO_BASE >> DOLA30_DATASUB
