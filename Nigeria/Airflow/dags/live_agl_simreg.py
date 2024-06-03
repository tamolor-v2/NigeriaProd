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
    #'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
dag = DAG(
    dag_id='Daily_Live_AGL_Registration',
    default_args=args,
    catchup=True,
    schedule_interval='0 * * * *',
    concurrency=1,
    max_active_runs=1
)

agl_reg = BashOperator(
     task_id='live_Reg_AGL' ,
     #depends_on_past=True,
     bash_command='/nas/share05/ops/mtnops/live_agl_reg.py',
     dag=dag,
     run_as_user = 'daasuser'
)

agl_reg
