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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='FLYTXT_LATCH_DUMP',
    default_args=args,
    schedule_interval='0 7,9,10,11,12,13 * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=3

)

FLYTXT_LATCH_DUMP = BashOperator(
     task_id='FLYTXT_LATCH_DUMP' ,
     bash_command='/nas/share05/ops/mtnops/latch_move_files.py `date  +%Y%m%d_%H%M` ',
     run_as_user='daasuser',
     dag=dag,
)

#SSH_EDGE = BashOperator(
#     task_id='ssh_edge' ,
#     bash_command='ssh edge01001',
#     run_as_user='daasuser',
#     dag=dag,
#)

#SSH_EDGE >> 
FLYTXT_LATCH_DUMP

