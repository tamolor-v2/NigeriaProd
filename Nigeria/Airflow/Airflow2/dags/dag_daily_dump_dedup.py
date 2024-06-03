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
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'concurrency' : 1,
    'catchup': False,
}

dag = DAG(
      dag_id='Daily-Usages-Summaries-SDP-Movement',
      default_args=args,
      schedule_interval='*/30 1-8 * * *',
      catchup=False,
      concurrency=1,
      max_active_runs=1
)

sdp = BashOperator(
     task_id='SDP_Dump_Summary' ,
     bash_command='/nas/share05/ops/mtnops/usage_summary.py -p SDP -s `date --date="-1 days" +%Y-%m-%d` -l 2',
     dag=dag,
     run_as_user = 'daasuser'
)


sdp2 = BashOperator(
     task_id='Provider_SDP_Dump_Summary' ,
     bash_command='/nas/share05/ops/mtnops/usage_summary.py -p SPONS_DATA -s `date --date="-1 days" +%Y-%m-%d` -l 5',
     dag=dag,
     run_as_user = 'daasuser'
)
 
sdp
sdp2
