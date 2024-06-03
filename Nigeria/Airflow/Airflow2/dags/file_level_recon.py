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
    'catchup':False,
}
 
dag = DAG(
    dag_id='Daily_file_level_Reconcilation',
    default_args=args,
#    schedule_interval='0 0,9-15 * * *',
    schedule_interval=None,
    concurrency=1,
    max_active_runs=1
)

file_recon = BashOperator(
     task_id='Payment' ,
     bash_command='/nas/share05/ops/mtnops/usage_summary.py -p FILE_REC -s `date --date="-1 days" +%Y-%m-%d` -l 10',
     dag=dag,
     run_as_user = 'daasuser'

)

file_recon