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
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
dag = DAG(
    dag_id='Daily_Finance_Reports',
    default_args=args,
    schedule_interval='0 3,12,23 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

Xtratime_report = BashOperator(
     task_id='Daily_Xtratime_report' ,
     bash_command='/nas/share05/ops/mtnops/usage_summary.py -p EXTRATIME l 5',
     dag=dag,
     run_as_user = 'daasuser'
)

cis_cdr = BashOperator(
     task_id='Daily_CIS_CDR_Sumary' ,
     bash_command='/nas/share05/ops/mtnops/usage_summary.py -p CIS_CDR_SUMMARY l 5',
     dag=dag,
     run_as_user = 'daasuser'
)

Xtratime_report
cis_cdr
