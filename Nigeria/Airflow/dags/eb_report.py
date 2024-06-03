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
    'email': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

dag = DAG(
    dag_id='EB_REPORTS', 
    default_args=args,
    schedule_interval='0 7 * * *',
    catchup=False,
    concurrency=3,
    max_active_runs=1

)
eb_reports = BashOperator(
     task_id='eb_reports' ,
     bash_command='/nas/share05/ops/mtnops/eb_reports_prod.py -p ALL > /nas/share05/ops/logs/eb_reports.txt',
     dag=dag,
     run_as_user = 'daasuser'
)
start = BashOperator(
     task_id='echo' ,
     bash_command='echo start',
     dag=dag,
     run_as_user = 'daasuser'
)

new = BashOperator(
     task_id='new' ,
     bash_command='/nas/share05/ops/mtnops/eb_reports_prod.py -p NEW > /nas/share05/ops/logs/eb_reports_new.txt',
     dag=dag,
     run_as_user = 'daasuser'
)

start >> eb_reports >> new
