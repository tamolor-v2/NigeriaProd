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
    'email': ['kayode.ogunyemi@mtn.com'],
    'email_on_failure': ['kayode.ogunyemi@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'catchup':True,
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 26),
}

dag = DAG(
    dag_id='Ussd_Report_Location',
    default_args=args,
    schedule_interval='0 1 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

t1 = BashOperator(
     task_id='Ussd_Report_Location' ,
     bash_command='/nas/share05/ops/daily/ussd_report_scripts.py',
     dag=dag,
     run_as_user = 'daasuser'
)
t1

