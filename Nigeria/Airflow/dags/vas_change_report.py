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
    'email': ['support@ligadata.com','a.qayyas@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_failure': ['support@ligadata.com','a.qayyas@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='insert_vas_change_report',
    default_args=args,
    schedule_interval='0 8 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

vas_change_report = BashOperator(
     task_id='insert_vas_change_report' ,
     bash_command='bash /nas/share05/scripts/VAS_CHANGE_REPORT/VAS_CHANGE_REPORT.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

vas_change_report
