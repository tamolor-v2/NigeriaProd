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
    'catchup':True,
}

dag = DAG(
    dag_id='EB_REPORT_RERUN', 
    default_args=args,
    schedule_interval='0 16 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

eb_reports_1 = BashOperator(
     task_id='eb_reports_1' ,
     bash_command='/nas/share05/ops/mtnops/eb_reports.py -p ALL -l 2 -s `date --date="-1 days" +%Y-%m-%d`  > /nas/share05/ops/logs/eb_reports.txt',
     dag=dag,
     run_as_user = 'daasuser'
)

new_1 = BashOperator(
     task_id='new_1' ,
     bash_command='/nas/share05/ops/mtnops/eb_reports.py -p NEW -l 2 -s `date --date="-1 days" +%Y-%m-%d`  > /nas/share05/ops/logs/eb_reports_new.txt',
     dag=dag,
     run_as_user = 'daasuser'
)


eb_reports_1 >> new_1
