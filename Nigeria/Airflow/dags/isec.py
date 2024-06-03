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
    'catchup':True,
}

dag = DAG(
    dag_id='ISEC',
    default_args=args,
    schedule_interval='30 8 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

ISEC = BashOperator(
     task_id='ISEC' ,
     bash_command='/nas/share05/ops/mtnops/SBA_SUMMARIES.py -p ISEC -l 4 -s `date --date="-1 days" +%Y-%m-%d` ',
     dag=dag,
     run_as_user = 'daasuser'
)

ISEC
