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
    'email': ['olorunsegun.adeniyi@mtn.com'],
    'email_on_failure': ['olorunsegun.adeniyi@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'catchup':False,
    'depends_on_past': False,
    'start_date': datetime(2019, 7, 16),
}

dag = DAG(
    dag_id='Fixed_Mou_Weekly',
    default_args=args,
    schedule_interval='0 4 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

t1 = BashOperator(
     task_id='fixed_MOU_WEEKLY' ,
     bash_command='/nas/share05/ops/mtnops/fixed_mou_weekly.py `date --date="-0 days" +%Y%m`',
     dag=dag,
     run_as_user = 'daasuser'
)

t1
