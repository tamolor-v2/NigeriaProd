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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'catchup':True,
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 13),
}

dag = DAG(
    dag_id='VP_BTS_HR',
    default_args=args,
#    schedule_interval='55 5-21 * * *',
    schedule_interval=None,
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

t1 = BashOperator(
     task_id='vp_bts_hr' ,
     bash_command='/nas/share05/ops/mtnops/vp_bts_hr.py -p VP_BTS',
     dag=dag,
     run_as_user = 'daasuser'
)

t1
