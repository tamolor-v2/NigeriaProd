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
    dag_id='VP_BTS5M',
    default_args=args,
#    schedule_interval='30 5-21 * * *',
    schedule_interval=None,
    catchup=True,
    concurrency=1,
    max_active_runs=1

)
 
t2 = BashOperator(
     task_id='vp_bts1' ,
     bash_command='/nas/share05/ops/mtnops/vp_bts.py -p VP_BTS4',
     dag=dag,
     run_as_user = 'daasuser'
)

t3 = BashOperator(
     task_id='vp_bts2' ,
     bash_command='/nas/share05/ops/mtnops/vp_bts.py -p VP_BTS5',
     dag=dag,
     run_as_user = 'daasuser'
)


t2 >> t3
