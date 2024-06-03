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
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='VAS_SHORTCODE_424',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

vas_shortcode_424 = BashOperator(
     task_id='vas_shortcode_424' ,
     bash_command='/nas/share05/ops/mtnops/vas_shortcode_424.py -p VAS_SHORTCODE_424 -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)

vas_shortcode_424 
