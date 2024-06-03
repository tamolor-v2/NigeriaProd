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
    'email': ['oladimeji.olanipekun@mtn.com','j.adeleke@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','j.adeleke@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='cs5_ccn_voice_ma_dasplit_new',
    default_args=args,
    schedule_interval='@hourly',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

voice_ma_dasplit = BashOperator(
     task_id='cs5_ccn_voice_ma_dasplit_new' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cs5_ccn_voice_ma_dasplit_new.py `date --date="-2 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

voice_ma_dasplit
