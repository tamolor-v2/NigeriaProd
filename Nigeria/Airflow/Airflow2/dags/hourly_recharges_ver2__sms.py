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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com','oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Recharge_SMS_2',
    default_args=args,
    schedule_interval='@hourly',
    concurrency=1,
    max_active_runs=1

)

Recharges_SMS = BashOperator(
     task_id='Recharges_SMS' ,
     bash_command='cd /nas/share05/ops/sms_alert/ && python3.6 /nas/share05/ops/sms_alert/hourly_recharges_ver2.py',
     dag=dag,
     run_as_user = 'daasuser', 
)

Recharges_SMS

