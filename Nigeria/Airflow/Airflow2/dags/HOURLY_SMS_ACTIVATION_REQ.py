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
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='SMS_ACTIVATION_REQ',
    default_args=args,
    schedule_interval='*/20 * * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


HOURLY_SMS_ACTIVATION_REQ = BashOperator(
     task_id='SMS_ACTIVATION_REQUEST' ,
     depends_on_past=False,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p B_SMS_ACTIVATION_REQUEST -l 1 ',
     #bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SMS_ACTIVATION_REQUEST -l 1 -s 2020-06-11',
     dag=dag,
     run_as_user = 'daasuser'
)

HOURLY_SMS_ACT_INGEST_REQ = BashOperator(
     task_id='SMS_ACT_INGEST' ,
     depends_on_past=False,
     bash_command='/nas/share05/ops/mtnops/sms_act_ingest.sh ',
     #bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SMS_ACTIVATION_REQUEST -l 1 -s 2020-06-11',
     dag=dag,
     run_as_user = 'daasuser'
)

HOURLY_SMS_ACTIVATION_REQ >> HOURLY_SMS_ACT_INGEST_REQ
