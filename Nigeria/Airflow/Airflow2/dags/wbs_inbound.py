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
    'email': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

 
 
dag = DAG(
    dag_id='WBS_INBOUND',
    default_args=args,
    schedule_interval='0 7 * * *',
    description='WBS OPERATOR REPORT FOR FINANCE',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)


WBS_INBOUND = BashOperator(
     task_id='WBS_INBOUND' ,
     bash_command='/nas/share05/ops/daily/wbs_inbound_v7.py -p ALL -s {0} -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


WBS_INBOUND
