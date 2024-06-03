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
    'email': ['j.fadare@ligadata.com','a.olabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','a.olabamidele@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d')

 
 
dag = DAG(
    dag_id='CONSOLIDATED_VR',
    default_args=args,
    schedule_interval='0 7 * * *',
    description='DO NOT TURN OFF',
    catchup=True,
    concurrency=2,
    max_active_runs=1
)


CONSOLIDATED_VR = BashOperator(
     task_id='CONSOLIDATED_VR' ,
     bash_command='python3.6 /nas/share05/ops/daily/daily_consolidated_vr_email.py -s {0} -p ALL -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


CONSOLIDATED_VR
