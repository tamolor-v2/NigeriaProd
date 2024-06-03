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
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


date_param_1 = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d') 
date_param_2 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d') 

dag = DAG(
    dag_id='CUSTOMERSASSIGNMENT',
    default_args=args,
    schedule_interval='0 7 * * *',
    catchup=False,
    concurrency=4,
    max_active_runs=4
)


CustomersAssignment = BashOperator(
     task_id='ayobaevents_to_flytxt' ,
     bash_command='bash /nas/share05/ops/samples/rechard_script.sh {0} {1} wbs_pm_rated_cdrs    '.format(date_param_1,date_param_2),
     dag=dag,
     run_as_user = 'daasuser'
)

CustomersAssignment 
