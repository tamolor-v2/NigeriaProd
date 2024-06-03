from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import os

import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': datetime(2021, 4, 30),
    'email': ['joshua.avbuere@mtn.com'],
    'email_on_failure': ['joshua.avbuere@mtn.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='digital_statement_monthly',
          default_args=args,
          schedule_interval='0 11 1 * *',
          catchup=False,
          concurrency=1,
          max_active_runs=1

)


def end_dag():
    print('Digital Statement Monthly Report added succesfully.')

with dag:
    digital_statement_monthly_write = BashOperator(task_id='digital_statement_monthly',bash_command ='bash /nas/share05/dataOps_dev/digital_statement/digital_statement_prod_v2.sh  `date -d "-1 month -$(($(date +%d)-1)) days" +%Y%m%d` `date -d "-$(date +%d) days -0 month" +%Y%m%d`' )

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    digital_statement_monthly_write >> end_dag