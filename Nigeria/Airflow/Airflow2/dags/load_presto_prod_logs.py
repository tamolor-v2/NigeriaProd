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
    'email': ['j.adeleke@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_failure': ['j.adeleke@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='load_presto_prod_audit_logs',
    default_args=args,
    schedule_interval='20 * * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

load_presto_prod_logs = BashOperator(
     task_id='load_presto_prod_audit_logs' ,
     bash_command='bash /nas/share03/presto_audit/presto_prod/oldscripts/runprod.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

load_presto_prod_logs
