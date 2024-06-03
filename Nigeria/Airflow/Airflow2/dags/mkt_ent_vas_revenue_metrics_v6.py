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
    dag_id='mkt_ent_vas_revenue_metrics',
    default_args=args,
    schedule_interval='45 8 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

VAS_refresh = BashOperator(
     task_id='VAS_refresh' ,
     bash_command='perl /nas/share05/ops/daily/mkt_ent_vas_revenue_metrics_v8.pl ',
     dag=dag,
     run_as_user = 'daasuser'
)

VAS_refresh
