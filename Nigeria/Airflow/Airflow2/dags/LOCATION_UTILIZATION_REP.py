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

today = datetime.today()
dateRun = datetime.today() + timedelta(days=int(-1))
dateRunStr = dateRun.strftime('%Y%m%d')

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

dag = DAG(
    dag_id='LOCATION_UTILIZATION_REP',
    default_args=args,
    schedule_interval='30 8,11,14 4 * *',
    description='DO NOT TURN OFF',
    catchup=False,
    concurrency=3,
    max_active_runs=1
)

LOCATION_UTILIZATION_REP_4G = BashOperator(
    task_id='LOCATION_UTILIZATION_REP_4G' ,
    bash_command='bash /nas/share05/ops/scripts/LOCATION_UTILIZATION_REP_4G/location_utilization_rep.sh ',
    run_as_user='daasuser',
    dag=dag,
)


