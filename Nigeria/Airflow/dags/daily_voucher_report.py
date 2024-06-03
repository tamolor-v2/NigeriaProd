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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','ayodeji.shadare@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

d_1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

dag = DAG(
    dag_id='DAILY_VOUCHER_REPORT',
    default_args=args,
    schedule_interval='50 6 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

DAILY_VOUCHER_REPORT = BashOperator(
     task_id='DAILY_VOUCHER_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/daily_voucher_report.sh {0} '.format(d_1),
     dag=dag,
     run_as_user = 'daasuser'
)

DAILY_VOUCHER_REPORT
