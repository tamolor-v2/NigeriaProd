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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='EVD_TRANSACTIONS_AND_456_REPORTS',
    default_args=args,
#    schedule_interval='0 9 * * *',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1

)

EVD_TRANSACTIONS_AND_456_REPORTS = BashOperator(
     task_id='evd_transactions_and_456_reports' ,
     bash_command='perl /nas/share05/scripts/EVD_REPORTS/EVD_Transactions_and_456_Reports.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     run_as_user='daasuser',
     dag=dag,
)

EVD_TRANSACTIONS_AND_456_REPORTS

