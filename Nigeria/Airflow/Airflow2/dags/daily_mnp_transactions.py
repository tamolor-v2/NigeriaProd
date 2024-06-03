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
    'start_date': datetime(2019, 11, 22),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
dag = DAG(
    dag_id='EMAIL_MNP_TRANSACTIONS',
    default_args=args,
#    schedule_interval='0 7 * * *',
    schedule_interval=None,
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

EMAIL_MNP_TRANSACTIONS = BashOperator(
     task_id='email_mnp_transactions' ,
     bash_command='bash /nas/share05/ops/daily/daily_mnp_transaction.sh',
     dag=dag,
     run_as_user = 'daasuser'
)

EMAIL_MNP_TRANSACTIONS
