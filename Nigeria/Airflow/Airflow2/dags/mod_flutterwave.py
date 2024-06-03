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
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['ayodeji.shadare@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}
 
dag = DAG(
    dag_id='mod_flutterwave',
    default_args=args,
    schedule_interval='0 7 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

mod_flutterwave_recharge = BashOperator(
     task_id='mod_flutterwave_recharge' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/mod_flutterwave_recharge.py `date --date="-2 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

mod_flutterwave_databundle = BashOperator(
     task_id='mod_flutterwave_databundle' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/mod_flutterwave_databundle.py `date --date="-2 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

mod_flutterwave_recharge>>mod_flutterwave_databundle

