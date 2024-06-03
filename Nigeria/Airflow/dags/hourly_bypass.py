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
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Hourly_by_pass',
    default_args=args,
    schedule_interval='0 * * * *',
    concurrency=1,
    catchup=False,
    max_active_runs=1

)

hourly_Airtel_bypass = BashOperator(
     task_id='hourly_Airtel_bypass' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/mtnops/hourly_Airtel_bypass.sh `date --date="-0 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser'
)

hourly_GLOBACOM_bypass = BashOperator(
     task_id='hourly_GLOBACOM_bypass' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/mtnops/hourly_GLOBACOM_bypass.sh `date --date="-0 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser'
)

hourly_9mobile_bypass = BashOperator(
     task_id='hourly_9mobile_bypass' ,
     bash_command='ssh edge01001 bash /nas/share05/ops/mtnops/hourly_9mobile_bypass.sh `date --date="-0 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser'
)

hourly_Airtel_bypass >> hourly_GLOBACOM_bypass >> hourly_9mobile_bypass
