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
 
dag = DAG(
    dag_id='Data-Ingestion-IFS',
    default_args=args,
    schedule_interval='0 4 * * *',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)

IFS_ACT_BAL = BashOperator(
     task_id='IFS_ACCOUNT_BALANCE' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion1.py -p IFS_ACT_BAL ',
     dag=dag,
     run_as_user = 'daasuser'
)

IFS_CODE = BashOperator(
     task_id='IFS_CODE' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p IFS_CODE ',
     dag=dag,
     run_as_user = 'daasuser'
)

IFS_BUDGET= BashOperator(
     task_id='IFS_BUDGET' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p BUDGET' ,
     dag=dag,
     run_as_user = 'daasuser'
)

IFS_ACT = BashOperator(
     task_id='IFS_ACT' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p IFS_ACT',
     dag=dag,
     run_as_user = 'daasuser'
)

Service_Centre_Sales = BashOperator(
     task_id='Service_Centre_Sales' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p sales',
     dag=dag,
     run_as_user = 'daasuser'
)
IFS_ACT >> IFS_ACT_BAL >> IFS_CODE >>  IFS_BUDGET >> Service_Centre_Sales
