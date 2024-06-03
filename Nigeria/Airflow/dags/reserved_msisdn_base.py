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
    dag_id='RESERVED_MSISDN_BASE',
    default_args=args,
    schedule_interval='0 5,6,7 * * *',
    catchup=False,  
    concurrency=1,
    max_active_runs=1

)
reserved_msisdn_base = BashOperator(
     task_id='reserved_msisdn_base' ,
     bash_command='/nas/share05/ops/mtnops/reserved_msisdn_base.py -p RESERVED_MSISDN_BASE -l 2',
     run_as_user = 'daasuser',
     dag=dag,
)

reserved_extract = BashOperator(
     task_id='reserved_estraction' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/pseudo_loader.py `date --date="-1 days" +%Y%m%d`',
     run_as_user = 'daasuser',
     dag=dag,
)


reserved_msisdn_base >> reserved_extract

