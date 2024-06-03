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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

dag = DAG(
    dag_id='BULK_USSD_GENERATOR', 
    default_args=args,
    schedule_interval='30 7 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

)
file_to_dcbs = BashOperator(
     task_id='file_to_dcbs' ,
     bash_command='bash /nas/share05/tools/ExtractTools/BULK_USSD_GENERATOR/File_To_DCBS.sh  ',
     dag=dag,
     run_as_user = 'daasuser'
     )

file_to_dcbs