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
    'catchup':True,
}
 
dag = DAG(
    dag_id='DIMENSION',
    default_args=args,
    schedule_interval='0 7,10 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

DIMENSION = BashOperator(
     task_id='dimensions' ,
     bash_command='nohup bash /nas/share05/scripts/dimensions/run_dimension.sh   `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  dim_cell > /dev/null &',
     dag=dag,
     run_as_user = 'daasuser'
)

DIMENSION
