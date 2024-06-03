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
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
date_param_1 = (datetime.now() - timedelta(days=3)).strftime('%Y%m%d')
 
 
dag = DAG(
    dag_id='SPONSORED_DATA',
    default_args=args,
    schedule_interval='0 3,4,5 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

SPONSORED_DATA = BashOperator(
     task_id='SPONSORED_DATA' ,
     bash_command='bash /nas/share05/dataOps_prod/sponsor_data/sponsored_data.sh {0}   '.format(date_param),
     dag=dag,
)

SPONSORED_DATA
