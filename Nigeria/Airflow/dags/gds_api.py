from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import calendar
from datetime import date


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
    'catchup':True,
}


#date_param = (datetime.now() - timedelta(days=4)).strftime('%Y%m%d')
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
#


dag = DAG(
    dag_id='GDS_API',
    default_args=args,
    schedule_interval='0 6,13,20 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)



GDS_API = BashOperator(
     task_id='GDS_API' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/GDS_API.sh {0}  '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


GDS_API
