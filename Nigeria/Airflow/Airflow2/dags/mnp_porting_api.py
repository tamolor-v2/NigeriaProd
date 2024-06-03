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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','s.adamson@ligadata.com','m.akano@ligadata.com','support@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

dateRunStr = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

dag = DAG(
    dag_id='MNP_PORTING_API',
    default_args=args,
    schedule_interval='0 7,12,21 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


MNP_PORTING_API = BashOperator(
     task_id='MNP_PORTING_API' ,
     bash_command='bash /nas/share05/FlareProd/Run/ApiSync/apiscripts/MNP_Porting.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

MNP_PORTING_API
