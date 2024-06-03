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
    'email': ['o.olanipekun@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d') 

dag = DAG(
    dag_id='Device_Sales',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=False,
    concurrency=4,
    max_active_runs=4
)


Device_Sales_report = BashOperator(
     task_id='Device_Sales_report' ,
     bash_command='/nas/share05/scripts/devicesales/run_devicesales.sh {0} {1}  '.format(date_param,date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

