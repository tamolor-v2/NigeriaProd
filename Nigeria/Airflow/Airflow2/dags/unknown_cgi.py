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
    'email': ['j.fadare@ligadata.com','a.olabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','a.olabamidele@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d')

 
 
dag = DAG(
    dag_id='UNKNOWN_CGI',
    default_args=args,
    schedule_interval='0 8 * * MON',
    description='DO NOT TURN OFF',
    catchup=False,
    concurrency=2,
    max_active_runs=1
)


UNKNOWN_CGI = BashOperator(
     task_id='UNKNOWN_CGI' ,
     bash_command='python3.6 /nas/share05/ops/daily/daily_unknown_cgi.py -s {0} -p ALL -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


UNKNOWN_CGI
