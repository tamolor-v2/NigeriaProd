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
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'catchup':False,
}


dag = DAG(
    dag_id='DAG_TEST_CREATE',
    default_args=args,
    schedule_interval='0 9 28-31 * *',
    catchup=False,
    concurrency=4,
    max_active_runs=4
)

whoami = BashOperator(
     task_id='whoami' ,
     bash_command='whoami',
     dag=dag,
     run_as_user = 'daasuser'
)

printtosin = BashOperator(
     task_id='printtosin' ,
     bash_command='echo "printtosin"',
     dag=dag,
     run_as_user = 'daasuser'
)


D_1 = BashOperator(
     task_id='D_1' ,
     bash_command='echo `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)


D_2 = BashOperator(
     task_id='D_2' ,
     bash_command='echo `date --date="-2 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)

D_3 = BashOperator(
     task_id='D_3' ,
     bash_command='echo `date --date="-3 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)

D_0 = BashOperator(
     task_id='D_0' ,
     bash_command='echo `date --date="-0 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)


done = BashOperator(
     task_id='done' ,
     bash_command='echo "done"  ',
     dag=dag,
     run_as_user = 'daasuser'
)

start = BashOperator(
     task_id='start' ,
     bash_command='echo "start"  ',
     dag=dag,
     run_as_user = 'daasuser'
)

printtosin1 = BashOperator(
     task_id='printtosin1' ,
     bash_command='echo "printtosin1"  ',
     dag=dag,
     run_as_user = 'daasuser'
)

start >> whoami >> [D_0, D_1, D_2, D_3] >> printtosin >> [printtosin1, done]
