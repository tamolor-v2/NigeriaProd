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
    'email': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='NIN_SMS_MOVE',
    default_args=args,
    schedule_interval='0 5 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

NIN_SMS_MOVE = BashOperator(
     task_id='NIN_SMS_MOVE' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/nin_sms_move.py `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


END_SMS2 = BashOperator(
     task_id='END_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/sms_alert/nin_sms_move.sh  ',
     dag=dag,
     run_as_user = 'daasuser',
)


NIN_SMS_MOVE >> END_SMS2
