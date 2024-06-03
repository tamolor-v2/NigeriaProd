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
    'email': ['support@ligadata.com','a.qayyas@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_failure': ['support@ligadata.com','a.qayyas@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='insert_digital_vas_by_msisdn',
    default_args=args,
    schedule_interval='0 7 * * * ',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

digital_vas_by_msisdn_p1 = BashOperator(
     task_id='insert_digital_vas_by_msisdn_p1' ,
     bash_command='bash /nas/share05/scripts/DIGITAL_VAS_BY_MSISDN/DIGITAL_VAS_BY_MSISDN_P1.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

digital_vas_by_msisdn_p2 = BashOperator(
     task_id='insert_digital_vas_by_msisdn_p2',
     bash_command='bash /nas/share05/scripts/DIGITAL_VAS_BY_MSISDN/DIGITAL_VAS_BY_MSISDN_P2.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)



digital_vas_by_msisdn_p1 >> digital_vas_by_msisdn_p2
