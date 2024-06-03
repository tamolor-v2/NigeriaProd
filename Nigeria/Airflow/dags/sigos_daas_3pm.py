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
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','ayodeji.shadare@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='sigos_daas_3pm',
    default_args=args,
    schedule_interval='5 15 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

sigos_daas_msc_cdr = BashOperator(
     task_id='sigos_daas_msc_cdr' ,
     bash_command='bash /nas/share05/ops/daily/sigos_daas_msc_cdr_03pm.sh  `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` ',
     dag=dag,
     run_as_user = 'daasuser'
)

sigos_daas_ggsn_cdr = BashOperator(
     task_id='sigos_daas_ggsn_cdr' ,
     bash_command='bash /nas/share05/ops/daily/sigos_daas_ggsn_cdr_03pm.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)

sigos_daas_cdr_rated = BashOperator(
     task_id='sigos_daas_cdr_rated' ,
     bash_command='bash /nas/share05/ops/daily/sigos_daas_cdr_rated_03pm.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)




sigos_daas_msc_cdr >> sigos_daas_ggsn_cdr >> sigos_daas_cdr_rated
