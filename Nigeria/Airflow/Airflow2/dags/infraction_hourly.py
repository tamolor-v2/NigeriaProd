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
    'email': ['oladimeji.olanipekun@mtn.com','t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d') 

dag = DAG(
    dag_id='Infraction_Hourly',
    default_args=args,
    schedule_interval='0 0,6,12,18 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

infraction_hrly = BashOperator(
     task_id='infraction_hrly' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p INFRACTION -s {0} -l 1'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)



#Db_Adjustment = BashOperator(
#     task_id='Debit_ADJ' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/data_ingestion.py -p DB_ADJ',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


infraction_hrly
#>> Db_Adjustment >>  Compensation >> CB_USERS >> Article_report >> SERV_CONECT
