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
    'email': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d') 

dag = DAG(
    dag_id='INFRACTION',
    default_args=args,
    schedule_interval='0 20 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)
#infraction = BashOperator(
#     task_id='infraction' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p INFRACTION',
#     dag=dag,
#)


#credit_control = BashOperator(
#     task_id='credit_control' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p CREDIT_CONTROL',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


infraction2 = BashOperator(
     task_id='infraction2' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p INFRACTION -s {0} -l 2'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)



infraction2 
#>> D_Connect >> CREDIT_INFO >> Cr_Adjustment >>  Compensation >> debit_adjustment >> CB_USERS >> Article_report >> SERV_CONECT #>> credit_control
#>> Db_Adjustment >>  Compensation >> CB_USERS >> Article_report >> SERV_CONECT
