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
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Agiliy_Data_Ingestion',
    default_args=args,
    schedule_interval='0 1 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

REG_PER_DEVICE = BashOperator(
     task_id='REG_PER_DEVICE' ,
     depends_on_past=False,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p REG_PER_DEVICE',
     dag=dag,
     run_as_user = 'daasuser'
)
   
#DEALER_LISTS = BashOperator(
#     task_id='DEALER_LISTS' ,
#     depends_on_past=False,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p DEALER_LISTS',
#     dag=dag,
#     run_as_user = 'daasuser'
#)
   
#PAY_SUMMARY = BashOperator(
#     task_id='PAY_SUMMARY' ,
#     depends_on_past=False,
#     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p PAY_SUMMARY',
#     dag=dag,
     #run_as_user = 'daasuser'
#)

#SERVICE_INVOICE = BashOperator(
#     task_id='SERVICE_INVOICE' ,
#     depends_on_past=False,
#     bash_command='python3.6 /nas/share05/ops/mtnops/data_ingestion.py -p SERVICE_INVOICE',
#     dag=dag,
#     run_as_user = 'daasuser'
#)
   
AGEING = BashOperator(
     task_id='AGEING' ,
     depends_on_past=False,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p AGEING',
     dag=dag,
     run_as_user = 'daasuser'
)
   
#BIOUPDT = BashOperator(
#     task_id='BIOUPDT' ,
#     depends_on_past=False,
#     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p BIOUPDT',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


#SRTT_CLOSED = BashOperator(
#     task_id='LOAD_DSSA_SRTT_CLOSED' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SRTT_CLOSED -l 1 -s `date --date="-1 days" +%Y-%m-%d`',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#SRTT_CREATED = BashOperator(
#     task_id='LOAD_DSSA_SRTT_CREATED' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SRTT_CREATED -l 1 -s `date --date="-1 days" +%Y-%m-%d`',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#HSDP_Lookup = BashOperator(
 #    task_id='HSDP_Lookup' ,
 #   bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p HSDP_LOOKUP',
 #  dag=dag,
 # run_as_user = 'daasuser'
#)

REG_PER_DEVICE >> AGEING #>> DEALER_LISTS
#PAY_SUMMARY >> SERVICE_INVOICE >> AGEING >> BIOUPDT >> COMEBACK_15M >> COMEBACK_REALTIME 
#>> REG_PER_DEVICE >> SMF_ALL_MAND  >> SRTT_CLOSED >> SRTT_CREATED >> HSDP_Lookup >> BIOUPDT
