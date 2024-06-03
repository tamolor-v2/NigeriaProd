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
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com','o.olanipekun@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Agiliy_Data_Ingestion_3',
    default_args=args,
    schedule_interval='0 0 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)


BILL_SUMMARY = BashOperator(
     task_id='LOAD_BILL_SUMMARY' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p BILL_SUMMARY',
     dag=dag,
     run_as_user = 'daasuser'
)
   
BIZ_PLAN = BashOperator(
     task_id='Business_plan' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p BIZ_PLAN',
     dag=dag,
     run_as_user = 'daasuser'
)
   
#Product_List = BashOperator(
#     task_id='HSDP_Product_List' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p product_list',
#     dag=dag,
#     run_as_user = 'daasuser'
#)
   
OBLIGOR = BashOperator(
     task_id='LOAD_DSSA_OBLIGOR' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p OBLIGOR',
     dag=dag,
     run_as_user = 'daasuser'
)

#VAS_SUBS = BashOperator(
#     task_id='VAS_SUBS' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p VAS_SUBS',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

SERV_WISE = BashOperator(
     task_id='SERV_WISE' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SERV_WISE',
     dag=dag,
     run_as_user = 'daasuser'
)

GSM_FLX = BashOperator(
     task_id='GSM_FLX' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p FXL_STS',
     dag=dag,
     run_as_user = 'daasuser'
)
    
#GSM_STS = BashOperator(
#     task_id='GSM_STS' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p GSM_STS',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#COMEBACK_15M = BashOperator(
#     task_id='COMEBACK_15M' ,
#     depends_on_past=False,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p COMEBACK_15M',
#     dag=dag,
#     run_as_user = 'daasuser'
#)
   
#COMEBACK_REALTIME = BashOperator(
#     task_id='COMEBACK_REALTIME' ,
#     depends_on_past=False,
#     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p COMEBACK_REALTIME',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#SMF_ALL_MAND = BashOperator(
#     task_id='SMF_ALL_MAND_FIELDS' ,
#     depends_on_past=False,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SMF_ALL_MAND',
#     dag=dag,
#     run_as_user = 'daasuser'
#) 
   
GSM_FLX >> SERV_WISE >> OBLIGOR >> BIZ_PLAN >> BILL_SUMMARY

#COMEBACK_15M >> SMF_ALL_MAND >> COMEBACK_REALTIME >> VAS_SUBS >> Product_List
