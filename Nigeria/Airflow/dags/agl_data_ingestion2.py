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
    dag_id='Agiliy_Data_Ingestion_2',
    default_args=args,
    schedule_interval='0 2 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

ARRIVAL_DATE = BashOperator(
     task_id='ARRIVAL_DATE' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p ARRIVAL_DATE',
     dag=dag,
     run_as_user = 'daasuser'
)

PAYMENT_ALL = BashOperator(
     task_id='PAYMENT_ALL' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p PAYMENT_ALL',
     dag=dag,
     run_as_user = 'daasuser'
)

Post_Paid_Report = BashOperator(
     task_id='Post-Paid-Report' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p POST_PAID',
     dag=dag,
     run_as_user = 'daasuser'
)
    
Service_Wise_Rpt = BashOperator(
     task_id='Service_Wise_Rpt' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SERV_WISE',
     dag=dag,
     run_as_user = 'daasuser'
)
  
SIM_SWOP = BashOperator(
     task_id='SIM_SWOP' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SIM_SWOP -l 5',
     dag=dag,
     run_as_user = 'daasuser'
)

Subscriber_Service = BashOperator(
     task_id='Subscriber_Service' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SUB_SERV_WISE',
     dag=dag,
     run_as_user = 'daasuser'
)

SUB_SERV_WISE_TBL = BashOperator(
     task_id='SUB_SERV_WISE_TBL' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SUB_SERV_WISE_TBL',
     dag=dag,
     run_as_user = 'daasuser'
)

VISAFONE_Revenue = BashOperator(
     task_id='VISAFONE_Revenue' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p VISAFONE_REV',
     dag=dag,
     run_as_user = 'daasuser'
)

#Email_Registration = BashOperator(
#     task_id='Email_Registration' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p bib_email',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

serageing_details = BashOperator(
     task_id='serageing_details' ,
     depends_on_past=False,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p AGEING',
     dag=dag,
     run_as_user = 'daasuser'
)
ARRIVAL_DATE >>  PAYMENT_ALL >> Service_Wise_Rpt >> SIM_SWOP >> Subscriber_Service >> SUB_SERV_WISE_TBL >> VISAFONE_Revenue >> serageing_details >> Post_Paid_Report #>> Email_Registration
