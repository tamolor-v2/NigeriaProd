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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d') 

dag = DAG(
    dag_id='Agiliy_Data_Ingestion_1',
    default_args=args,
    schedule_interval='0 3 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)
#infraction = BashOperator(
#     task_id='infraction' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p INFRACTION',
#     dag=dag,
#)

Article_report = BashOperator(
     task_id='Agility_Article_report' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p ARTICLE_RPT',
     dag=dag,
     run_as_user = 'daasuser'
)

CB_USERS = BashOperator(
     task_id='CB_USERS' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p CB_USERS',
     dag=dag,
     run_as_user = 'daasuser'
)
   
Compensation = BashOperator(
     task_id='Compensation' ,
     bash_command='bash /nas/share05/ops/mtnops/upload_comp.sh `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

Cr_Adjustment = BashOperator(
     task_id='Credit_Adjustment' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p CR_ADJ',
     dag=dag,
     run_as_user = 'daasuser'
)

#credit_control = BashOperator(
#     task_id='credit_control' ,
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p CREDIT_CONTROL',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

CREDIT_INFO = BashOperator(
     task_id='CREDIT_INFO' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p CREDIT_INFO',
     dag=dag,
     run_as_user = 'daasuser'
)

D_Connect = BashOperator(
     task_id='D_Connect' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p d_connect',
     dag=dag,
     run_as_user = 'daasuser'
)

SERV_CONECT = BashOperator(
     task_id='SERV_CONECT' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p SERV_CONECT',
     dag=dag,
     run_as_user = 'daasuser'
)

infraction2 = BashOperator(
     task_id='infraction2' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p INFRACTION -s {0} -l 8'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

debit_adjustment = BashOperator(
     task_id='debit_adjustment' ,
     bash_command='/nas/share05/ops/mtnops/upload_debitadj.sh  `date --date="-1 month" +%Y%m`	',
     dag=dag,
     run_as_user = 'daasuser'
)



#Db_Adjustment = BashOperator(
#     task_id='Debit_ADJ' ,
#     bash_command='python3.6 /nas/share05/ops/mtnops/data_ingestion.py -p DB_ADJ',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


infraction2 >> D_Connect >> CREDIT_INFO >> Cr_Adjustment >>  Compensation >> debit_adjustment >> CB_USERS >> Article_report >> SERV_CONECT #>> credit_control
#>> Db_Adjustment >>  Compensation >> CB_USERS >> Article_report >> SERV_CONECT
