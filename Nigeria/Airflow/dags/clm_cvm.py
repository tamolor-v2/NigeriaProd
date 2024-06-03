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
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
  
dag = DAG(
    dag_id='CLM_CVM',
    default_args=args,
    schedule_interval='0 8,11,14 * * *',
    catchup=False,
    concurrency=5,
    max_active_runs=5
)

CHECK_SUB = BashOperator(
     task_id='CHECK_SUB' ,
     bash_command='bash /nas/share05/ops/mtnops/clm_cvm_sub_check.sh `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
)

CLM_ASH_DETAILS = BashOperator(
     task_id='CLM_ASH_DETAILS' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_ASH_DETAILS -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_ASH_SUMMARY = BashOperator(
     task_id='CLM_ASH_SUMMARY' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_ASH_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_INBOUND_DETAILS = BashOperator(
     task_id='CLM_INBOUND_DETAILS' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_INBOUND_DETAILS -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_PROVISION_DETAILS = BashOperator(
     task_id='CLM_PROVISION_DETAILS' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_PROVISION_DETAILS -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_PROVISION_SMS_DELIVERY = BashOperator(
     task_id='CLM_PROVISION_SMS_DELIVERY' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_PROVISION_SMS_DELIVERY -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_LATCHING_DASHBOARD = BashOperator(
     task_id='CLM_LATCHING_DASHBOARD' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_LATCHING_DASHBOARD -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_PROVISION_SUCCESS_FILE = BashOperator(
     task_id='CLM_PROVISION_SUCCESS_FILE' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_PROVISION_SUCCESS_FILE -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_PROVISION_SUMMARY = BashOperator(
     task_id='CLM_PROVISION_SUMMARY' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_PROVISION_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_PROVISION_WBO_ELIGIBLE_DETAIL = BashOperator(
     task_id='CLM_PROVISION_WBO_ELIGIBLE_DETAIL' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_PROVISION_WBO_ELIGIBLE_DETAIL -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)

CLM_PROVISION_WBO_ELIGIBLE_SUMMARY = BashOperator(
     task_id='CLM_PROVISION_WBO_ELIGIBLE_SUMMARY' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CLM_PROVISION_WBO_ELIGIBLE_SUMMARY -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
),

CUST_RETENTION_LATCH_CUBE = BashOperator(
     task_id='CUST_RETENTION_LATCH_CUBE' ,
     bash_command='/nas/share05/ops/mtnops/clm_cvm.py -p CUST_RETENTION_LATCH_CUBE -s `date --date="-1 days" +%Y-%m-%d` -l 3',
     dag=dag,
)


CHECK_SUB >> CLM_PROVISION_DETAILS >> [CLM_PROVISION_SMS_DELIVERY, CLM_LATCHING_DASHBOARD] >> CLM_PROVISION_SUCCESS_FILE >> CLM_PROVISION_SUMMARY 
CHECK_SUB >> CLM_ASH_DETAILS >> CLM_ASH_SUMMARY >> CLM_INBOUND_DETAILS
CHECK_SUB >> CLM_PROVISION_WBO_ELIGIBLE_DETAIL >> CLM_PROVISION_WBO_ELIGIBLE_SUMMARY 
CHECK_SUB >> CUST_RETENTION_LATCH_CUBE
