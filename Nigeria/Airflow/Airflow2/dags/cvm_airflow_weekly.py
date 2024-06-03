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

args  = {
    'owner': 'MTN Nigeria CVM',
  #'start_date': airflow.utils.dates.days_ago(1),
    'email': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success':True,      
    'retries': 5,
  #'retry_delay': timedelta(minutes = 5),
  #'catchup':False,
  #'queue': 'bash_queue',
  #'pool': 'backfill',
  #'priority_weight': 10,
  #'end_date': datetime(2016, 1, 1),
  #'wait_for_downstream': False,
  #'dag': dag,
  #'sla': timedelta(hours = 2),
  #'execution_timeout': timedelta(seconds = 300),
  #'on_failure_callback': some_function,
  #'on_success_callback': some_other_function,
  #'on_retry_callback': another_function,
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(8),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
dag = DAG(
    'cvm_Weekly_BOBR_datamart',
    default_args=args,
    description='To insert records into cvm datamart weekly',
    schedule_interval='0 7 * * 1',
    catchup=False,
    concurrency=3,
    max_active_runs=1
    )
input_validation = BashOperator(
    task_id='input_validation',
    bash_command= 'bash /nas/share05/scripts/cvm/cvm_weekly/input_validation.sh ',
    dag=dag,
    run_as_user='daasuser')
bridges_validation = BashOperator(
    task_id='bridges_validation',
    bash_command= 'bash /nas/share05/scripts/cvm/cvm_weekly/bridges_validation.sh  ',
    dag=dag,
    run_as_user='daasuser')
active_subs = BashOperator(
    task_id='active_subs',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/active_subs.sh  ',
    dag=dag,
    run_as_user='daasuser')
get_recharge_data = BashOperator(
    task_id='get_recharge_data',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/rech.sh  ',
    dag=dag,
    run_as_user='daasuser')
get_revenue_data = BashOperator(
    task_id='get_revenue_data',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/rev.sh  ',
    dag=dag,
    run_as_user='daasuser')
get_rgs_data = BashOperator(
    task_id='get_rgs_data',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/rgs.sh  ',
    dag=dag,
    run_as_user='daasuser')
get_usage_data= BashOperator(
    task_id='get_usage_data',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/usg.sh  ',
    dag=dag,
    run_as_user='daasuser')
get_balance_amont = BashOperator(
    task_id='get_balance_amont',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/bal_amt.sh  ',
    dag=dag,
    run_as_user='daasuser')
get_data_balance = BashOperator(
    task_id='get_data_balance',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/bal_data.sh  ',
    dag=dag,
    run_as_user='daasuser')
pref_Channel = BashOperator(
    task_id='pref_Channel',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/pref_channel.sh  ',
    dag=dag,
    run_as_user='daasuser')
bobr = BashOperator(
    task_id='bobr',
    bash_command='bash /nas/share05/scripts/cvm/cvm_weekly/bobr.sh  ',
    dag=dag,
    run_as_user='daasuser')
input_validation >> active_subs >> [get_data_balance,get_balance_amont,pref_Channel,get_recharge_data,get_revenue_data,get_rgs_data,get_usage_data] >> bridges_validation >> bobr
