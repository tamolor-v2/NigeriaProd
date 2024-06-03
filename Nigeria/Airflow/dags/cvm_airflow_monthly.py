import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta, date
default_args = {
  'owner': 'daasuser',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(30),
  #'start_date': datetime.datetime(2020, 6, 1),
  'email': ['support@ligadata.com','m.shekha@ligadata.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'email_on_success':True,      
  'retries': 1,
  'retry_delay': timedelta(minutes=1),
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
}



dag = DAG(
    'cvm_Monthly_BOBR_datamart',
    default_args=default_args,
    description='To insert records into cvm datamart Monthly',
    schedule_interval='0 7 1 * *') 
input_validation = BashOperator(
    task_id='input_validation',
    bash_command= 'bash /nas/share05/scripts/cvm/cvm_monthly/input_validation.sh  ',
    dag=dag,
    run_as_user='daasuser')
bridges_validation = BashOperator(
    task_id='bridges_validation',
    bash_command= 'bash /nas/share05/scripts/cvm/cvm_monthly/bridges_validation.sh ',
    dag=dag,
    run_as_user='daasuser')
cmd_actsubs = ' bash /nas/share05/scripts/cvm/cvm_monthly/active_subs.sh yest '
active_subs = BashOperator(
    task_id='active_subs',
    bash_command=cmd_actsubs,
    dag=dag,
    run_as_user='daasuser')
cmd_rech = ' bash /nas/share05/scripts/cvm/cvm_monthly/rech.sh '
rech = BashOperator(
    task_id='rech',
    bash_command=cmd_rech,
    dag=dag,
    run_as_user='daasuser')
cmd_rev = ' bash /nas/share05/scripts/cvm/cvm_monthly/rev.sh '
rev = BashOperator(
    task_id='rev',
    bash_command=cmd_rev,
    dag=dag,
    run_as_user='daasuser')
cmd_rgs = ' bash /nas/share05/scripts/cvm/cvm_monthly/rgs.sh '
rgs = BashOperator(
    task_id='rgs',
    bash_command=cmd_rgs,
    dag=dag,
    run_as_user='daasuser')
cmd_usg = ' bash /nas/share05/scripts/cvm/cvm_monthly/usg.sh '
usg= BashOperator(
    task_id='usg',
    bash_command=cmd_usg,
    dag=dag,
    run_as_user='daasuser')
cmd_balance= ' bash /nas/share05/scripts/cvm/cvm_monthly/balance.sh '
balance = BashOperator(
    task_id='balance',
    bash_command=cmd_balance,
    dag=dag,
    run_as_user='daasuser')
cmd_data_balance= ' bash /nas/share05/scripts/cvm/cvm_monthly/data_balance.sh '
data_balance= BashOperator(
    task_id='data_balance',
    bash_command=cmd_data_balance,
    dag=dag,
    run_as_user='daasuser')
cmd_bobr = ' bash /nas/share05/scripts/cvm/cvm_monthly/bobr.sh '
bobr = BashOperator(
    task_id='bobr',
    bash_command=cmd_bobr,
    dag=dag,
    run_as_user='daasuser')

input_validation >> active_subs >> [rech,rev,rgs,usg,balance,data_balance] >> bridges_validation >>  bobr 
