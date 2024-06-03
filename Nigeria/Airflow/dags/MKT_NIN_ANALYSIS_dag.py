import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='MKT_NIN_analysis',
          default_args=args,
          schedule_interval='15 8 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


with dag:
  kick_off_dag = BashOperator(task_id='Initialize_NIN_analysis',bash_command='echo "kick Off Dag..." ',  dag=dag, run_as_user = 'daasuser')
  check_5by5 = BashOperator(task_id='check_5by5' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/check_5by5.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task1 = BashOperator(task_id='Task1' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_1.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task2 = BashOperator(task_id='Task2' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_2.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task3 = BashOperator(task_id='Task3' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_3.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task4 = BashOperator(task_id='Task4' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_4.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task5 = BashOperator(task_id='Task5' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_5.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task6 = BashOperator(task_id='Task6' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_6.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task7 = BashOperator(task_id='Task7' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_7.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task8 = BashOperator(task_id='Task8' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_8.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  Task9 = BashOperator(task_id='Task9' ,bash_command='bash /nas/share05/dataOps_prod/mkt_nin_process/Task_9.sh `date --date="-1 days" +%Y%m%d` `date --date="-2 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')

  End_dag = BashOperator(task_id='End_DAG',bash_command='echo "End NIN Analysis DAG..." ',  dag=dag, run_as_user = 'daasuser')
  kick_off_dag >>check_5by5 >>  [Task1,Task2,Task3,Task4,Task5,Task6,Task7,Task8,Task9] >> End_dag
