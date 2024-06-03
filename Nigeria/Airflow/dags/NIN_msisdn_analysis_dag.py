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
dag = DAG(dag_id='NIN_analysis',
          default_args=args,
          schedule_interval='0 7 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


with dag:
  kick_off_dag = BashOperator(task_id='Initialize_NIN_analysis',bash_command='echo "kick Off Dag..." ',  dag=dag, run_as_user = 'daasuser')
  cumm_dag = BashOperator(task_id='cummulative_analysis' ,bash_command='bash /nas/share05/dataOps_dev/nin_msisdn_summary/daily_cumm_msisdn_count_.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  uniq_dag = BashOperator(task_id='unique_msisdn_daily' ,bash_command='bash /nas/share05/dataOps_dev/nin_msisdn_summary/daily_uniq_msisdn_count_.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  channel_dag = BashOperator(task_id='Count_per_channel' ,bash_command='bash /nas/share05/dataOps_dev/nin_msisdn_summary/daily_msisdn_count_per_channel.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  #init_nin_dag = BashOperator(task_id='Initialize_NIN_analysis2',bash_command='echo "initiate NIN centric Dag..." ',  dag=dag, run_as_user = 'daasuser')
  nin_cumm_dag = BashOperator(task_id='cummulative_analysis_on_nin' ,bash_command='bash /nas/share05/dataOps_dev/nin_msisdn_summary/daily_cumm_nin_count.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  nin_uniq_dag = BashOperator(task_id='unique_nin_daily' ,bash_command='bash /nas/share05/dataOps_dev/nin_msisdn_summary/daily_uniq_nin_count.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  nin_channel_dag = BashOperator(task_id='Count_per_channel_on_nin' ,bash_command='bash /nas/share05/dataOps_dev/nin_msisdn_summary/daily_nin_count_per_channel.sh `date --date="-1 days" +%Y%m%d` ',dag=dag, run_as_user = 'daasuser')
  End_dag = BashOperator(task_id='End_DAG',bash_command='echo "End NIN Analysis DAG..." ',  dag=dag, run_as_user = 'daasuser')
  kick_off_dag >> [cumm_dag,uniq_dag,channel_dag ,nin_cumm_dag,nin_uniq_dag,nin_channel_dag] >> End_dag
