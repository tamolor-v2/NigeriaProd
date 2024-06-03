#! /usr/bin/env /usr/bin/python3.6
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,timedelta

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['gbenga.agosu@infonomicsng.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='ncc_48hrsbarred_dag2',
      	default_args=args,
        schedule_interval='0 11 * * *',
        catchup=False,
    	concurrency=1,
        max_active_runs=1
          )

step_one = BashOperator(task_id='stage1',bash_command='python3.6 /nas/share05/ops/dev/ncc48hrsbarred2.py -p num_notin_69200 -l 1 -s `date --date="-3 days" +%Y-%m-%d`	 ',      dag=dag,
     run_as_user = 'daasuser')
step_two = BashOperator(task_id='stage2',bash_command='python3.6 /nas/share05/ops/dev/ncc48hrsbarred2.py -p num_notin_nccexclu -l 1 -s `date --date="-3 days" +%Y-%m-%d`	 ',      dag=dag,
     run_as_user = 'daasuser')
step_three = BashOperator(task_id='stage3',bash_command='python3.6 /nas/share05/ops/dev/ncc48hrsbarred2.py -p not_in3537 -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_four = BashOperator(task_id='stage4',bash_command='python3.6 /nas/share05/ops/dev/ncc48hrsbarred2.py -p not_inrgs90 -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_five = BashOperator(task_id='stage5',bash_command='ssh edge01001 bash /nas/share05/ops/dev/ncc48hrsbarred2.sh `date --date="-3 days" +%Y%m%d`	',     dag=dag,
     run_as_user = 'daasuser')
end_dag = BashOperator(task_id='end_dag',bash_command='echo end_dag ',      dag=dag,
     run_as_user = 'daasuser')

step_one >> step_two >> step_three >> step_four >> step_five >> end_dag

