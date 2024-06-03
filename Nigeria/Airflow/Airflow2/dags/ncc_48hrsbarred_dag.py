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
dag = DAG(dag_id='ncc_48hrsbarred_dag',
      	default_args=args,
#        schedule_interval='0 10 * * *',
        schedule_interval=None,
        catchup=False,
    	concurrency=1,
        max_active_runs=1
          )

step_one = BashOperator(task_id='stage1',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p tmp_daily_barred_numbers -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_two = BashOperator(task_id='stage2',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p tmp_daily_barred_numbers_2 -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_three = BashOperator(task_id='stage3',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p bar_notinsc -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_four = BashOperator(task_id='stage4',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p daily_barred_numbers -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_five = BashOperator(task_id='stage5',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p june_barring -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_six = BashOperator(task_id='stage6',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p june_barring3 -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_seven = BashOperator(task_id='stage7',bash_command='/nas/share05/ops/dev/ncc48hrsbarred.py -p final_ncc48tbl -l 1 -s `date --date="-3 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_eight = BashOperator(task_id='stage8' ,bash_command='ssh edge01001 bash /nas/share05/ops/dev/ncc48hrsbarred.sh `date --date="-3 days" +%Y%m%d`	',
     run_as_user = 'daasuser',  dag=dag,)
end_dag = BashOperator(task_id='end_dag',bash_command='echo end_dag ',      dag=dag,
     run_as_user = 'daasuser')

step_one >> step_two >> step_three >> step_four >> step_five >> step_six >> step_seven >> step_eight >> end_dag

