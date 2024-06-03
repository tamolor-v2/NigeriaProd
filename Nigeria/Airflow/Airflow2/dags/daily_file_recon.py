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
dag = DAG(dag_id='daily_file_recon',
      	default_args=args,
        schedule_interval='0 3 * * *',
        catchup=False,
    	concurrency=1,
        max_active_runs=1
          )

step_one = BashOperator(task_id='stage1',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_VOICE -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_two = BashOperator(task_id='stage2',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_SMS -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_three = BashOperator(task_id='stage3',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_GPRS -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_four = BashOperator(task_id='stage4',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_ADJ_MA -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_five = BashOperator(task_id='stage5',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_REFILL_MA -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_six = BashOperator(task_id='stage6',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_MSC -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_seven = BashOperator(task_id='stage7',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_GGSN -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_eight = BashOperator(task_id='stage8',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_B4U_VOICE -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_nine = BashOperator(task_id='stage9',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p RATED_B4U_GPRS -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_nine1 = BashOperator(task_id='stage10',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p CS6_RATED_VOICE -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_nine2 = BashOperator(task_id='stage11',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p CS6_RATED_SMS -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_nine3 = BashOperator(task_id='stage12',bash_command='python3.6 /nas/share05/ops/mtnops/OT_summary.py -p CS6_RATED_GPRS -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_ten = BashOperator(task_id='stage13',bash_command='python3.6 /nas/share05/ops/mtnops/daily_reports.py -p FILE_RECON_SUMMARY -l 1 -s `date --date="0 days" +%Y-%m-%d`	',      dag=dag,
     run_as_user = 'daasuser')
step_eleven = BashOperator(task_id='stage14' ,bash_command='bash /nas/share05/ops/dev/dailyfilerecon.sh `date --date="-1 days" +%Y%m%d`	',
     run_as_user = 'daasuser',  dag=dag,)
end_dag = BashOperator(task_id='end_dag',bash_command='echo end_dag ',      dag=dag,
     run_as_user = 'daasuser')

step_one >> step_two >> step_three >> step_four >> step_five >> step_six >> step_seven >> step_eight >> step_nine >> step_nine1 >> step_nine2 >> step_nine3 >> step_ten >> step_eleven >> end_dag

