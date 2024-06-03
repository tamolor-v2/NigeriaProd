import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,timedelta
#import datetime

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': ['olorunsegun.adeniyi@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['olorunsegun.adeniyi@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

# instantiate dag
dag = DAG(dag_id='market_share',
      	default_args=args,
        schedule_interval='33 3 * * *',
        catchup=True,
    	concurrency=3,
        max_active_runs=3
          )

phase_one = BashOperator(task_id='stage1',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_1',      dag=dag,
     run_as_user = 'daasuser')
phase_two = BashOperator(task_id = 'stage2',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_2 ',      dag=dag,
     run_as_user = 'daasuser')
phase_three = BashOperator(task_id = 'stage3',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_3 ',      dag=dag,
     run_as_user = 'daasuser')
phase_four = BashOperator(task_id='stage4',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_4 ',      dag=dag,
     run_as_user = 'daasuser')
phase_fourb = BashOperator(task_id='stage4b',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_B4 ',      dag=dag,
     run_as_user = 'daasuser')
phase_fourc = BashOperator(task_id='stage4c',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_C4 ',      dag=dag,
     run_as_user = 'daasuser')
phase_five = BashOperator(task_id='stage5',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_5 ',      dag=dag,
     run_as_user = 'daasuser')
phase_six = BashOperator(task_id='stage6',bash_command='/nas/share05/ops/mtnops/mshare_unknown.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MSHARE_UNKNOWN ',      dag=dag,
     run_as_user = 'daasuser')
phase_seven = BashOperator(task_id='stage7',bash_command='/nas/share05/ops/mtnops/mshare_ops_dashboard.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MSHARE_VALIDATION ',      dag=dag,
     run_as_user = 'daasuser')
join = DummyOperator(task_id='join',trigger_rule='all_success',      dag=dag,
     run_as_user = 'daasuser')
end_dag = BashOperator(task_id='end_dag',bash_command='echo end_dag ',      dag=dag,
     run_as_user = 'daasuser')

# Set the dependencies for both possibilities
#kick_off_dag >> branch
#branch >> phase_one 
#branch >> phase_two 
#phase_one >> join
#phase_two >> join
#join >> phase_three >> [phase_four,phase_fourb] >> phase_five >> end_dag 
#>> phase_five >> phase_six
#[phase_four,phase_fourb] >> phase_five >> end_dag

[phase_one,phase_two] >> join >> phase_three >> [phase_fourb,phase_four,phase_fourc] >> phase_five >> phase_six >> phase_seven >> end_dag
#[phase_one,phase_two] >> join >> phase_three >> phase_four >> phase_five >> end_dag >> phase_fourb

