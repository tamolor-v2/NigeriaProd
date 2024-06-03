import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,timedelta
#import datetime


args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'email': ['olorunsegun.adeniyi@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['olorunsegun.adeniyi@mtn.com','o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
    'start_date': airflow.utils.dates.days_ago(0),
}

# instantiate dag
dag = DAG(dag_id='market_share_5',
      	default_args=args,
#        schedule_interval='0 15 * * *',
        schedule_interval=None,
        catchup=True,
    	concurrency=1,
        max_active_runs=1
          )

phase_one = BashOperator(task_id='stage5',bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s `date --date="-1 days" +%Y-%m-%d` -p MKSHARE_5 ',      dag=dag,
     run_as_user = 'daasuser')
#join = DummyOperator(task_id='join',trigger_rule='all_success',      dag=dag,
#     run_as_user = 'daasuser')
#end_dag = BashOperator(task_id='end_dag',bash_command='echo end_dag ',      dag=dag,
#     run_as_user = 'daasuser')

# Set the dependencies for both possibilities
#kick_off_dag >> branch
#branch >> phase_one 
#branch >> phase_two 
#phase_one >> join
#phase_two >> join
#join >> phase_three >> phase_four 
#>> phase_five >> phase_six
#phase_four >> phase_five >> end_dag
phase_one
