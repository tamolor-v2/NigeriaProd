import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.executors.celery_executor import CeleryExecutor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime,timedelta
from airflow.operators.subdag import SubDagOperator

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
date_param_d0 = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')


dag = DAG(dag_id='status_test',
      	default_args=args,
        schedule_interval=None,
        catchup=True,
    	concurrency=4,
        max_active_runs=1
          )
        

     
subpred_check = BashOperator(task_id='subpred_check',bash_command='python3.6 /nas/share05/ops/mtnops/subpred_check.py ' ,      dag=dag,
     run_as_user = 'daasuser')
     



subpred_check
