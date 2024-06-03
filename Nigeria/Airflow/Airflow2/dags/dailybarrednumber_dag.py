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
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='dailybarrednumber_dag',
      	default_args=args,
        schedule_interval='45 4 * * *',
        catchup=False,
    	concurrency=1,
        max_active_runs=1
          )

step_one = BashOperator(task_id='stage1',bash_command='bash /nas/share05/ops/dev/Dailybarrednumbers.sh `date --date="-1 days" +%Y%m%d`	',     dag=dag,
     run_as_user = 'daasuser')
end_dag = BashOperator(task_id='end_dag',bash_command='echo end_dag ',      dag=dag,
     run_as_user = 'daasuser')

step_one >> end_dag

