import airflow
import csv
import json, sys, time, shutil, gzip, os, psycopg2, fnmatch
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.operators import PythonOperator, BashOperator, EmailOperator, BranchPythonOperator

today = datetime.today()
strToday= today .strftime('%Y-%m-%d-%H')
daydate= today .strftime('%Y-%m-%d')

default_args = {
    'owner': 'Monitoring',
    'depends_on_past': False,
    'start_date': datetime(2020,4,4),
    'email': ['support@ligadata.com'],
    'email_on_failure': ['support@ligadata.com'],
    'email_on_retry': True,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG('Bandwidth_All_Nodes',
          default_args=default_args,
          schedule_interval=' 0 * * * * ',
          catchup=False,
          concurrency=2,
          max_active_runs=1
          )



run_check = BashOperator(
    task_id='run_check',
    bash_command='bash /nas/share05/tools/Bandwidth_Remote_Testing/bin/Run.sh ',
    dag=dag,
    run_as_user='daasuser'
    )

success = BashOperator(
    task_id='success',
    bash_command='echo success ',
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
    )

run_check >> success
