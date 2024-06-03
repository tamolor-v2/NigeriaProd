import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.branchpython_operator import BranchPythonOperator
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
    'retries': 4,
    'retry_delay': timedelta(minutes=3),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='Upgrade4G_job',
          default_args=args,
          schedule_interval='10 10 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
date_param2 = (datetime.now() - timedelta(days=0)).strftime('%Y%m%d')


def end_dag():
    print('4G Upgrade report refreshed succesfully for {0}'.format(date_param))

with dag:
    kick_off_4GUpgrade = BashOperator(task_id='kick_off_4GUpgrade',bash_command='python3.6 /nas/share05/dataOps_prod/Upgrade_4G/Upgrade4G_job.py `date --date="-1 days" +%Y%m%d` '.format(date_param),dag=dag,run_as_user = 'daasuser')

    D_4GUpgrade = BashOperator(task_id='D_4GUpgrade',bash_command='python3.6 /nas/share05/dataOps_prod/Upgrade_4G/Upgrade4G_job.py `date --date="-0 days" +%Y%m%d` '.format(date_param2),dag=dag,run_as_user = 'daasuser')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)


    # Set the dependencies for both possibilities
    D_4GUpgrade >> kick_off_4GUpgrade >> end_dag

