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
    'depends_on_past':False,
    'start_date': seven_days_ago,
    'email': 'joshua.avbuere@mtn.com',
    'email_on_failure': 'joshua.avbuere@mtn.com',
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=3),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='Banks_USSD_logs',
          default_args=args,
          schedule_interval='10 8 * * *')
def init_dag():
    print('Banks USSD logs extracted initializing...')

def end_dag():
    print('Banks USSD logs extracted succesfully')

with dag:
    extract_ussd_bc_logs = BashOperator(task_id='extract_ussd_bc',bash_command='bash /nas/share05/dataOps_prod/ussd_logs_generator/iter_ussd_gen_v1.sh ',dag=dag,run_as_user = 'daasuser')

    extract_consent_ussd_logs = BashOperator(task_id='extract_ussd_consent',bash_command='bash /nas/share05/dataOps_prod/ussd_logs_generator/iter_consent_gen.sh ',dag=dag,run_as_user = 'daasuser')

    recon_dag = BashOperator(task_id='recon',bash_command='echo recon ',dag=dag,run_as_user = 'daasuser')

    ussd_summary = BashOperator(task_id='summary',bash_command='bash /nas/share05/dataOps_prod/ussd_logs_generator/summary_oneoff_v3.sh `date --date="-1 days" +%Y%m%d` ',dag=dag,run_as_user = 'daasuser')

    end_dag = PythonOperator(task_id='end_dag',python_callable=end_dag)

    init_dag = PythonOperator(task_id='init_dag',python_callable=init_dag)


    # Set the dependencies for both possibilities
    init_dag >> recon_dag >> [extract_ussd_bc_logs,extract_consent_ussd_logs,ussd_summary] >> end_dag

