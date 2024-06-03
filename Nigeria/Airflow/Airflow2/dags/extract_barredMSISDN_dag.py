import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

one_day_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': one_day_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='EDA_barred_msisdn',
          default_args=args,
          schedule_interval='0 21 * * *',
          max_active_runs=1)


with dag:
  fetch_msisdn = BashOperator(task_id='fetch_barred_msisdn_eda',bash_command='python3.6 /nas/share05/dataOps_prod/EDA_barredMSISDN/EDA_barredMSISDN_extract.py `date --date="-0 days" +%Y%m%d` ',run_as_user = 'daasuser')
  barred_msisdn = BashOperator(task_id='extract_barred_msisdn_eda',bash_command='bash /nas/share05/dataOps_prod/EDA_barredMSISDN/Ingest_barredMSISDN.sh `date --date="-0 days" +%Y%m%d` ',run_as_user = 'daasuser')
  ingest_EDA = BashOperator(task_id='ingest_EDA',bash_command='python3.6 /nas/share05/dataOps_prod/pseudo_loader.py `date --date="-1 days" +%Y%m%d` ',run_as_user = 'daasuser')
  # Set the dependencies for both possibilities
  fetch_msisdn >> barred_msisdn >> ingest_EDA
  