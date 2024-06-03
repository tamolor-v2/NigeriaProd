import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='Ayoba_Daily_Usage_report',
          default_args=args,
          schedule_interval='0 5,7 * * *')


with dag:
	ayoba_daily_rpt = BashOperator(task_id='Ayoba_daily_report',bash_command='bash /nas/share05/dataOps_prod/ayoba_util_report/ayoba_util_report.sh `date --date="-1 days" +%Y%m%d` && bash /nas/share05/dataOps_prod/ayoba_util_report/ayoba_util_report.sh `date --date="-2 days" +%Y%m%d` && bash /nas/share05/dataOps_prod/ayoba_util_report/ayoba_util_report.sh `date --date="-3 days" +%Y%m%d`  ')
	# Set the dependencies for both possibilities
	ayoba_daily_rpt
	