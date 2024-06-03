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
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='mtnn_mediation_metrics_hrly',
          default_args=args,
          schedule_interval='0 */2 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


with dag:
    kick_off_dag = BashOperator(task_id='mtnn_mediation_metrics_hrly',bash_command='echo `hostname` && python3.6 /nas/share05/ops/mtnops/man_Mediation_metrics_hrly.py `date --date="-0 days" +%Y%m%d` ', dag=dag)
    sms_dag = BashOperator(task_id='med_metric_sms' ,bash_command='bash /nas/share05/ops/mtnops/hourly_med_metrics.sh  ',dag=dag)
    kick_off_dag >> sms_dag
