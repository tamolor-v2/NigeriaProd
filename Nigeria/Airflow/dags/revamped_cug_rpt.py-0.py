import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator,PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'split',
    'depends_on_past': False,
    'start_date': datetime(2019,10,27),
    'email': ['support@ligadata.com'],
    'email_on_failure': ['support@ligadata.com'],
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('revamped_cug_rpt.py-0.py',
          default_args=default_args,
          schedule_interval=' 0 6 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='python3.6 /nas/share05/dataOps_dev/revamped_cug/revamped_cug_rpt.py ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
