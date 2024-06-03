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
    'email': ['test@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('NIN_ENROLL_RECON_DEDUP',
          default_args=default_args,
          schedule_interval=' 45 8,13,16 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='bash /nas/share05/ops/mtnops/NIN_Enrollment_Validation/NIN_ENROLL_RECON_DEDUP.sh ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
