import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'CLM',
    'depends_on_past':False,
    'start_date': datetime(2019,10,27),
    'email': ['test@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('clm_provision_success_file.sh-0.py',
          default_args=default_args,
#          schedule_interval=' 35 7 * * *  ',
          schedule_interval=None,
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='bash /mnt/beegfs_bsl/scripts/clm_cvm/clm_provision_success_file.sh ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
