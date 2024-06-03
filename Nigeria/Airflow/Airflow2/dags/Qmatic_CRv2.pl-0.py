import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'split',
    'depends_on_past':False,
    'start_date': datetime(2019,10,27),
    'email': ['test@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('Qmatic_CRv2.pl-0.py',
          default_args=default_args,
#          schedule_interval=' 0 9 * * *  ',
          schedule_interval=None,
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='perl /mnt/beegfs_bsl/scripts/qmatic_report/Qmatic_CRv2.pl `date --date="-1 days" +%Y%m%d` ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
