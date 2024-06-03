import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator,PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'minitar',
    'depends_on_past': False,
    'start_date': datetime(2019,10,27),
    'email': ['support@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('minitar_7.sh-0.py',
          default_args=default_args,
          schedule_interval=' */45 * * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='bash /nas/share05/tools/SupportTeam/minitar_cron/minitar_7.sh ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
