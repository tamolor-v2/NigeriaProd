import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'split',
    'depends_on_past': False,
    'start_date': datetime(2019,10,27),
    'email': ['a.olabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('extract_EBU_GROUP_ESCALATIONS_CLOSED_D-1.sh-0.py',
          default_args=default_args,
          schedule_interval=' 30 3 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='bash /nas/share05/tools/ExtractTools/EBU_GROUP_ESCALATIONS_CLOSED/extract_EBU_GROUP_ESCALATIONS_CLOSED_D-1.sh ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
