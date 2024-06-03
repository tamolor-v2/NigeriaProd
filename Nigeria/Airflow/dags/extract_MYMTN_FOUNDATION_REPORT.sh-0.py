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
    'email': ['a.olabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('extract_MYMTN_FOUNDATION_REPORT.sh-0.py',
          default_args=default_args,
          schedule_interval=' 0 8 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='bash /nas/share05/tools/ExtractTools/MYMTN_FOUNDATION_REPORT/extract_MYMTN_FOUNDATION_REPORT.sh ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
