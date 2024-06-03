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
    'email': ['support@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('imei_submission.py',
          default_args=default_args,
          schedule_interval=' 0 6 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

imei_submission = BashOperator(
    task_id='imei_sub',
    bash_command='bash /nas/share05/scripts/imei/run_imei_submission_commission.sh  `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` 8099',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)


