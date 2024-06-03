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


dag = DAG('wbs_d2_d1.py',
          default_args=default_args,
          schedule_interval=' 0 3 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

wbs_d2_d1 = BashOperator(
    task_id='wbs_d2_d1',
    bash_command='bash /nas/share05/tools/ExtractTools/WBS_REPORT/extract_WBS_REPORT_OneDate.sh `date --date="-2 days" +%Y%m%d` & bash /nas/share05/tools/ExtractTools/WBS_REPORT/extract_WBS_REPORT_OneDate.sh  `date --date="-1 days" +%Y%m%d`',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)


