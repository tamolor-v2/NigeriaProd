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
yesterday = datetime.today() - timedelta(days=1)
d_1 = yesterday.strftime('%Y%m%d')
dag = DAG('Bulk_USSD_Report.pl-0.py',
          default_args=default_args,
          schedule_interval=' 0 7 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='Command',
    bash_command='perl /mnt/beegfs_bsl/scripts/bulk_ussd/Bulk_USSD_Report.pl  {0}'.format(d_1),
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)
