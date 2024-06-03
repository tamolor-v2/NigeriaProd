from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['n.najjar@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['n.najjar@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Extract_DCBS_FEEDS_monthly',
    default_args=args,
    schedule_interval='0 5 5 * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

extract_17_DCBS_REPORTS_TAX_INVOICE_VW_monthly_yest = BashOperator(
     task_id='extract_17_DCBS_REPORTS_TAX_INVOICE_VW_monthly_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_17_DCBS_REPORTS_TAX_INVOICE_VW_monthly_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_18_DCBS_REPORTS_ACCOUNT_SUMMARY_monthly_yest = BashOperator(
     task_id='extract_18_DCBS_REPORTS_ACCOUNT_SUMMARY_monthly_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_18_DCBS_REPORTS_ACCOUNT_SUMMARY_monthly_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_02_DCBS_REPORTS_FIFO_SERV_AGEING_REPORT_VW_monthly_yest = BashOperator(
     task_id='extract_02_DCBS_REPORTS_FIFO_SERV_AGEING_REPORT_VW_monthly_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_02_DCBS_REPORTS_FIFO_SERV_AGEING_REPORT_VW_monthly_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_17_DCBS_REPORTS_TAX_INVOICE_VW_monthly_yest >> extract_18_DCBS_REPORTS_ACCOUNT_SUMMARY_monthly_yest >> extract_02_DCBS_REPORTS_FIFO_SERV_AGEING_REPORT_VW_monthly_yest