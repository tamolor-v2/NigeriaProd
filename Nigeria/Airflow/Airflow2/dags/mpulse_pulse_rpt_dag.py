from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['aolabamidele@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
dag = DAG(
    dag_id='MPULSE_PULSE_REPORT',
    default_args=args,
    schedule_interval='0 10 * * *',
    description='MPULSE_PULSE_REPORT',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)
initiate_datasets = BashOperator(
     task_id='initiate_datasets' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/initiate_datasets.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
mpulse_recharges = BashOperator(
     task_id='mpulse_recharges' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpulse_recharges.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
mpulse_Usim = BashOperator(
     task_id='mpulse_Usim' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpulse_Usim.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
mpulse_ayoba = BashOperator(
     task_id='mpulse_ayoba' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpulse_ayoba.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
mpulse_myMTNAPP = BashOperator(
     task_id='mpulse_myMTNAPP' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpulse_myMTNAPP.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
mpulse_myMTNAPP_dwld = BashOperator(
     task_id='mpulse_myMTNAPP_dwld' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpulse_myMTNAPP_dwld.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
mpulse_NIN = BashOperator(
     task_id='mpulse_NIN' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpulse_NIN.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)
summarize_mpulse_rpt = BashOperator(
     task_id='summarize_mpulse_rpt' ,
     bash_command='bash /nas/share05/dataOps_prod/mpulse_kpi_report/mpluse_summary.sh {0} '.format(date_param) ,
     dag=dag,
     run_as_user = 'daasuser',retry_delay=timedelta(minutes=5),retries=2)

initiate_datasets >> [mpulse_recharges,mpulse_Usim,mpulse_ayoba,mpulse_myMTNAPP,mpulse_myMTNAPP_dwld,mpulse_NIN] >> summarize_mpulse_rpt

