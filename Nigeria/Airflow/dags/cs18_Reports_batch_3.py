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
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='CS18_Daily_Reports_Batch_3',
    default_args=args,
    schedule_interval='* 18-21 * * *',
    catchup=False, 
    concurrency=1,
    max_active_runs=1
)

ADSL_OFFERS = BashOperator(
     task_id='ADSL_OFFERS' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p ADSL_OFFERS -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
ADSL_OFFERS

EXTRATIME_COMM = BashOperator(
     task_id='EXTRATIME_COMM' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p EXTRATIME_COMM -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
EXTRATIME_COMM


CUG_ACCESS_FEES = BashOperator(
     task_id='CUG_ACCESS_FEES' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p CUG_ACCESS_FEES -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
CUG_ACCESS_FEES

CUG_TRANS = BashOperator(
     task_id='CUG_TRANS' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p CUG_TRANS -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
CUG_TRANS

HYNET_DAAS = BashOperator(
     task_id='HYNET_DAAS' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p HYNET_DAAS -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
HYNET_DAAS

HYNET_REV = BashOperator(
     task_id='HYNET_REV' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p HYNET_REV -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
HYNET_REV

REV_PER_BTS = BashOperator(
     task_id='REV_PER_BTS' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p REV_PER_BTS -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
REV_PER_BTS

SHORT_CODES = BashOperator(
     task_id='SHORT_CODES' ,
     depends_on_past=False,
     bash_command='/nas/share05/dataOps_prod/platform_revenue/cs18_reports.py -p SHORT_CODES -s `date --date="-1 days" +%Y-%m-%d` -l 22',
     dag=dag,
     run_as_user = 'daasuser',
)
SHORT_CODES

ADSL_OFFERS >> CUG_ACCESS_FEES >> CUG_TRANS >> HYNET_DAAS >> HYNET_REV >> REV_PER_BTS >> SHORT_CODES >> EXTRATIME_COMM
