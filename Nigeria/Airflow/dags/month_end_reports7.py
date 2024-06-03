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
    'email': ['o.olanipekun@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Month-End-REPORTS_7',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1

) 

HYNET_FLEX = BashOperator(
     task_id='hynet_flex' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p HYNET_FLEX',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'

)

PAM_DIRECT = BashOperator(
     task_id='pam_direct' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p PAM_DIRECT',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'


)

MA_USAGE = BashOperator(
     task_id='ma_usage' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MA_USAGE',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'

)

MOD_ICB = BashOperator(
     task_id='mod_icb' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MOD_ICB',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'

)

VTU_DATABUNDLE = BashOperator(
     task_id='vtu_databundle' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p VTU_DATABUNDLE',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'

)

VOUCHER_SPLIT = BashOperator(
     task_id='voucher_split' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p VOUCHER_SPLIT',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'

)

GOODYBAG = BashOperator(
     task_id='goodybag' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p GOODY_BAG',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'


)

VAS_REPORT = BashOperator(
     task_id='vas_report' ,
     bash_command='perl /nas/share05/ops/DAAS_VAS_DETAILED.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
     
)

HYNET_FLEX >> PAM_DIRECT >> MA_USAGE >> MOD_ICB >> VTU_DATABUNDLE >> VOUCHER_SPLIT >> GOODYBAG >> VAS_REPORT
