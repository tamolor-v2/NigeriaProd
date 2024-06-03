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
    'email': ['oladimeji.olanipekun@mtn.com'],
    'email_on_failure': ['oladimeji.olanipekun@mtn.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
dag = DAG(
    dag_id='Month-End-REPORTS_4',
    default_args=args,
    schedule_interval='0 10 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

MTECH = BashOperator(
     task_id='MTECH' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MTECH -l 5',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

MTN_BIZPLUS = BashOperator(
     task_id='MTN_BIZPLUS' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MTN_BIZPLUS',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

MYMTNAPP = BashOperator(
     task_id='MYMTNAPP' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MYMTNAPP',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

ONEBOX = BashOperator(
     task_id='ONEBOX' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p ONEBOX',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

ONE_FI = BashOperator(
     task_id='ONE_FI' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p ONE_FI -l 5',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

PREPAID_ROAMING = BashOperator(
     task_id='PREPAID_ROAMING' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p PREPAID_ROAMING',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

REFILL_TREND = BashOperator(
     task_id='REFILL_TREND' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p REFILL_TREND -l 5',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

ROAMING_BUNDLE = BashOperator(
     task_id='ROAMING_BUNDLE' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p ROAMING_BUNDLE',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SC_MIGRATION = BashOperator(
     task_id='SC_MIGRATION' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SC_MIGRATION -l 5',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SDP_BAL_MA = BashOperator(
     task_id='SDP_BAL_MA' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SDP_BAL_MA -l 5',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SDP_BAL__MA = BashOperator(
     task_id='SDP_BAL__MA' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SDP_BAL__MA -l 5',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

AGENT_REP = BashOperator(
     task_id='AGENT_REP' ,
     bash_command='perl /nas/share05/ops/daily/agent_rep.pl  `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
     
)

PORT_IN_OUT = BashOperator(
     task_id='PORT_IN_OUT' ,
     bash_command='perl /nas/share05/ops/bin/port_in_out.pl `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
     
)

MTECH >> MTN_BIZPLUS >> MYMTNAPP >> ONEBOX >> ONE_FI >> PREPAID_ROAMING >> REFILL_TREND >> ROAMING_BUNDLE >> SC_MIGRATION >> SDP_BAL_MA >> SDP_BAL__MA >> AGENT_REP >> PORT_IN_OUT
