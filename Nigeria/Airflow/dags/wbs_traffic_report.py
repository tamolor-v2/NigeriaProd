from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


import airflow
from airflow.models import DAG
from airflow.operators import BashOperator,PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_param2 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

 
 
dag = DAG(
    dag_id='WBS_TRAFFIC_REPORT',
    default_args=args,
    schedule_interval='0 5 * * *',
    description='WBS TRAFFIC REPORT FOR FINANCE',
    catchup=False,
    concurrency=2,
    max_active_runs=2
)


EXTRACT_FROM_WBS = BashOperator(
     task_id='extract_from_wbs' ,
     bash_command='ssh 10.1.197.142 /nas/share05/ops/daily/WBS_BIB_REPORT_DAAS/scripts/wbs_pm_report_daas.sh {0} '.format(date_param2) ,
     dag=dag,
     run_as_user = 'daasuser'

)

INSERT_INTO_PM_REPORT = BashOperator(
     task_id='insert_into_pm_report' ,
     bash_command='/nas/share05/ops/daily/wbs_pm_report_daas_insert.py -p PM_REPORT -s {0} -l 1'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'


)

WBS_INBOUND_INCOMING = BashOperator(
     task_id='wbs_inbound_incoming' ,
     bash_command='/nas/share05/ops/daily/wbs_inbound_v7.py -p INCOMING -s {0} -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


WBS_INBOUND_OUTGOING = BashOperator(
     task_id='wbs_inbound_outgoing' ,
     bash_command='/nas/share05/ops/daily/wbs_inbound_v7.py -p OUTGOING -s {0} -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


EXTRACT_FROM_WBS >> INSERT_INTO_PM_REPORT >> [WBS_INBOUND_INCOMING,WBS_INBOUND_OUTGOING]

