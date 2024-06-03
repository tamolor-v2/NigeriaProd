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
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')




dag = DAG(
    dag_id='CIO_TRACKER',
    default_args=args,
    schedule_interval='30 4 * * *',
    description='Summaries on CIO Tracker',
    catchup=False,
    concurrency=10,
    max_active_runs=10
)

start_dag = BashOperator(
    task_id='start_dag',
    bash_command='echo start_dag ',
    dag=dag,
    run_as_user = 'daasuser')


EXBYTE_EXTIME_COMM = BashOperator(
     task_id='EXBYTE_EXTIME_COMM' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p EXBYTE_EXTIME_COMM',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

INSERT_EXBYTE_EXTIME_COMM = BashOperator(
     task_id='INSERT_EXBYTE_EXTIME_COMM' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p EXBYTE_EXTIME_COMM -l 1',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


CARDLOAD = BashOperator(
     task_id='cardload' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p cardload -l 3 -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser',
)

INSERT_CARDLOAD = BashOperator(
     task_id='INSERT_CARDLOAD' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p CARDLOAD -l 1',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


TOP1000_SPENDER = BashOperator(
     task_id='TOP1000_SPENDER' ,
     bash_command='bash /nas/share05/ops/scripts/top1000/run_top1000.sh {0}  {0}  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

RUN_REV = BashOperator(
     task_id='run_rev' ,
     bash_command='bash /nas/share05/scripts/revenue/run_rev.sh {0}  {0}  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


INSERT_TOP1000_SPENDER = BashOperator(
     task_id='INSERT_TOP1000_SPENDER' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p TOP1000_SPENDER -l 1',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)


INCOMING_TRAFFIC = BashOperator(
     task_id='INCOMING_TRAFFIC' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p INCOMING_TRAFFIC',
     run_as_user = 'daasuser',
     dag=dag,
)

INSERT_INCOMING_TRAFFIC = BashOperator(
     task_id='INSERT_INCOMING_TRAFFIC' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p INCOMING_TRAFFIC -l 1',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

REP_INCOMING_TRAFFIC_MON = BashOperator(
     task_id='REP_INCOMING_TRAFFIC_MON' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p REP_INCOMING_TRAFFIC_MON',
     run_as_user = 'daasuser',
     dag=dag,
)


DAILY_NUMBER_MGMT = BashOperator(
     task_id='DAILY_NUMBER_MGMT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p Daily_Number_Mgmt -l 3',
     dag=dag,
     run_as_user = 'daasuser',
)

INSERT_DAILY_NUMBER_MGMT = BashOperator(
     task_id='INSERT_DAILY_NUMBER_MGMT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p DAILY_NUMBER_MGMT -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)


MSC_RECON= BashOperator(
     task_id='msc_recon' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/daily_reports.py -p MSC_CCN_RECON -l 3 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

CS18_MSC_RECON= BashOperator(
     task_id='cs18_msc_recon' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p cs18_msc_ccn_recon -l 3 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

CS18_MSC_TREND= BashOperator(
     task_id='cs18_msc_trend' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cs18_ops_reports.py -p MSC_CCN_TREND -l 3 ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)



INSERT_MSC_CCN_TREND = BashOperator(
     task_id='INSERT_MSC_CCN_TREND' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p MSC_CCN_TREND -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)


IDR= BashOperator(
     task_id='idr' ,
     bash_command='perl /nas/share05/ops/monthly/international_dialing_revenue.pl  `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d` ',
  #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

INSERT_IDR = BashOperator(
     task_id='INSERT_IDR' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p IDR -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)


DAILY_RECHARGES_VTU = BashOperator(
     task_id='daily_recharges_vtu' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p DAILY_RECHARGES_VTU',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

CHECK_USAGE_SUMMARY = BashOperator(
     task_id='DAILY_RECHARGES_VTU' ,
     bash_command='bash /nas/share05/ops/mtnops/feed_check.sh check_usage_summary_vtu {0}	'.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

WAIT_CUSTOMERSUBJECT = BashOperator(
     task_id='WAIT_CUSTOMERSUBJECT' ,
     bash_command='bash /nas/share05/ops/mtnops/feed_check.sh wait_customersubject {0}       '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

NOMGMT = BashOperator(
     task_id='NOMGMT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p NOMGMT -l 2 {0}     '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

INSERT_DAILY_RECHARGES_VTU = BashOperator(
     task_id='INSERT_DAILY_RECHARGES_VTU' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p DAILY_RECHARGES_VTU -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)

D2C_REPORT = BashOperator(
     task_id='D2C_REPORT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p D2C_REPORT',
     run_as_user = 'daasuser',
     dag=dag,
)

INSERT_D2C_REPORT = BashOperator(
     task_id='INSERT_D2C_REPORT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p D2C_REPORT -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)

DOLA = BashOperator(
     task_id='dola' ,
     bash_command='perl /nas/share05/ops/monthly/dola_id0083.pl `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser',
)

INSERT_DOLA = BashOperator(
     task_id='INSERT_DOLA' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p DOLA -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)

SPOOL_OWC_ACTV_LOG = BashOperator(
     task_id='SPOOL_OWC_ACTV_LOG' ,
     bash_command='bash /nas/share05/dataOps_prod/spool_OWC_actv_log.sh {0}   '.format(date_param_d1),
     run_as_user = 'daasuser',
     dag=dag,
)


GSM_MASTER = BashOperator(
     task_id='GSM_MASTER' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/GSM_SERVICE_MASTER.py {0}   '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

SFX_CLIENT_ACTIVITY_LOG = BashOperator(
     task_id='SFX_CLIENT_ACTIVITY_LOG' ,
     bash_command='bash /nas/share05/dataOps_prod/spool_OWC_actv_log.sh  {0}   '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

MAN_MEDIATION_METRICS = BashOperator(
     task_id='MAN_MEDIATION_METRICS' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/man_Mediation_metrics.py {0}   '.format(date_param),
     run_as_user = 'daasuser',
     dag=dag,
)

SPOOL_OWC1 = BashOperator(
     task_id='SPOOL_OWC1' ,
     bash_command='bash /nas/share05/ops/mtnops/spool_OWC1.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)

SPOOL_OWC3 = BashOperator(
     task_id='SPOOL_OWC3' ,
     bash_command='bash /nas/share05/ops/mtnops/spool_OWC3.sh  ',
     run_as_user = 'daasuser',
     dag=dag,
)


DEVICE_PERFORMANCE = BashOperator(
     task_id='DEVICE_PERFORMANCE' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/device_perf.py -p ALL -s  {0}   '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser',
)

INSERT_DEVICE_PERFORMANCE = BashOperator(
     task_id='INSERT_DEVICE_PERFORMANCE' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/cio_tracker.py -p DEVICE_PERFORMANCE -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)

start_dag >> CHECK_USAGE_SUMMARY >> DAILY_RECHARGES_VTU >> INSERT_DAILY_RECHARGES_VTU
start_dag >> EXBYTE_EXTIME_COMM >> INSERT_EXBYTE_EXTIME_COMM
start_dag >> CARDLOAD >> INSERT_CARDLOAD
start_dag >> RUN_REV >> TOP1000_SPENDER >> INSERT_TOP1000_SPENDER
start_dag >> INCOMING_TRAFFIC >> REP_INCOMING_TRAFFIC_MON >> INSERT_INCOMING_TRAFFIC
start_dag >> WAIT_CUSTOMERSUBJECT >> NOMGMT >> DAILY_NUMBER_MGMT >> INSERT_DAILY_NUMBER_MGMT
start_dag >> WAIT_CUSTOMERSUBJECT >> DOLA >> INSERT_DOLA
start_dag >> MSC_RECON >> INSERT_MSC_CCN_TREND >> CS18_MSC_RECON >> CS18_MSC_TREND 
start_dag >> IDR >> INSERT_IDR
start_dag >> D2C_REPORT >> INSERT_D2C_REPORT
start_dag >> SPOOL_OWC_ACTV_LOG >> GSM_MASTER >> SFX_CLIENT_ACTIVITY_LOG >> MAN_MEDIATION_METRICS >> SPOOL_OWC1 >> DEVICE_PERFORMANCE >> INSERT_DEVICE_PERFORMANCE
start_dag >> SPOOL_OWC3
