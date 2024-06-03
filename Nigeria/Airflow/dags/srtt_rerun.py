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
    'email_on_failure': ['t.adigun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='SRTT_RERUN',
    default_args=args,
    schedule_interval='0 5 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1

)

GDS_USAGE_REPORT = BashOperator(
     task_id='gds_usage_report' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/gds_usage_report.py `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`',
     #depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

Sponsored_Data = BashOperator(
     task_id='Sponsored_Data' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p SPONSORED ',
     dag=dag,
     run_as_user = 'daasuser'
)

USSD_Summary = BashOperator(
     task_id='USSD_Summary' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p ussd -s `date --date="-1 days" +%Y-%m-%d` -l 1',
     dag=dag,
     run_as_user = 'daasuser'
)

MSISDN_SC_MA_DA_NEW = BashOperator(
     task_id='msisdn_sc_ma_da_new' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/month_end_reports.py -p MSISDN_SC_MA_DA_NEW -s `date --date="-1 days" +%Y-%m-%d` -l 1',
     dag=dag,
     run_as_user = 'daasuser',
)

SIM4G_DEVICE_REPORT = BashOperator(
     task_id='SIM4G_DEVICE_REPORT' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/SIM4G_DEVICE_REPORT.py `date --date="-1 days" +%Y%m%d`  `date --date="-1 days" +%Y%m%d`    ',
     run_as_user = 'daasuser',
     dag=dag,
)

KEEP_MY_NUMBER = BashOperator(
     task_id='Keep_My_Number' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/kmn.py -p kmn -l 2 ',
     dag=dag,
     run_as_user = 'daasuser',
)

GDS_CONSUMER = BashOperator(
     task_id='GDS_CONSUMER' ,
     bash_command='bash /nas/share05/ops/mtnops/agl_gds_consumer_upload.sh `date --date="-1 days" +%Y%m%d`  ',
     run_as_user = 'daasuser',
     dag=dag,
)


ISP_LOGIN = BashOperator(
     task_id='ISP_LOGIN' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/data_ingestion.py -p isp_login ',
     run_as_user = 'daasuser',
     dag=dag,
)

GDS_CDR = BashOperator(
     task_id='GDS_CDR' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/gds_usage_cdr.py  `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser',
)

DPI_CCN = BashOperator(
     task_id='DPI_CCN' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/dpi_vs_ccn.py  `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser',
)


#END_SMS1 = BashOperator(
#     task_id='end_sms1' ,
#     bash_command='bash /mnt/beegfs_bsl/ops/sms_alert/report_sms1.sh',
#     dag=dag,
#     run_as_user = 'daasuser',
#)


USSD_Summary >> GDS_USAGE_REPORT >> MSISDN_SC_MA_DA_NEW >> SIM4G_DEVICE_REPORT >> GDS_CDR >> KEEP_MY_NUMBER >> GDS_CONSUMER >> ISP_LOGIN >> DPI_CCN >> Sponsored_Data #>> END_SMS1
