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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com','ayodeji.shadare@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Live_UI_Dedup',
    default_args=args,
#    schedule_interval='30 6,8,10,12,14,16,18,20,22 * * *',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

NGVS_CDR = BashOperator(
     task_id='NGVS_CDR' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` NGVS_CDR',
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_AIR_REFILL_MA = BashOperator(
     task_id='CS5_AIR_REFILL_MA' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` CS5_AIR_REFILL_MA',
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_SDP_ACC_ADJ_MA = BashOperator(
     task_id='CS5_SDP_ACC_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` CS5_SDP_ACC_ADJ_MA',
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_GPRS_MA = BashOperator(
     task_id='CS5_CCN_GPRS_MA' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` CS5_CCN_GPRS_MA',
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_VOICE_MA = BashOperator(
     task_id='CS5_CCN_VOICE_MA' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` CS5_CCN_VOICE_MA',
     dag=dag,
     run_as_user = 'daasuser'
)

CS5_CCN_SMS_MA = BashOperator(
     task_id='CS5_CCN_SMS_MA' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` CS5_CCN_SMS_MA',
     dag=dag,
     run_as_user = 'daasuser'
)

BUNDLE4U_GPRS  = BashOperator(
     task_id='BUNDLE4U_GPRS' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` BUNDLE4U_GPRS',
     dag=dag,
     run_as_user = 'daasuser'
)

BUNDLE4U_VOICE = BashOperator(
     task_id='BUNDLE4U_VOICE' ,
     bash_command='bash /nas/share05/ops/daily/dedup.sh `date --date="-1 days" +%Y%m%d` `date --date="-0 days" +%Y%m%d` BUNDLE4U_VOICE',
     dag=dag,
     run_as_user = 'daasuser'
)


NGVS_CDR >> CS5_AIR_REFILL_MA >> CS5_SDP_ACC_ADJ_MA >> CS5_CCN_GPRS_MA >> CS5_CCN_VOICE_MA >> CS5_CCN_SMS_MA >> BUNDLE4U_GPRS >> BUNDLE4U_VOICE
