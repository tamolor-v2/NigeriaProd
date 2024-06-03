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

from collections import defaultdict
from os import popen
import os
import re

today =  datetime.today()
dateRun = datetime.today() - timedelta(days=7)
#dateRunStr = dateRun.strftime('%Y%m%d')

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': datetime(2020,3,20), #airflow.utils.dates.days_ago(1),
    'email': ['m.nabeel@ligadata.com'],
    'email_on_failure': ['m.nabeel@ligadata.com','support@ligadata.com','t.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'catchup':False,
}

dag = DAG(
    dag_id='CVM20_WeeklyAreas',
    default_args=args,
    schedule_interval= " 0 8 * * 1 ",
    catchup=False,
    concurrency=3,
    max_active_runs=1
)


def getDate(date_str):
    date = datetime.strptime(date_str[0:10], '%Y-%m-%d')
    currentdate=(date - timedelta(0)).strftime('%Y%m%d')
    return currentdate

getDateNode = PythonOperator(
    task_id = 'Get_Date',
    python_callable = getDate,
    priority_weight = 10,
    op_args=[popen('echo {{ execution_date }};').read()],
    dag=dag
)

dateRunStr="{{ task_instance.xcom_pull(task_ids='Get_Date') }}"

cvm20_subs_tmp = BashOperator(
     task_id='cvm20_subs_tmp' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_SUBS_TMP_base.sh {0} '.format(dateRunStr),
     dag=dag,
     priority_weight = 20,
     run_as_user = 'daasuser'
)

cvm20_refill_data_bundle = BashOperator(
     task_id='cvm20_refill_data_bundle' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REFILL_DATA_BUNDLE_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_reg_sbscr_flags = BashOperator(
     task_id='cvm20_reg_sbscr_flags' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REG_SBSCR_FLAGS_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_refill_and_subscription_bacth2 = BashOperator(
     task_id='cvm20_refill_and_subscription_bacth2' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REFILL_AND_SUBSCRIPTION_BACTH2_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_campaign_related_attributes_outbound = BashOperator(
     task_id='cvm20_campaign_related_attributes_outbound' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_CAMPAIGN_RELATED_ATTRIBUTES_OUTBOUND_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_campaign_related_attributes = BashOperator(
     task_id='cvm20_campaign_related_attributes' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_CAMPAIGN_RELATED_ATTRIBUTES_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_rev_voi_sms_data_tmp = BashOperator(
     task_id='cvm20_rev_voi_sms_data_tmp' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REV_VOI_SMS_DATA_TMP_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_cc = BashOperator(
     task_id='cvm20_cc' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_CC_base.sh {0} '.format(dateRunStr),
     dag=dag,
     priority_weight = 20,
     run_as_user = 'daasuser'
)

cvm20_rec_engine_dev2 = BashOperator(
     task_id='cvm20_rec_engine_dev2' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REC_ENGINE_DEV2_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_deviceinfo = BashOperator(
     task_id='cvm20_deviceinfo' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_DEVICEINFO_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_usg_sms = BashOperator(
     task_id='cvm20_usg_sms' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_USG_SMS_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_usg_voice = BashOperator(
     task_id='cvm20_usg_voice' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_USG_VOICE_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_customerinfo = BashOperator(
     task_id='cvm20_customerinfo' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_CUSTOMERINFO_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)

cvm20_usg_sms_base = BashOperator(
     task_id='cvm20_usg_sms_base' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_USG_SMS_BASE_base.sh {0} '.format(dateRunStr),
     dag=dag,
     priority_weight = 20,
     run_as_user = 'daasuser'
)

cvm20_rec_engine_dev_prod = BashOperator(
     task_id='cvm20_rec_engine_dev_prod' ,
     bash_command='bash /nas/share05/tools/CVM_Reports/Insert_CVM20_REC_ENGINE_DEV_PROD_base.sh {0} '.format(dateRunStr),
     dag=dag,
     run_as_user = 'daasuser'
)
getDateNode >> [cvm20_subs_tmp,cvm20_refill_data_bundle,cvm20_reg_sbscr_flags,cvm20_refill_and_subscription_bacth2,cvm20_campaign_related_attributes_outbound,cvm20_campaign_related_attributes,cvm20_rev_voi_sms_data_tmp,cvm20_cc,cvm20_rec_engine_dev2,cvm20_deviceinfo,cvm20_usg_sms,cvm20_usg_voice,cvm20_customerinfo]
cvm20_subs_tmp >> [cvm20_rec_engine_dev2,cvm20_deviceinfo] 
cvm20_subs_tmp >> [cvm20_usg_sms,cvm20_usg_voice,cvm20_customerinfo] 
cvm20_usg_sms_base >> [cvm20_usg_sms,cvm20_usg_voice,cvm20_customerinfo]
[cvm20_subs_tmp,cvm20_refill_data_bundle,cvm20_reg_sbscr_flags,cvm20_refill_and_subscription_bacth2,cvm20_campaign_related_attributes_outbound,cvm20_campaign_related_attributes,cvm20_rev_voi_sms_data_tmp,cvm20_cc,cvm20_rec_engine_dev2,cvm20_deviceinfo,cvm20_usg_sms,cvm20_usg_voice,cvm20_customerinfo] >> cvm20_rec_engine_dev_prod
