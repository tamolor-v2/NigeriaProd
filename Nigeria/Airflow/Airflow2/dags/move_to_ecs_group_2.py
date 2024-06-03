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
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='MOVE_TO_ECS_GROUP_2',
    default_args=args,
    schedule_interval='0 7 * * *',
    catchup=False,  
    concurrency=5,
    max_active_runs=5

)

START = BashOperator(
     task_id='start' ,
     bash_command='echo "start"	',
     run_as_user = 'daasuser',
     dag=dag,
)

SDP_DMP_DA =BashOperator(
     task_id='SDP_DMP_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SDP_DMP_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SDP_DMP_AC =BashOperator(
     task_id='SDP_DMP_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SDP_DMP_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)

SGSN_CDR =BashOperator(
     task_id='SGSN_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SGSN_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
MA_MONITOR =BashOperator(
     task_id='MA_MONITOR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MA_MONITOR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_GPRS_DA =BashOperator(
     task_id='CS5_CCN_GPRS_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_GPRS_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
OFFER_DUMP =BashOperator(
     task_id='OFFER_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh OFFER_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
DPI_CDR_NEW =BashOperator(
     task_id='DPI_CDR_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DPI_CDR_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_VOICE_DA =BashOperator(
     task_id='CS5_CCN_VOICE_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_VOICE_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_REFILL_AC =BashOperator(
     task_id='CS5_AIR_REFILL_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_AIR_REFILL_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC =BashOperator(
     task_id='SMSC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMSC	',
     run_as_user = 'daasuser',
     dag=dag,
)
UC_DUMP =BashOperator(
     task_id='UC_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh UC_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NEWREG_BIOUPDT_POOL	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS6_SDP_CDR =BashOperator(
     task_id='CS6_SDP_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS6_SDP_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SERV_MAST_VIEW_LIVE =BashOperator(
     task_id='CB_SERV_MAST_VIEW_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SERV_MAST_VIEW_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
UT_DUMP =BashOperator(
     task_id='UT_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh UT_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_MA =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_ACC_ADJ_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_ACCOUNT_MASTER =BashOperator(
     task_id='CB_ACCOUNT_MASTER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_ACCOUNT_MASTER	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_PAM_ALL =BashOperator(
     task_id='CS5_SDP_PAM_ALL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_PAM_ALL	',
     run_as_user = 'daasuser',
     dag=dag,
)
DMC_DUMP_ALL =BashOperator(
     task_id='DMC_DUMP_ALL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DMC_DUMP_ALL	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_SMS_AC =BashOperator(
     task_id='CS5_CCN_SMS_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_SMS_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)
UDC_DUMP =BashOperator(
     task_id='UDC_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh UDC_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS6_AIR_CDR =BashOperator(
     task_id='CS6_AIR_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS6_AIR_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
SDP_DMP_MA =BashOperator(
     task_id='SDP_DMP_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SDP_DMP_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
USSD_CDR =BashOperator(
     task_id='USSD_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh USSD_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_SMS_MA =BashOperator(
     task_id='CS5_CCN_SMS_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_SMS_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_CDR =BashOperator(
     task_id='HSDP_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HSDP_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
NGVS_DUMP =BashOperator(
     task_id='NGVS_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NGVS_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_AC =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_ACC_ADJ_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)
GSM_SIMS_MASTER_LIVE =BashOperator(
     task_id='GSM_SIMS_MASTER_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh GSM_SIMS_MASTER_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
IVR_SERVICE =BashOperator(
     task_id='IVR_SERVICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IVR_SERVICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_REFILL_MA =BashOperator(
     task_id='CS5_AIR_REFILL_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_AIR_REFILL_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_DA =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_ACC_ADJ_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_WEEKLY =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_WEEKLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NEWREG_BIOUPDT_POOL_WEEKLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
LOCATION =BashOperator(
     task_id='LOCATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh LOCATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
DSLCUSTOMERVERSIONED =BashOperator(
     task_id='DSLCUSTOMERVERSIONED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DSLCUSTOMERVERSIONED	',
     run_as_user = 'daasuser',
     dag=dag,
)
CIS_CDR =BashOperator(
     task_id='CIS_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CIS_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
NGVS_CDR =BashOperator(
     task_id='NGVS_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NGVS_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_DATA =BashOperator(
     task_id='CDR_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CDR_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMEREVENTSMSG =BashOperator(
     task_id='CUSTOMEREVENTSMSG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMEREVENTSMSG	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_ADJ_MA =BashOperator(
     task_id='CS5_AIR_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_AIR_ADJ_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_REFILL_DA =BashOperator(
     task_id='CS5_AIR_REFILL_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_AIR_REFILL_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_SMS_DA =BashOperator(
     task_id='CS5_CCN_SMS_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_CCN_SMS_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
FLYTXT_CAMPAIGN_EVENTS_DATA =BashOperator(
     task_id='FLYTXT_CAMPAIGN_EVENTS_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FLYTXT_CAMPAIGN_EVENTS_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
NGVS_DUMP_20190325 =BashOperator(
     task_id='NGVS_DUMP_20190325' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NGVS_DUMP_20190325	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SERV_MAST_VIEW_LIVE_INC =BashOperator(
     task_id='CB_SERV_MAST_VIEW_LIVE_INC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SERV_MAST_VIEW_LIVE_INC	',
     run_as_user = 'daasuser',
     dag=dag,
)
MVAS_DND_MSISDN_REPORT =BashOperator(
     task_id='MVAS_DND_MSISDN_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MVAS_DND_MSISDN_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_SUBSCRIPTION =BashOperator(
     task_id='CDR_SUBSCRIPTION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CDR_SUBSCRIPTION	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_LIVE =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NEWREG_BIOUPDT_POOL_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_RENEWAL_BASE =BashOperator(
     task_id='HSDP_RENEWAL_BASE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HSDP_RENEWAL_BASE	',
     run_as_user = 'daasuser',
     dag=dag,
)
RECON =BashOperator(
     task_id='RECON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RECON	',
     run_as_user = 'daasuser',
     dag=dag,
)
SUBSCRIBER_TRANSACTIONS_CDR =BashOperator(
     task_id='SUBSCRIBER_TRANSACTIONS_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SUBSCRIBER_TRANSACTIONS_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_SUMP_PRESTO =BashOperator(
     task_id='HSDP_SUMP_PRESTO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HSDP_SUMP_PRESTO	',
     run_as_user = 'daasuser',
     dag=dag,
)
ERS_VEND_NEW =BashOperator(
     task_id='ERS_VEND_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ERS_VEND_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
ERS_VEND =BashOperator(
     task_id='ERS_VEND' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ERS_VEND	',
     run_as_user = 'daasuser',
     dag=dag,
)
RECHARGE =BashOperator(
     task_id='RECHARGE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RECHARGE	',
     run_as_user = 'daasuser',
     dag=dag,
)
LBN =BashOperator(
     task_id='LBN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh LBN	',
     run_as_user = 'daasuser',
     dag=dag,
)
EDW_REPORT =BashOperator(
     task_id='EDW_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh EDW_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_MA_TMP =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_MA_TMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_ACC_ADJ_MA_TMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
DIGITAL_FOOTPRINT_NEW =BashOperator(
     task_id='DIGITAL_FOOTPRINT_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DIGITAL_FOOTPRINT_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_DOL_LOG =BashOperator(
     task_id='HSDP_DOL_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HSDP_DOL_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_REGISTRATIONS =BashOperator(
     task_id='MFS_REGISTRATIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_REGISTRATIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_ACQUISITION =BashOperator(
     task_id='MFS_ACQUISITION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_ACQUISITION	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_ADJ_DA =BashOperator(
     task_id='CS5_AIR_ADJ_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_AIR_ADJ_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMART_APP_CDR =BashOperator(
     task_id='SMART_APP_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMART_APP_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_CLIENT_DAARS_TAPIN =BashOperator(
     task_id='WBS_CLIENT_DAARS_TAPIN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh WBS_CLIENT_DAARS_TAPIN	',
     run_as_user = 'daasuser',
     dag=dag,
)
CLIENT_ACTIVITY_LOG =BashOperator(
     task_id='CLIENT_ACTIVITY_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CLIENT_ACTIVITY_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
SAG_API =BashOperator(
     task_id='SAG_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SAG_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_ACCOUNTS =BashOperator(
     task_id='MFS_ACCOUNTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_ACCOUNTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SERV_MAST_VIEW =BashOperator(
     task_id='CB_SERV_MAST_VIEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SERV_MAST_VIEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_VOICE =BashOperator(
     task_id='CDR_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CDR_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_CLIENT_DAARS_TAPOUT =BashOperator(
     task_id='WBS_CLIENT_DAARS_TAPOUT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh WBS_CLIENT_DAARS_TAPOUT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_VTU_DUMP =BashOperator(
     task_id='CS5_VTU_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_VTU_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_GPRS =BashOperator(
     task_id='TAPIN_GPRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPIN_GPRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MYMTNAPP =BashOperator(
     task_id='MYMTNAPP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MYMTNAPP	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIS_TOKEN =BashOperator(
     task_id='SIS_TOKEN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIS_TOKEN	',
     run_as_user = 'daasuser',
     dag=dag,
)
AVAYA_IVR =BashOperator(
     task_id='AVAYA_IVR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AVAYA_IVR	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST_LIVE =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMS_ACTIVATION_REQUEST_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTEVENTSMSGDAILY =BashOperator(
     task_id='CUSTEVENTSMSGDAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTEVENTSMSGDAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
TBL_IMEI_REGISTRATION_DTLS_VW =BashOperator(
     task_id='TBL_IMEI_REGISTRATION_DTLS_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TBL_IMEI_REGISTRATION_DTLS_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROFILE_WEEK =BashOperator(
     task_id='PROFILE_WEEK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PROFILE_WEEK	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROFIL_BDAIL =BashOperator(
     task_id='PROFIL_BDAIL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PROFIL_BDAIL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROFIL_ADAIL =BashOperator(
     task_id='PROFIL_ADAIL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PROFIL_ADAIL	',
     run_as_user = 'daasuser',
     dag=dag,
)
HUAMSC_DAAS_STG =BashOperator(
     task_id='HUAMSC_DAAS_STG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HUAMSC_DAAS_STG	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_MANUF_FILE_DETAIL =BashOperator(
     task_id='IFSAPP_MANUF_FILE_DETAIL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_MANUF_FILE_DETAIL	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_RECHARGE =BashOperator(
     task_id='CDR_RECHARGE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CDR_RECHARGE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_DAILY =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_DAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NEWREG_BIOUPDT_POOL_DAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
FLYTXT_INBOUND_EVENTS_DATA =BashOperator(
     task_id='FLYTXT_INBOUND_EVENTS_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FLYTXT_INBOUND_EVENTS_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
REVENUE =BashOperator(
     task_id='REVENUE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh REVENUE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CALL_REASON =BashOperator(
     task_id='CALL_REASON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CALL_REASON	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_ACTIVE_SUBSCRIBERS =BashOperator(
     task_id='MFS_ACTIVE_SUBSCRIBERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_ACTIVE_SUBSCRIBERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
QRIOS_TRANSACTIONS =BashOperator(
     task_id='QRIOS_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh QRIOS_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
NAS_LISTING =BashOperator(
     task_id='NAS_LISTING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NAS_LISTING	',
     run_as_user = 'daasuser',
     dag=dag,
)
USG_REALTIME =BashOperator(
     task_id='USG_REALTIME' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh USG_REALTIME	',
     run_as_user = 'daasuser',
     dag=dag,
)
USG_VOICE_IC =BashOperator(
     task_id='USG_VOICE_IC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh USG_VOICE_IC	',
     run_as_user = 'daasuser',
     dag=dag,
)
UPC_SUBSCRIPTION =BashOperator(
     task_id='UPC_SUBSCRIPTION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh UPC_SUBSCRIPTION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_D =BashOperator(
     task_id='MAPS_3G_CELL_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CELL_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
USG_DATA_SMS =BashOperator(
     task_id='USG_DATA_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh USG_DATA_SMS	',
     run_as_user = 'daasuser',
     dag=dag,
)
IVR_DATA =BashOperator(
     task_id='IVR_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IVR_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_VOICE =BashOperator(
     task_id='TAPIN_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPIN_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST_KPI =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST_KPI' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMS_ACTIVATION_REQUEST_KPI	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SUBS_POS_SERVICES =BashOperator(
     task_id='CB_SUBS_POS_SERVICES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SUBS_POS_SERVICES	',
     run_as_user = 'daasuser',
     dag=dag,
)
ABLT_GSM_STARTER_PACK =BashOperator(
     task_id='ABLT_GSM_STARTER_PACK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ABLT_GSM_STARTER_PACK	',
     run_as_user = 'daasuser',
     dag=dag,
)
FCT_FIN_INTERCONNECT =BashOperator(
     task_id='FCT_FIN_INTERCONNECT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FCT_FIN_INTERCONNECT	',
     run_as_user = 'daasuser',
     dag=dag,
)
QUARANTINED_MSISDN_BIB_LIVE =BashOperator(
     task_id='QUARANTINED_MSISDN_BIB_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh QUARANTINED_MSISDN_BIB_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
HISTORY_LOG_ATTRIBUTE =BashOperator(
     task_id='HISTORY_LOG_ATTRIBUTE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HISTORY_LOG_ATTRIBUTE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SRM_REQUEST =BashOperator(
     task_id='SRM_REQUEST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SRM_REQUEST	',
     run_as_user = 'daasuser',
     dag=dag,
)
AUDIT_LOGS =BashOperator(
     task_id='AUDIT_LOGS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AUDIT_LOGS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_D =BashOperator(
     task_id='MAPS_2G_CELL_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CELL_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_VW =BashOperator(
     task_id='SMS_ACTIVATION_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMS_ACTIVATION_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
AVAYA_CLID =BashOperator(
     task_id='AVAYA_CLID' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AVAYA_CLID	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_GPRS =BashOperator(
     task_id='TAPOUT_GPRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPOUT_GPRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST_JOIN =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST_JOIN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMS_ACTIVATION_REQUEST_JOIN	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_PROCESS_AVG_TIME =BashOperator(
     task_id='MSO_PROCESS_AVG_TIME' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSO_PROCESS_AVG_TIME	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_SMS =BashOperator(
     task_id='CDR_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CDR_SMS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_AGENT_COMMISSION =BashOperator(
     task_id='MFS_AGENT_COMMISSION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_AGENT_COMMISSION	',
     run_as_user = 'daasuser',
     dag=dag,
)
EVD_TRANSACTIONS =BashOperator(
     task_id='EVD_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh EVD_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS3G =BashOperator(
     task_id='MAPS3G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS3G	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUNDLE4U_VOICE =BashOperator(
     task_id='BUNDLE4U_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BUNDLE4U_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
DUMP_SHARESELL =BashOperator(
     task_id='DUMP_SHARESELL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DUMP_SHARESELL	',
     run_as_user = 'daasuser',
     dag=dag,
)
MOBILE_MONEY =BashOperator(
     task_id='MOBILE_MONEY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MOBILE_MONEY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_LCY_CLR_DA =BashOperator(
     task_id='CS5_SDP_LCY_CLR_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_LCY_CLR_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SERV_STATUS_SEAMFIX_REF =BashOperator(
     task_id='SERV_STATUS_SEAMFIX_REF' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SERV_STATUS_SEAMFIX_REF	',
     run_as_user = 'daasuser',
     dag=dag,
)
BSL_LISTING_FILES =BashOperator(
     task_id='BSL_LISTING_FILES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BSL_LISTING_FILES	',
     run_as_user = 'daasuser',
     dag=dag,
)
CLAWBACK_RPT =BashOperator(
     task_id='CLAWBACK_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CLAWBACK_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SCHEDULES =BashOperator(
     task_id='CB_SCHEDULES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SCHEDULES	',
     run_as_user = 'daasuser',
     dag=dag,
)
AGENT_USER_BIB_LIVE =BashOperator(
     task_id='AGENT_USER_BIB_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AGENT_USER_BIB_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MULTIPLE_REG_BIB =BashOperator(
     task_id='MULTIPLE_REG_BIB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MULTIPLE_REG_BIB	',
     run_as_user = 'daasuser',
     dag=dag,
)
BILL_SUMMARY_REP_MON =BashOperator(
     task_id='BILL_SUMMARY_REP_MON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BILL_SUMMARY_REP_MON	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_BIB_REPORT =BashOperator(
     task_id='WBS_BIB_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh WBS_BIB_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS2G =BashOperator(
     task_id='MAPS2G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS2G	',
     run_as_user = 'daasuser',
     dag=dag,
)
BALANCES =BashOperator(
     task_id='BALANCES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BALANCES	',
     run_as_user = 'daasuser',
     dag=dag,
)
D_DIRECT_EVENT_YYYYMM_LIVE =BashOperator(
     task_id='D_DIRECT_EVENT_YYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh D_DIRECT_EVENT_YYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
INV_SPECIFIC_PAYMENT_VW =BashOperator(
     task_id='INV_SPECIFIC_PAYMENT_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INV_SPECIFIC_PAYMENT_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
XAAS_DAILY =BashOperator(
     task_id='XAAS_DAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh XAAS_DAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_INVENTORY_TRANSACTION_HIS =BashOperator(
     task_id='IFSAPP_INVENTORY_TRANSACTION_HIS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_INVENTORY_TRANSACTION_HIS	',
     run_as_user = 'daasuser',
     dag=dag,
)
REFILL_EVENT =BashOperator(
     task_id='REFILL_EVENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh REFILL_EVENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
ALL_STMT_SERVICE_OPEN_CRED_VW =BashOperator(
     task_id='ALL_STMT_SERVICE_OPEN_CRED_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ALL_STMT_SERVICE_OPEN_CRED_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
AGENT_USER_BIB =BashOperator(
     task_id='AGENT_USER_BIB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AGENT_USER_BIB	',
     run_as_user = 'daasuser',
     dag=dag,
)
EXCESS_PAYMENT_RPT =BashOperator(
     task_id='EXCESS_PAYMENT_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh EXCESS_PAYMENT_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
APLIMAN_OBD =BashOperator(
     task_id='APLIMAN_OBD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh APLIMAN_OBD	',
     run_as_user = 'daasuser',
     dag=dag,
)
TRANSACTING_AGENT =BashOperator(
     task_id='TRANSACTING_AGENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TRANSACTING_AGENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CALL_REASON_MONTHLY =BashOperator(
     task_id='CALL_REASON_MONTHLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CALL_REASON_MONTHLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_POS_TRANSACTIONS =BashOperator(
     task_id='CB_POS_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_POS_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DEVICE_MAPPER_LIVE =BashOperator(
     task_id='DEVICE_MAPPER_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DEVICE_MAPPER_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
FINANCIAL_LOG =BashOperator(
     task_id='FINANCIAL_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FINANCIAL_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
PACK_SUB_EVNT =BashOperator(
     task_id='PACK_SUB_EVNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PACK_SUB_EVNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_PRO_BUDGET_COMMITMENTS =BashOperator(
     task_id='IFSAPP_PRO_BUDGET_COMMITMENTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_PRO_BUDGET_COMMITMENTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_VOICE =BashOperator(
     task_id='TAPOUT_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPOUT_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_TOTAL_DELIVERY_SUCCESS_RATE =BashOperator(
     task_id='SMSC_TOTAL_DELIVERY_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMSC_TOTAL_DELIVERY_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
OTP_STATUS =BashOperator(
     task_id='OTP_STATUS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh OTP_STATUS	',
     run_as_user = 'daasuser',
     dag=dag,
)
EVD_STOCK_LEVEL =BashOperator(
     task_id='EVD_STOCK_LEVEL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh EVD_STOCK_LEVEL	',
     run_as_user = 'daasuser',
     dag=dag,
)
ALL_STMT_SERVICE_OPEN_DEBT_VW =BashOperator(
     task_id='ALL_STMT_SERVICE_OPEN_DEBT_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ALL_STMT_SERVICE_OPEN_DEBT_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUNDLE4U_GPRS =BashOperator(
     task_id='BUNDLE4U_GPRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BUNDLE4U_GPRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMS_ACTIVATION_REQUEST	',
     run_as_user = 'daasuser',
     dag=dag,
)
SESSION =BashOperator(
     task_id='SESSION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SESSION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTNBIB_PALLET_ALLSKYYYYMM_LIVE =BashOperator(
     task_id='MTNBIB_PALLET_ALLSKYYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTNBIB_PALLET_ALLSKYYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CREDIT_INFORMATION_MON =BashOperator(
     task_id='CREDIT_INFORMATION_MON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CREDIT_INFORMATION_MON	',
     run_as_user = 'daasuser',
     dag=dag,
)
FLYTXT_LATCH_DUMP =BashOperator(
     task_id='FLYTXT_LATCH_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FLYTXT_LATCH_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
LEDGER_ITEM_TAB =BashOperator(
     task_id='LEDGER_ITEM_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh LEDGER_ITEM_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
NAS_LISTING_DIRC =BashOperator(
     task_id='NAS_LISTING_DIRC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NAS_LISTING_DIRC	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUG_ACCESS_FEE_BASE_VW =BashOperator(
     task_id='CUG_ACCESS_FEE_BASE_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUG_ACCESS_FEE_BASE_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
FINANCIALLOG =BashOperator(
     task_id='FINANCIALLOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FINANCIALLOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
HISTORY_LOG =BashOperator(
     task_id='HISTORY_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HISTORY_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MULTIPLE_REG_BIB_LIVE =BashOperator(
     task_id='MULTIPLE_REG_BIB_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MULTIPLE_REG_BIB_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
INPUTADAPTERSTATSMSG_TEXT =BashOperator(
     task_id='INPUTADAPTERSTATSMSG_TEXT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INPUTADAPTERSTATSMSG_TEXT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MNP =BashOperator(
     task_id='MNP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MNP	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMART_APP_NEW_USERS =BashOperator(
     task_id='SMART_APP_NEW_USERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMART_APP_NEW_USERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE =BashOperator(
     task_id='MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_CUSTOMER_ORDER_LINE =BashOperator(
     task_id='IFSAPP_CUSTOMER_ORDER_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_CUSTOMER_ORDER_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CGW_API =BashOperator(
     task_id='CGW_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CGW_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
ALL_STMT_ACCOUNT_OPEN_INV_VW =BashOperator(
     task_id='ALL_STMT_ACCOUNT_OPEN_INV_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ALL_STMT_ACCOUNT_OPEN_INV_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_ORDER_HISTORY =BashOperator(
     task_id='CUSTOMER_ORDER_HISTORY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_ORDER_HISTORY	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_INCENTIVE_IMEI_VW =BashOperator(
     task_id='SMS_INCENTIVE_IMEI_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMS_INCENTIVE_IMEI_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_TRANSACTIONS =BashOperator(
     task_id='SIM_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIM_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_PURCHASE_ORDER_LINE_TAB =BashOperator(
     task_id='IFSAPP_PURCHASE_ORDER_LINE_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_PURCHASE_ORDER_LINE_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_VOICE_FINAL =BashOperator(
     task_id='TAPIN_VOICE_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPIN_VOICE_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
GEN_LED_VOUCHER =BashOperator(
     task_id='GEN_LED_VOUCHER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh GEN_LED_VOUCHER	',
     run_as_user = 'daasuser',
     dag=dag,
)
IB_API =BashOperator(
     task_id='IB_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IB_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_STOCK_LEVEL =BashOperator(
     task_id='MFS_STOCK_LEVEL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_STOCK_LEVEL	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_WEEKLY =BashOperator(
     task_id='MAPS_2G_CELL_WEEKLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CELL_WEEKLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_PR_DETAILS =BashOperator(
     task_id='MTN_PR_DETAILS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTN_PR_DETAILS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_AUDIT_LOG =BashOperator(
     task_id='MFS_AUDIT_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MFS_AUDIT_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_D_BH =BashOperator(
     task_id='MAPS_3G_CELL_D_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CELL_D_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_TAB =BashOperator(
     task_id='PAYMENT_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
HPD_HELP_DESK =BashOperator(
     task_id='HPD_HELP_DESK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HPD_HELP_DESK	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUDGET_PERIOD_AMOUNT =BashOperator(
     task_id='BUDGET_PERIOD_AMOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BUDGET_PERIOD_AMOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
ACCOUNTING_BALANCE_TAB =BashOperator(
     task_id='ACCOUNTING_BALANCE_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ACCOUNTING_BALANCE_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
INVENTORY_PART_IN_STOCK =BashOperator(
     task_id='INVENTORY_PART_IN_STOCK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INVENTORY_PART_IN_STOCK	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_ORDER_LINE_TAB =BashOperator(
     task_id='CUSTOMER_ORDER_LINE_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_ORDER_LINE_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAN_SUPP_INVOICE_ITEM =BashOperator(
     task_id='MAN_SUPP_INVOICE_ITEM' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAN_SUPP_INVOICE_ITEM	',
     run_as_user = 'daasuser',
     dag=dag,
)
RBT_EVENT =BashOperator(
     task_id='RBT_EVENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RBT_EVENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_OVERVIEW_COMM_RPT =BashOperator(
     task_id='MTN_OVERVIEW_COMM_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTN_OVERVIEW_COMM_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_REPORT_ALL =BashOperator(
     task_id='PAYMENT_REPORT_ALL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_REPORT_ALL	',
     run_as_user = 'daasuser',
     dag=dag,
)
EDW_SIM_SWAP =BashOperator(
     task_id='EDW_SIM_SWAP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh EDW_SIM_SWAP	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_GPRS_FINAL =BashOperator(
     task_id='TAPIN_GPRS_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPIN_GPRS_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_REQ_LINE =BashOperator(
     task_id='PURCHASE_REQ_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCHASE_REQ_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_ORDER_TAB =BashOperator(
     task_id='CUSTOMER_ORDER_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_ORDER_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER_LINE =BashOperator(
     task_id='PURCHASE_ORDER_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCHASE_ORDER_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_M =BashOperator(
     task_id='MAPS_3G_CELL_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CELL_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
KM_USER =BashOperator(
     task_id='KM_USER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh KM_USER	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_MNP_NUMBER_SUMMARY =BashOperator(
     task_id='MSO_MNP_NUMBER_SUMMARY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSO_MNP_NUMBER_SUMMARY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAN_SUPP_INVOICE =BashOperator(
     task_id='MAN_SUPP_INVOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAN_SUPP_INVOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_INV_CELL =BashOperator(
     task_id='MAPS_INV_CELL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_INV_CELL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_RECEIPT_TAB =BashOperator(
     task_id='PURCHASE_RECEIPT_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCHASE_RECEIPT_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS4G =BashOperator(
     task_id='MAPS4G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS4G	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAX_ITEM_QRY =BashOperator(
     task_id='TAX_ITEM_QRY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAX_ITEM_QRY	',
     run_as_user = 'daasuser',
     dag=dag,
)
BLACKLIST_HISTORY =BashOperator(
     task_id='BLACKLIST_HISTORY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BLACKLIST_HISTORY	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_MTN_RECEIPT_RECONC_RPT =BashOperator(
     task_id='IFSAPP_MTN_RECEIPT_RECONC_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_MTN_RECEIPT_RECONC_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUDGET_YEAR_AMOUNT =BashOperator(
     task_id='BUDGET_YEAR_AMOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BUDGET_YEAR_AMOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMART_APP_DOWNLOAD =BashOperator(
     task_id='SMART_APP_DOWNLOAD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMART_APP_DOWNLOAD	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_DAILY_TMP =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_DAILY_TMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NEWREG_BIOUPDT_POOL_DAILY_TMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_M_BH =BashOperator(
     task_id='MAPS_3G_CELL_M_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CELL_M_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
BSL_LISTING =BashOperator(
     task_id='BSL_LISTING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BSL_LISTING	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SCHEDULES_LIVE =BashOperator(
     task_id='CB_SCHEDULES_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SCHEDULES_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
DFS_LISTING =BashOperator(
     task_id='DFS_LISTING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DFS_LISTING	',
     run_as_user = 'daasuser',
     dag=dag,
)
ECW_TRANSACTION =BashOperator(
     task_id='ECW_TRANSACTION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ECW_TRANSACTION	',
     run_as_user = 'daasuser',
     dag=dag,
)
KM_USER_ROLE =BashOperator(
     task_id='KM_USER_ROLE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh KM_USER_ROLE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NODE =BashOperator(
     task_id='NODE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NODE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NODE_ASSIGNMENT =BashOperator(
     task_id='NODE_ASSIGNMENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NODE_ASSIGNMENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER_HIST =BashOperator(
     task_id='PURCHASE_ORDER_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCHASE_ORDER_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCH_REQ_APPROVAL =BashOperator(
     task_id='PURCH_REQ_APPROVAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCH_REQ_APPROVAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER_APPROVAL =BashOperator(
     task_id='PURCHASE_ORDER_APPROVAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCHASE_ORDER_APPROVAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
DBA_TAB_PRIVS =BashOperator(
     task_id='DBA_TAB_PRIVS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DBA_TAB_PRIVS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DAILY_BVN_LINKING =BashOperator(
     task_id='DAILY_BVN_LINKING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DAILY_BVN_LINKING	',
     run_as_user = 'daasuser',
     dag=dag,
)
TBL_DATA_AGENT_REGISTRATION_VW =BashOperator(
     task_id='TBL_DATA_AGENT_REGISTRATION_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TBL_DATA_AGENT_REGISTRATION_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
PREPAID_PAYMENTS_LOG =BashOperator(
     task_id='PREPAID_PAYMENTS_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PREPAID_PAYMENTS_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_SITE_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_SITE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_SITE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUG_ACCESS_FEES =BashOperator(
     task_id='CUG_ACCESS_FEES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUG_ACCESS_FEES	',
     run_as_user = 'daasuser',
     dag=dag,
)
TBL_DATA_AGENT_REGISTRATION_VW_LIVE =BashOperator(
     task_id='TBL_DATA_AGENT_REGISTRATION_VW_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TBL_DATA_AGENT_REGISTRATION_VW_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
TOKEN_REPORT =BashOperator(
     task_id='TOKEN_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TOKEN_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_EXTRATIME_METRIC =BashOperator(
     task_id='ESM_EXTRATIME_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_EXTRATIME_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SHARENSELL_METRICS =BashOperator(
     task_id='ESM_SHARENSELL_METRICS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_SHARENSELL_METRICS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SPONSORED_DATA_MA =BashOperator(
     task_id='SPONSORED_DATA_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SPONSORED_DATA_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_VVE_CLR_DA =BashOperator(
     task_id='CS5_SDP_VVE_CLR_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CS5_SDP_VVE_CLR_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_BOARD_D =BashOperator(
     task_id='MAPS_4G_BOARD_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_BOARD_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SUB_METRIC =BashOperator(
     task_id='ESM_SUB_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_SUB_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
SME_REPORTS =BashOperator(
     task_id='SME_REPORTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SME_REPORTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
HOURLY_RECHARGES_CHANNEL =BashOperator(
     task_id='HOURLY_RECHARGES_CHANNEL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HOURLY_RECHARGES_CHANNEL	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SIM_REG_METRIC =BashOperator(
     task_id='ESM_SIM_REG_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_SIM_REG_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
PORT_IN_OUT =BashOperator(
     task_id='PORT_IN_OUT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PORT_IN_OUT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SPONSORED_DATA_DA =BashOperator(
     task_id='SPONSORED_DATA_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SPONSORED_DATA_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HLR_D =BashOperator(
     task_id='MAPS_CORE_HLR_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_HLR_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_D_BH =BashOperator(
     task_id='MAPS_2G_CN_D_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CN_D_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_PMT_METRIC =BashOperator(
     task_id='ESM_PMT_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_PMT_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE =BashOperator(
     task_id='DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_CN_D =BashOperator(
     task_id='MAPS_CORE_CN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_CN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_D_BH =BashOperator(
     task_id='MAPS_3G_CN_D_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CN_D_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_REFILL_METRIC =BashOperator(
     task_id='ESM_REFILL_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_REFILL_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE =BashOperator(
     task_id='SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_UNSUB_METRIC =BashOperator(
     task_id='ESM_UNSUB_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_UNSUB_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_D =BashOperator(
     task_id='MAPS_3G_CN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFS_SALES_HIST =BashOperator(
     task_id='IFS_SALES_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFS_SALES_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
PRE_BSL_COUNTS =BashOperator(
     task_id='PRE_BSL_COUNTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PRE_BSL_COUNTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_DSR_ADHERENCE =BashOperator(
     task_id='TAS_DSR_ADHERENCE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAS_DSR_ADHERENCE	',
     run_as_user = 'daasuser',
     dag=dag,
)
FND_USER_ROLE =BashOperator(
     task_id='FND_USER_ROLE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FND_USER_ROLE	',
     run_as_user = 'daasuser',
     dag=dag,
)

END = BashOperator(
     task_id='end' ,
     bash_command='echo "end"	',
     run_as_user = 'daasuser',
     dag=dag,
)




START >> [SDP_DMP_DA, SDP_DMP_AC, SGSN_CDR, MA_MONITOR, CS5_CCN_GPRS_DA, OFFER_DUMP, DPI_CDR_NEW, CS5_CCN_VOICE_DA, CS5_AIR_REFILL_AC, SMSC, UC_DUMP, NEWREG_BIOUPDT_POOL, CS6_SDP_CDR, CB_SERV_MAST_VIEW_LIVE, UT_DUMP, CS5_SDP_ACC_ADJ_MA, CB_ACCOUNT_MASTER, CS5_SDP_PAM_ALL, DMC_DUMP_ALL, CS5_CCN_SMS_AC, UDC_DUMP, CS6_AIR_CDR, SDP_DMP_MA, USSD_CDR, CS5_CCN_SMS_MA, HSDP_CDR, NGVS_DUMP, CS5_SDP_ACC_ADJ_AC, GSM_SIMS_MASTER_LIVE, IVR_SERVICE, CS5_AIR_REFILL_MA, CS5_SDP_ACC_ADJ_DA, NEWREG_BIOUPDT_POOL_WEEKLY, LOCATION, DSLCUSTOMERVERSIONED, CIS_CDR, NGVS_CDR, CDR_DATA, CUSTOMEREVENTSMSG, CS5_AIR_ADJ_MA, CS5_AIR_REFILL_DA, CS5_CCN_SMS_DA, FLYTXT_CAMPAIGN_EVENTS_DATA, NGVS_DUMP_20190325, CB_SERV_MAST_VIEW_LIVE_INC, MVAS_DND_MSISDN_REPORT, CDR_SUBSCRIPTION, NEWREG_BIOUPDT_POOL_LIVE, HSDP_RENEWAL_BASE, RECON, SUBSCRIBER_TRANSACTIONS_CDR, HSDP_SUMP_PRESTO, ERS_VEND_NEW, ERS_VEND, RECHARGE, LBN, EDW_REPORT, CS5_SDP_ACC_ADJ_MA_TMP, DIGITAL_FOOTPRINT_NEW, HSDP_DOL_LOG, MFS_REGISTRATIONS, MFS_ACQUISITION, CS5_AIR_ADJ_DA, SMART_APP_CDR, WBS_CLIENT_DAARS_TAPIN, CLIENT_ACTIVITY_LOG, SAG_API, MFS_ACCOUNTS, CB_SERV_MAST_VIEW, CDR_VOICE, WBS_CLIENT_DAARS_TAPOUT, CS5_VTU_DUMP, TAPIN_GPRS, MYMTNAPP, SIS_TOKEN, AVAYA_IVR, SMS_ACTIVATION_REQUEST_LIVE, CUSTEVENTSMSGDAILY, TBL_IMEI_REGISTRATION_DTLS_VW, PROFILE_WEEK, PROFIL_BDAIL, PROFIL_ADAIL, HUAMSC_DAAS_STG, IFSAPP_MANUF_FILE_DETAIL, CDR_RECHARGE, NEWREG_BIOUPDT_POOL_DAILY, FLYTXT_INBOUND_EVENTS_DATA, REVENUE, CALL_REASON, MFS_ACTIVE_SUBSCRIBERS, QRIOS_TRANSACTIONS, NAS_LISTING, USG_REALTIME, USG_VOICE_IC, UPC_SUBSCRIPTION, MAPS_3G_CELL_D, USG_DATA_SMS, IVR_DATA, TAPIN_VOICE, SMS_ACTIVATION_REQUEST_KPI, CB_SUBS_POS_SERVICES, ABLT_GSM_STARTER_PACK, FCT_FIN_INTERCONNECT, QUARANTINED_MSISDN_BIB_LIVE, HISTORY_LOG_ATTRIBUTE, SRM_REQUEST, AUDIT_LOGS, MAPS_2G_CELL_D, SMS_ACTIVATION_VW, AVAYA_CLID, TAPOUT_GPRS, SMS_ACTIVATION_REQUEST_JOIN, MSO_PROCESS_AVG_TIME, CDR_SMS, MFS_AGENT_COMMISSION, EVD_TRANSACTIONS, MAPS3G, BUNDLE4U_VOICE, DUMP_SHARESELL, MOBILE_MONEY, CS5_SDP_LCY_CLR_DA, SERV_STATUS_SEAMFIX_REF, BSL_LISTING_FILES, CLAWBACK_RPT, CB_SCHEDULES, AGENT_USER_BIB_LIVE, MULTIPLE_REG_BIB, BILL_SUMMARY_REP_MON, WBS_BIB_REPORT, MAPS2G, BALANCES, D_DIRECT_EVENT_YYYYMM_LIVE, INV_SPECIFIC_PAYMENT_VW, XAAS_DAILY, IFSAPP_INVENTORY_TRANSACTION_HIS, REFILL_EVENT, ALL_STMT_SERVICE_OPEN_CRED_VW, AGENT_USER_BIB, EXCESS_PAYMENT_RPT, APLIMAN_OBD, TRANSACTING_AGENT, CALL_REASON_MONTHLY, CB_POS_TRANSACTIONS, DEVICE_MAPPER_LIVE, FINANCIAL_LOG, PACK_SUB_EVNT, IFSAPP_PRO_BUDGET_COMMITMENTS, TAPOUT_VOICE, SMSC_TOTAL_DELIVERY_SUCCESS_RATE, OTP_STATUS, EVD_STOCK_LEVEL, ALL_STMT_SERVICE_OPEN_DEBT_VW, BUNDLE4U_GPRS, SMS_ACTIVATION_REQUEST, SESSION, MTNBIB_PALLET_ALLSKYYYYMM_LIVE, CREDIT_INFORMATION_MON, FLYTXT_LATCH_DUMP, LEDGER_ITEM_TAB, NAS_LISTING_DIRC, CUG_ACCESS_FEE_BASE_VW, FINANCIALLOG, HISTORY_LOG, MULTIPLE_REG_BIB_LIVE, INPUTADAPTERSTATSMSG_TEXT, MNP, SMART_APP_NEW_USERS, MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE, IFSAPP_CUSTOMER_ORDER_LINE, CGW_API, ALL_STMT_ACCOUNT_OPEN_INV_VW, CUSTOMER_ORDER_HISTORY, SMS_INCENTIVE_IMEI_VW, SIM_TRANSACTIONS, IFSAPP_PURCHASE_ORDER_LINE_TAB, TAPIN_VOICE_FINAL, GEN_LED_VOUCHER, IB_API, MFS_STOCK_LEVEL, MAPS_2G_CELL_WEEKLY, MTN_PR_DETAILS, MFS_AUDIT_LOG, MAPS_3G_CELL_D_BH, PAYMENT_TAB, HPD_HELP_DESK, BUDGET_PERIOD_AMOUNT, ACCOUNTING_BALANCE_TAB, INVENTORY_PART_IN_STOCK, CUSTOMER_ORDER_LINE_TAB, MAN_SUPP_INVOICE_ITEM, RBT_EVENT, MTN_OVERVIEW_COMM_RPT, PAYMENT_REPORT_ALL, EDW_SIM_SWAP, TAPIN_GPRS_FINAL, PURCHASE_REQ_LINE, CUSTOMER_ORDER_TAB, PURCHASE_ORDER_LINE, MAPS_3G_CELL_M, KM_USER, MSO_MNP_NUMBER_SUMMARY, MAN_SUPP_INVOICE, MAPS_INV_CELL, PURCHASE_RECEIPT_TAB, MAPS4G, TAX_ITEM_QRY, BLACKLIST_HISTORY, IFSAPP_MTN_RECEIPT_RECONC_RPT, BUDGET_YEAR_AMOUNT, SMART_APP_DOWNLOAD, NEWREG_BIOUPDT_POOL_DAILY_TMP, MAPS_3G_CELL_M_BH, BSL_LISTING, CB_SCHEDULES_LIVE, DFS_LISTING, ECW_TRANSACTION, KM_USER_ROLE, NODE, NODE_ASSIGNMENT, PURCHASE_ORDER_HIST, PURCH_REQ_APPROVAL, PURCHASE_ORDER_APPROVAL, DBA_TAB_PRIVS, DAILY_BVN_LINKING, TBL_DATA_AGENT_REGISTRATION_VW, PREPAID_PAYMENTS_LOG, MKT_DAILY_SITE_AH_REPORT, CUG_ACCESS_FEES, TBL_DATA_AGENT_REGISTRATION_VW_LIVE, TOKEN_REPORT, ESM_EXTRATIME_METRIC, ESM_SHARENSELL_METRICS, SPONSORED_DATA_MA, CS5_SDP_VVE_CLR_DA, MAPS_4G_BOARD_D, ESM_SUB_METRIC, SME_REPORTS, HOURLY_RECHARGES_CHANNEL, ESM_SIM_REG_METRIC, PORT_IN_OUT, SPONSORED_DATA_DA, MAPS_CORE_HLR_D, MAPS_2G_CN_D_BH, ESM_PMT_METRIC, DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE, MAPS_CORE_CN_D, MAPS_3G_CN_D_BH, ESM_REFILL_METRIC, SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE, ESM_UNSUB_METRIC, MAPS_3G_CN_D, IFS_SALES_HIST, PRE_BSL_COUNTS, TAS_DSR_ADHERENCE, FND_USER_ROLE] >> END

#CCN_CDR_Detail, LEA_MAPPING_MSC_DAAS 

