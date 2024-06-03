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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}


dag = DAG(
    dag_id='MOVE_TO_ECS_BACKLOGS',
    default_args=args,
    schedule_interval=None,
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
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SDP_DMP_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SDP_DMP_AC =BashOperator(
     task_id='SDP_DMP_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SDP_DMP_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)

SGSN_CDR =BashOperator(
     task_id='SGSN_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SGSN_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
MA_MONITOR =BashOperator(
     task_id='MA_MONITOR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MA_MONITOR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_GPRS_DA =BashOperator(
     task_id='CS5_CCN_GPRS_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_GPRS_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
OFFER_DUMP =BashOperator(
     task_id='OFFER_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh OFFER_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
DPI_CDR_NEW =BashOperator(
     task_id='DPI_CDR_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DPI_CDR_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_VOICE_DA =BashOperator(
     task_id='CS5_CCN_VOICE_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_VOICE_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_REFILL_AC =BashOperator(
     task_id='CS5_AIR_REFILL_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_AIR_REFILL_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC =BashOperator(
     task_id='SMSC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMSC	',
     run_as_user = 'daasuser',
     dag=dag,
)
UC_DUMP =BashOperator(
     task_id='UC_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh UC_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NEWREG_BIOUPDT_POOL	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS6_SDP_CDR =BashOperator(
     task_id='CS6_SDP_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS6_SDP_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SERV_MAST_VIEW_LIVE =BashOperator(
     task_id='CB_SERV_MAST_VIEW_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SERV_MAST_VIEW_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
UT_DUMP =BashOperator(
     task_id='UT_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh UT_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_MA =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_ACC_ADJ_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_ACCOUNT_MASTER =BashOperator(
     task_id='CB_ACCOUNT_MASTER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_ACCOUNT_MASTER	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_PAM_ALL =BashOperator(
     task_id='CS5_SDP_PAM_ALL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_PAM_ALL	',
     run_as_user = 'daasuser',
     dag=dag,
)
DMC_DUMP_ALL =BashOperator(
     task_id='DMC_DUMP_ALL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DMC_DUMP_ALL	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_SMS_AC =BashOperator(
     task_id='CS5_CCN_SMS_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_SMS_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)
UDC_DUMP =BashOperator(
     task_id='UDC_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh UDC_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS6_AIR_CDR =BashOperator(
     task_id='CS6_AIR_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS6_AIR_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
SDP_DMP_MA =BashOperator(
     task_id='SDP_DMP_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SDP_DMP_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
USSD_CDR =BashOperator(
     task_id='USSD_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh USSD_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_SMS_MA =BashOperator(
     task_id='CS5_CCN_SMS_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_SMS_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_CDR =BashOperator(
     task_id='HSDP_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HSDP_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
NGVS_DUMP =BashOperator(
     task_id='NGVS_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NGVS_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_AC =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_AC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_ACC_ADJ_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)
GSM_SIMS_MASTER_LIVE =BashOperator(
     task_id='GSM_SIMS_MASTER_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh GSM_SIMS_MASTER_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
IVR_SERVICE =BashOperator(
     task_id='IVR_SERVICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IVR_SERVICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_REFILL_MA =BashOperator(
     task_id='CS5_AIR_REFILL_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_AIR_REFILL_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_DA =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_ACC_ADJ_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_WEEKLY =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_WEEKLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NEWREG_BIOUPDT_POOL_WEEKLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
LOCATION =BashOperator(
     task_id='LOCATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh LOCATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
DSLCUSTOMERVERSIONED =BashOperator(
     task_id='DSLCUSTOMERVERSIONED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DSLCUSTOMERVERSIONED	',
     run_as_user = 'daasuser',
     dag=dag,
)
CIS_CDR =BashOperator(
     task_id='CIS_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CIS_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
NGVS_CDR =BashOperator(
     task_id='NGVS_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NGVS_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_DATA =BashOperator(
     task_id='CDR_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CDR_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMEREVENTSMSG =BashOperator(
     task_id='CUSTOMEREVENTSMSG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMEREVENTSMSG	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_ADJ_MA =BashOperator(
     task_id='CS5_AIR_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_AIR_ADJ_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_REFILL_DA =BashOperator(
     task_id='CS5_AIR_REFILL_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_AIR_REFILL_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_CCN_SMS_DA =BashOperator(
     task_id='CS5_CCN_SMS_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_SMS_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
FLYTXT_CAMPAIGN_EVENTS_DATA =BashOperator(
     task_id='FLYTXT_CAMPAIGN_EVENTS_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FLYTXT_CAMPAIGN_EVENTS_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
NGVS_DUMP_20190325 =BashOperator(
     task_id='NGVS_DUMP_20190325' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NGVS_DUMP_20190325	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SERV_MAST_VIEW_LIVE_INC =BashOperator(
     task_id='CB_SERV_MAST_VIEW_LIVE_INC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SERV_MAST_VIEW_LIVE_INC	',
     run_as_user = 'daasuser',
     dag=dag,
)
MVAS_DND_MSISDN_REPORT =BashOperator(
     task_id='MVAS_DND_MSISDN_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MVAS_DND_MSISDN_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_SUBSCRIPTION =BashOperator(
     task_id='CDR_SUBSCRIPTION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CDR_SUBSCRIPTION	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_LIVE =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NEWREG_BIOUPDT_POOL_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_RENEWAL_BASE =BashOperator(
     task_id='HSDP_RENEWAL_BASE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HSDP_RENEWAL_BASE	',
     run_as_user = 'daasuser',
     dag=dag,
)
RECON =BashOperator(
     task_id='RECON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RECON	',
     run_as_user = 'daasuser',
     dag=dag,
)
SUBSCRIBER_TRANSACTIONS_CDR =BashOperator(
     task_id='SUBSCRIBER_TRANSACTIONS_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SUBSCRIBER_TRANSACTIONS_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_SUMP_PRESTO =BashOperator(
     task_id='HSDP_SUMP_PRESTO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HSDP_SUMP_PRESTO	',
     run_as_user = 'daasuser',
     dag=dag,
)
ERS_VEND_NEW =BashOperator(
     task_id='ERS_VEND_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ERS_VEND_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
ERS_VEND =BashOperator(
     task_id='ERS_VEND' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ERS_VEND	',
     run_as_user = 'daasuser',
     dag=dag,
)
RECHARGE =BashOperator(
     task_id='RECHARGE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RECHARGE	',
     run_as_user = 'daasuser',
     dag=dag,
)
LBN =BashOperator(
     task_id='LBN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh LBN	',
     run_as_user = 'daasuser',
     dag=dag,
)
EDW_REPORT =BashOperator(
     task_id='EDW_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh EDW_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_ACC_ADJ_MA_TMP =BashOperator(
     task_id='CS5_SDP_ACC_ADJ_MA_TMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_ACC_ADJ_MA_TMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
DIGITAL_FOOTPRINT_NEW =BashOperator(
     task_id='DIGITAL_FOOTPRINT_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DIGITAL_FOOTPRINT_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_DOL_LOG =BashOperator(
     task_id='HSDP_DOL_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HSDP_DOL_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_REGISTRATIONS =BashOperator(
     task_id='MFS_REGISTRATIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_REGISTRATIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_ACQUISITION =BashOperator(
     task_id='MFS_ACQUISITION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_ACQUISITION	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_AIR_ADJ_DA =BashOperator(
     task_id='CS5_AIR_ADJ_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_AIR_ADJ_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMART_APP_CDR =BashOperator(
     task_id='SMART_APP_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMART_APP_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_CLIENT_DAARS_TAPIN =BashOperator(
     task_id='WBS_CLIENT_DAARS_TAPIN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh WBS_CLIENT_DAARS_TAPIN	',
     run_as_user = 'daasuser',
     dag=dag,
)
CLIENT_ACTIVITY_LOG =BashOperator(
     task_id='CLIENT_ACTIVITY_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CLIENT_ACTIVITY_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
SAG_API =BashOperator(
     task_id='SAG_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SAG_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_ACCOUNTS =BashOperator(
     task_id='MFS_ACCOUNTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_ACCOUNTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SERV_MAST_VIEW =BashOperator(
     task_id='CB_SERV_MAST_VIEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SERV_MAST_VIEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_VOICE =BashOperator(
     task_id='CDR_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CDR_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_CLIENT_DAARS_TAPOUT =BashOperator(
     task_id='WBS_CLIENT_DAARS_TAPOUT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh WBS_CLIENT_DAARS_TAPOUT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_VTU_DUMP =BashOperator(
     task_id='CS5_VTU_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_VTU_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_GPRS =BashOperator(
     task_id='TAPIN_GPRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPIN_GPRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MYMTNAPP =BashOperator(
     task_id='MYMTNAPP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MYMTNAPP	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIS_TOKEN =BashOperator(
     task_id='SIS_TOKEN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIS_TOKEN	',
     run_as_user = 'daasuser',
     dag=dag,
)
AVAYA_IVR =BashOperator(
     task_id='AVAYA_IVR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AVAYA_IVR	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST_LIVE =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMS_ACTIVATION_REQUEST_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTEVENTSMSGDAILY =BashOperator(
     task_id='CUSTEVENTSMSGDAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTEVENTSMSGDAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
TBL_IMEI_REGISTRATION_DTLS_VW =BashOperator(
     task_id='TBL_IMEI_REGISTRATION_DTLS_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TBL_IMEI_REGISTRATION_DTLS_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROFILE_WEEK =BashOperator(
     task_id='PROFILE_WEEK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PROFILE_WEEK	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROFIL_BDAIL =BashOperator(
     task_id='PROFIL_BDAIL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PROFIL_BDAIL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROFIL_ADAIL =BashOperator(
     task_id='PROFIL_ADAIL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PROFIL_ADAIL	',
     run_as_user = 'daasuser',
     dag=dag,
)
HUAMSC_DAAS_STG =BashOperator(
     task_id='HUAMSC_DAAS_STG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HUAMSC_DAAS_STG	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_MANUF_FILE_DETAIL =BashOperator(
     task_id='IFSAPP_MANUF_FILE_DETAIL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_MANUF_FILE_DETAIL	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_RECHARGE =BashOperator(
     task_id='CDR_RECHARGE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CDR_RECHARGE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_DAILY =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_DAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NEWREG_BIOUPDT_POOL_DAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
FLYTXT_INBOUND_EVENTS_DATA =BashOperator(
     task_id='FLYTXT_INBOUND_EVENTS_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FLYTXT_INBOUND_EVENTS_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
REVENUE =BashOperator(
     task_id='REVENUE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh REVENUE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CALL_REASON =BashOperator(
     task_id='CALL_REASON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CALL_REASON	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_ACTIVE_SUBSCRIBERS =BashOperator(
     task_id='MFS_ACTIVE_SUBSCRIBERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_ACTIVE_SUBSCRIBERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
QRIOS_TRANSACTIONS =BashOperator(
     task_id='QRIOS_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh QRIOS_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
NAS_LISTING =BashOperator(
     task_id='NAS_LISTING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NAS_LISTING	',
     run_as_user = 'daasuser',
     dag=dag,
)
USG_REALTIME =BashOperator(
     task_id='USG_REALTIME' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh USG_REALTIME	',
     run_as_user = 'daasuser',
     dag=dag,
)
USG_VOICE_IC =BashOperator(
     task_id='USG_VOICE_IC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh USG_VOICE_IC	',
     run_as_user = 'daasuser',
     dag=dag,
)
UPC_SUBSCRIPTION =BashOperator(
     task_id='UPC_SUBSCRIPTION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh UPC_SUBSCRIPTION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_D =BashOperator(
     task_id='MAPS_3G_CELL_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CELL_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
USG_DATA_SMS =BashOperator(
     task_id='USG_DATA_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh USG_DATA_SMS	',
     run_as_user = 'daasuser',
     dag=dag,
)
IVR_DATA =BashOperator(
     task_id='IVR_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IVR_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_VOICE =BashOperator(
     task_id='TAPIN_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPIN_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST_KPI =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST_KPI' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMS_ACTIVATION_REQUEST_KPI	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SUBS_POS_SERVICES =BashOperator(
     task_id='CB_SUBS_POS_SERVICES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SUBS_POS_SERVICES	',
     run_as_user = 'daasuser',
     dag=dag,
)
ABLT_GSM_STARTER_PACK =BashOperator(
     task_id='ABLT_GSM_STARTER_PACK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ABLT_GSM_STARTER_PACK	',
     run_as_user = 'daasuser',
     dag=dag,
)
FCT_FIN_INTERCONNECT =BashOperator(
     task_id='FCT_FIN_INTERCONNECT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FCT_FIN_INTERCONNECT	',
     run_as_user = 'daasuser',
     dag=dag,
)
QUARANTINED_MSISDN_BIB_LIVE =BashOperator(
     task_id='QUARANTINED_MSISDN_BIB_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh QUARANTINED_MSISDN_BIB_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
HISTORY_LOG_ATTRIBUTE =BashOperator(
     task_id='HISTORY_LOG_ATTRIBUTE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HISTORY_LOG_ATTRIBUTE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SRM_REQUEST =BashOperator(
     task_id='SRM_REQUEST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SRM_REQUEST	',
     run_as_user = 'daasuser',
     dag=dag,
)
AUDIT_LOGS =BashOperator(
     task_id='AUDIT_LOGS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AUDIT_LOGS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_D =BashOperator(
     task_id='MAPS_2G_CELL_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CELL_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_VW =BashOperator(
     task_id='SMS_ACTIVATION_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMS_ACTIVATION_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
AVAYA_CLID =BashOperator(
     task_id='AVAYA_CLID' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AVAYA_CLID	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_GPRS =BashOperator(
     task_id='TAPOUT_GPRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPOUT_GPRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST_JOIN =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST_JOIN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMS_ACTIVATION_REQUEST_JOIN	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_PROCESS_AVG_TIME =BashOperator(
     task_id='MSO_PROCESS_AVG_TIME' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSO_PROCESS_AVG_TIME	',
     run_as_user = 'daasuser',
     dag=dag,
)
CDR_SMS =BashOperator(
     task_id='CDR_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CDR_SMS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_AGENT_COMMISSION =BashOperator(
     task_id='MFS_AGENT_COMMISSION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_AGENT_COMMISSION	',
     run_as_user = 'daasuser',
     dag=dag,
)
EVD_TRANSACTIONS =BashOperator(
     task_id='EVD_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh EVD_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS3G =BashOperator(
     task_id='MAPS3G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS3G	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUNDLE4U_VOICE =BashOperator(
     task_id='BUNDLE4U_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BUNDLE4U_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
DUMP_SHARESELL =BashOperator(
     task_id='DUMP_SHARESELL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DUMP_SHARESELL	',
     run_as_user = 'daasuser',
     dag=dag,
)
MOBILE_MONEY =BashOperator(
     task_id='MOBILE_MONEY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MOBILE_MONEY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_LCY_CLR_DA =BashOperator(
     task_id='CS5_SDP_LCY_CLR_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_LCY_CLR_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SERV_STATUS_SEAMFIX_REF =BashOperator(
     task_id='SERV_STATUS_SEAMFIX_REF' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SERV_STATUS_SEAMFIX_REF	',
     run_as_user = 'daasuser',
     dag=dag,
)
BSL_LISTING_FILES =BashOperator(
     task_id='BSL_LISTING_FILES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BSL_LISTING_FILES	',
     run_as_user = 'daasuser',
     dag=dag,
)
CLAWBACK_RPT =BashOperator(
     task_id='CLAWBACK_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CLAWBACK_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SCHEDULES =BashOperator(
     task_id='CB_SCHEDULES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SCHEDULES	',
     run_as_user = 'daasuser',
     dag=dag,
)
AGENT_USER_BIB_LIVE =BashOperator(
     task_id='AGENT_USER_BIB_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AGENT_USER_BIB_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MULTIPLE_REG_BIB =BashOperator(
     task_id='MULTIPLE_REG_BIB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MULTIPLE_REG_BIB	',
     run_as_user = 'daasuser',
     dag=dag,
)
BILL_SUMMARY_REP_MON =BashOperator(
     task_id='BILL_SUMMARY_REP_MON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BILL_SUMMARY_REP_MON	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_BIB_REPORT =BashOperator(
     task_id='WBS_BIB_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh WBS_BIB_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS2G =BashOperator(
     task_id='MAPS2G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS2G	',
     run_as_user = 'daasuser',
     dag=dag,
)
BALANCES =BashOperator(
     task_id='BALANCES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BALANCES	',
     run_as_user = 'daasuser',
     dag=dag,
)
D_DIRECT_EVENT_YYYYMM_LIVE =BashOperator(
     task_id='D_DIRECT_EVENT_YYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh D_DIRECT_EVENT_YYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
INV_SPECIFIC_PAYMENT_VW =BashOperator(
     task_id='INV_SPECIFIC_PAYMENT_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INV_SPECIFIC_PAYMENT_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
XAAS_DAILY =BashOperator(
     task_id='XAAS_DAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh XAAS_DAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_INVENTORY_TRANSACTION_HIS =BashOperator(
     task_id='IFSAPP_INVENTORY_TRANSACTION_HIS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_INVENTORY_TRANSACTION_HIS	',
     run_as_user = 'daasuser',
     dag=dag,
)
REFILL_EVENT =BashOperator(
     task_id='REFILL_EVENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh REFILL_EVENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
ALL_STMT_SERVICE_OPEN_CRED_VW =BashOperator(
     task_id='ALL_STMT_SERVICE_OPEN_CRED_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ALL_STMT_SERVICE_OPEN_CRED_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
AGENT_USER_BIB =BashOperator(
     task_id='AGENT_USER_BIB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AGENT_USER_BIB	',
     run_as_user = 'daasuser',
     dag=dag,
)
EXCESS_PAYMENT_RPT =BashOperator(
     task_id='EXCESS_PAYMENT_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh EXCESS_PAYMENT_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
APLIMAN_OBD =BashOperator(
     task_id='APLIMAN_OBD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh APLIMAN_OBD	',
     run_as_user = 'daasuser',
     dag=dag,
)
TRANSACTING_AGENT =BashOperator(
     task_id='TRANSACTING_AGENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TRANSACTING_AGENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CALL_REASON_MONTHLY =BashOperator(
     task_id='CALL_REASON_MONTHLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CALL_REASON_MONTHLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_POS_TRANSACTIONS =BashOperator(
     task_id='CB_POS_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_POS_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DEVICE_MAPPER_LIVE =BashOperator(
     task_id='DEVICE_MAPPER_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DEVICE_MAPPER_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
FINANCIAL_LOG =BashOperator(
     task_id='FINANCIAL_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FINANCIAL_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
PACK_SUB_EVNT =BashOperator(
     task_id='PACK_SUB_EVNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PACK_SUB_EVNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_PRO_BUDGET_COMMITMENTS =BashOperator(
     task_id='IFSAPP_PRO_BUDGET_COMMITMENTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_PRO_BUDGET_COMMITMENTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_VOICE =BashOperator(
     task_id='TAPOUT_VOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPOUT_VOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_TOTAL_DELIVERY_SUCCESS_RATE =BashOperator(
     task_id='SMSC_TOTAL_DELIVERY_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMSC_TOTAL_DELIVERY_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
OTP_STATUS =BashOperator(
     task_id='OTP_STATUS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh OTP_STATUS	',
     run_as_user = 'daasuser',
     dag=dag,
)
EVD_STOCK_LEVEL =BashOperator(
     task_id='EVD_STOCK_LEVEL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh EVD_STOCK_LEVEL	',
     run_as_user = 'daasuser',
     dag=dag,
)
ALL_STMT_SERVICE_OPEN_DEBT_VW =BashOperator(
     task_id='ALL_STMT_SERVICE_OPEN_DEBT_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ALL_STMT_SERVICE_OPEN_DEBT_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUNDLE4U_GPRS =BashOperator(
     task_id='BUNDLE4U_GPRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BUNDLE4U_GPRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_ACTIVATION_REQUEST =BashOperator(
     task_id='SMS_ACTIVATION_REQUEST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMS_ACTIVATION_REQUEST	',
     run_as_user = 'daasuser',
     dag=dag,
)
SESSION =BashOperator(
     task_id='SESSION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SESSION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTNBIB_PALLET_ALLSKYYYYMM_LIVE =BashOperator(
     task_id='MTNBIB_PALLET_ALLSKYYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTNBIB_PALLET_ALLSKYYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CREDIT_INFORMATION_MON =BashOperator(
     task_id='CREDIT_INFORMATION_MON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CREDIT_INFORMATION_MON	',
     run_as_user = 'daasuser',
     dag=dag,
)
FLYTXT_LATCH_DUMP =BashOperator(
     task_id='FLYTXT_LATCH_DUMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FLYTXT_LATCH_DUMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
LEDGER_ITEM_TAB =BashOperator(
     task_id='LEDGER_ITEM_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh LEDGER_ITEM_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
NAS_LISTING_DIRC =BashOperator(
     task_id='NAS_LISTING_DIRC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NAS_LISTING_DIRC	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUG_ACCESS_FEE_BASE_VW =BashOperator(
     task_id='CUG_ACCESS_FEE_BASE_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUG_ACCESS_FEE_BASE_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
FINANCIALLOG =BashOperator(
     task_id='FINANCIALLOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FINANCIALLOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
HISTORY_LOG =BashOperator(
     task_id='HISTORY_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HISTORY_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MULTIPLE_REG_BIB_LIVE =BashOperator(
     task_id='MULTIPLE_REG_BIB_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MULTIPLE_REG_BIB_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
INPUTADAPTERSTATSMSG_TEXT =BashOperator(
     task_id='INPUTADAPTERSTATSMSG_TEXT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INPUTADAPTERSTATSMSG_TEXT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MNP =BashOperator(
     task_id='MNP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MNP	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMART_APP_NEW_USERS =BashOperator(
     task_id='SMART_APP_NEW_USERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMART_APP_NEW_USERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE =BashOperator(
     task_id='MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_CUSTOMER_ORDER_LINE =BashOperator(
     task_id='IFSAPP_CUSTOMER_ORDER_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_CUSTOMER_ORDER_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CGW_API =BashOperator(
     task_id='CGW_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CGW_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
ALL_STMT_ACCOUNT_OPEN_INV_VW =BashOperator(
     task_id='ALL_STMT_ACCOUNT_OPEN_INV_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ALL_STMT_ACCOUNT_OPEN_INV_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_ORDER_HISTORY =BashOperator(
     task_id='CUSTOMER_ORDER_HISTORY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_ORDER_HISTORY	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMS_INCENTIVE_IMEI_VW =BashOperator(
     task_id='SMS_INCENTIVE_IMEI_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMS_INCENTIVE_IMEI_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_TRANSACTIONS =BashOperator(
     task_id='SIM_TRANSACTIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIM_TRANSACTIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_PURCHASE_ORDER_LINE_TAB =BashOperator(
     task_id='IFSAPP_PURCHASE_ORDER_LINE_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_PURCHASE_ORDER_LINE_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_VOICE_FINAL =BashOperator(
     task_id='TAPIN_VOICE_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPIN_VOICE_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
GEN_LED_VOUCHER =BashOperator(
     task_id='GEN_LED_VOUCHER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh GEN_LED_VOUCHER	',
     run_as_user = 'daasuser',
     dag=dag,
)
IB_API =BashOperator(
     task_id='IB_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IB_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_STOCK_LEVEL =BashOperator(
     task_id='MFS_STOCK_LEVEL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_STOCK_LEVEL	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_WEEKLY =BashOperator(
     task_id='MAPS_2G_CELL_WEEKLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CELL_WEEKLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_PR_DETAILS =BashOperator(
     task_id='MTN_PR_DETAILS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTN_PR_DETAILS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MFS_AUDIT_LOG =BashOperator(
     task_id='MFS_AUDIT_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MFS_AUDIT_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_D_BH =BashOperator(
     task_id='MAPS_3G_CELL_D_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CELL_D_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_TAB =BashOperator(
     task_id='PAYMENT_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
HPD_HELP_DESK =BashOperator(
     task_id='HPD_HELP_DESK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HPD_HELP_DESK	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUDGET_PERIOD_AMOUNT =BashOperator(
     task_id='BUDGET_PERIOD_AMOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BUDGET_PERIOD_AMOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
ACCOUNTING_BALANCE_TAB =BashOperator(
     task_id='ACCOUNTING_BALANCE_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ACCOUNTING_BALANCE_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
INVENTORY_PART_IN_STOCK =BashOperator(
     task_id='INVENTORY_PART_IN_STOCK' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INVENTORY_PART_IN_STOCK	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_ORDER_LINE_TAB =BashOperator(
     task_id='CUSTOMER_ORDER_LINE_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_ORDER_LINE_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAN_SUPP_INVOICE_ITEM =BashOperator(
     task_id='MAN_SUPP_INVOICE_ITEM' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAN_SUPP_INVOICE_ITEM	',
     run_as_user = 'daasuser',
     dag=dag,
)
RBT_EVENT =BashOperator(
     task_id='RBT_EVENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RBT_EVENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_OVERVIEW_COMM_RPT =BashOperator(
     task_id='MTN_OVERVIEW_COMM_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTN_OVERVIEW_COMM_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_REPORT_ALL =BashOperator(
     task_id='PAYMENT_REPORT_ALL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_REPORT_ALL	',
     run_as_user = 'daasuser',
     dag=dag,
)
EDW_SIM_SWAP =BashOperator(
     task_id='EDW_SIM_SWAP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh EDW_SIM_SWAP	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPIN_GPRS_FINAL =BashOperator(
     task_id='TAPIN_GPRS_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPIN_GPRS_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_REQ_LINE =BashOperator(
     task_id='PURCHASE_REQ_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCHASE_REQ_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_ORDER_TAB =BashOperator(
     task_id='CUSTOMER_ORDER_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_ORDER_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER_LINE =BashOperator(
     task_id='PURCHASE_ORDER_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCHASE_ORDER_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_M =BashOperator(
     task_id='MAPS_3G_CELL_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CELL_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
KM_USER =BashOperator(
     task_id='KM_USER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh KM_USER	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_MNP_NUMBER_SUMMARY =BashOperator(
     task_id='MSO_MNP_NUMBER_SUMMARY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSO_MNP_NUMBER_SUMMARY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAN_SUPP_INVOICE =BashOperator(
     task_id='MAN_SUPP_INVOICE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAN_SUPP_INVOICE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_INV_CELL =BashOperator(
     task_id='MAPS_INV_CELL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_INV_CELL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_RECEIPT_TAB =BashOperator(
     task_id='PURCHASE_RECEIPT_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCHASE_RECEIPT_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS4G =BashOperator(
     task_id='MAPS4G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS4G	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAX_ITEM_QRY =BashOperator(
     task_id='TAX_ITEM_QRY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAX_ITEM_QRY	',
     run_as_user = 'daasuser',
     dag=dag,
)
BLACKLIST_HISTORY =BashOperator(
     task_id='BLACKLIST_HISTORY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BLACKLIST_HISTORY	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_MTN_RECEIPT_RECONC_RPT =BashOperator(
     task_id='IFSAPP_MTN_RECEIPT_RECONC_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_MTN_RECEIPT_RECONC_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
BUDGET_YEAR_AMOUNT =BashOperator(
     task_id='BUDGET_YEAR_AMOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BUDGET_YEAR_AMOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMART_APP_DOWNLOAD =BashOperator(
     task_id='SMART_APP_DOWNLOAD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMART_APP_DOWNLOAD	',
     run_as_user = 'daasuser',
     dag=dag,
)
NEWREG_BIOUPDT_POOL_DAILY_TMP =BashOperator(
     task_id='NEWREG_BIOUPDT_POOL_DAILY_TMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NEWREG_BIOUPDT_POOL_DAILY_TMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CELL_M_BH =BashOperator(
     task_id='MAPS_3G_CELL_M_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CELL_M_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
BSL_LISTING =BashOperator(
     task_id='BSL_LISTING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BSL_LISTING	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SCHEDULES_LIVE =BashOperator(
     task_id='CB_SCHEDULES_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SCHEDULES_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
DFS_LISTING =BashOperator(
     task_id='DFS_LISTING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DFS_LISTING	',
     run_as_user = 'daasuser',
     dag=dag,
)
ECW_TRANSACTION =BashOperator(
     task_id='ECW_TRANSACTION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ECW_TRANSACTION	',
     run_as_user = 'daasuser',
     dag=dag,
)
KM_USER_ROLE =BashOperator(
     task_id='KM_USER_ROLE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh KM_USER_ROLE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NODE =BashOperator(
     task_id='NODE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NODE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NODE_ASSIGNMENT =BashOperator(
     task_id='NODE_ASSIGNMENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NODE_ASSIGNMENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER_HIST =BashOperator(
     task_id='PURCHASE_ORDER_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCHASE_ORDER_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCH_REQ_APPROVAL =BashOperator(
     task_id='PURCH_REQ_APPROVAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCH_REQ_APPROVAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER_APPROVAL =BashOperator(
     task_id='PURCHASE_ORDER_APPROVAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCHASE_ORDER_APPROVAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
DBA_TAB_PRIVS =BashOperator(
     task_id='DBA_TAB_PRIVS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DBA_TAB_PRIVS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DAILY_BVN_LINKING =BashOperator(
     task_id='DAILY_BVN_LINKING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DAILY_BVN_LINKING	',
     run_as_user = 'daasuser',
     dag=dag,
)
TBL_DATA_AGENT_REGISTRATION_VW =BashOperator(
     task_id='TBL_DATA_AGENT_REGISTRATION_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TBL_DATA_AGENT_REGISTRATION_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
PREPAID_PAYMENTS_LOG =BashOperator(
     task_id='PREPAID_PAYMENTS_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PREPAID_PAYMENTS_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_SITE_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_SITE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_SITE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUG_ACCESS_FEES =BashOperator(
     task_id='CUG_ACCESS_FEES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUG_ACCESS_FEES	',
     run_as_user = 'daasuser',
     dag=dag,
)
TBL_DATA_AGENT_REGISTRATION_VW_LIVE =BashOperator(
     task_id='TBL_DATA_AGENT_REGISTRATION_VW_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TBL_DATA_AGENT_REGISTRATION_VW_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
TOKEN_REPORT =BashOperator(
     task_id='TOKEN_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TOKEN_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_EXTRATIME_METRIC =BashOperator(
     task_id='ESM_EXTRATIME_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_EXTRATIME_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SHARENSELL_METRICS =BashOperator(
     task_id='ESM_SHARENSELL_METRICS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_SHARENSELL_METRICS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SPONSORED_DATA_MA =BashOperator(
     task_id='SPONSORED_DATA_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SPONSORED_DATA_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
CS5_SDP_VVE_CLR_DA =BashOperator(
     task_id='CS5_SDP_VVE_CLR_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_SDP_VVE_CLR_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_BOARD_D =BashOperator(
     task_id='MAPS_4G_BOARD_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_BOARD_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SUB_METRIC =BashOperator(
     task_id='ESM_SUB_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_SUB_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
SME_REPORTS =BashOperator(
     task_id='SME_REPORTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SME_REPORTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
HOURLY_RECHARGES_CHANNEL =BashOperator(
     task_id='HOURLY_RECHARGES_CHANNEL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HOURLY_RECHARGES_CHANNEL	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SIM_REG_METRIC =BashOperator(
     task_id='ESM_SIM_REG_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_SIM_REG_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
PORT_IN_OUT =BashOperator(
     task_id='PORT_IN_OUT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PORT_IN_OUT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SPONSORED_DATA_DA =BashOperator(
     task_id='SPONSORED_DATA_DA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SPONSORED_DATA_DA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HLR_D =BashOperator(
     task_id='MAPS_CORE_HLR_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_HLR_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_D_BH =BashOperator(
     task_id='MAPS_2G_CN_D_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CN_D_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_PMT_METRIC =BashOperator(
     task_id='ESM_PMT_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_PMT_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE =BashOperator(
     task_id='DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_CN_D =BashOperator(
     task_id='MAPS_CORE_CN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_CN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_D_BH =BashOperator(
     task_id='MAPS_3G_CN_D_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CN_D_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_REFILL_METRIC =BashOperator(
     task_id='ESM_REFILL_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_REFILL_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE =BashOperator(
     task_id='SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_UNSUB_METRIC =BashOperator(
     task_id='ESM_UNSUB_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_UNSUB_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_D =BashOperator(
     task_id='MAPS_3G_CN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFS_SALES_HIST =BashOperator(
     task_id='IFS_SALES_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFS_SALES_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
PRE_BSL_COUNTS =BashOperator(
     task_id='PRE_BSL_COUNTS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PRE_BSL_COUNTS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_DSR_ADHERENCE =BashOperator(
     task_id='TAS_DSR_ADHERENCE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAS_DSR_ADHERENCE	',
     run_as_user = 'daasuser',
     dag=dag,
)
FND_USER_ROLE =BashOperator(
     task_id='FND_USER_ROLE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FND_USER_ROLE	',
     run_as_user = 'daasuser',
     dag=dag,
)

IFSAPP_BUDGET_COMM_COST_VAR =BashOperator(
     task_id='IFSAPP_BUDGET_COMM_COST_VAR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_BUDGET_COMM_COST_VAR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_DETAILS =BashOperator(
     task_id='CUSTOMER_DETAILS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_DETAILS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DBA_ROLE_PRIVS =BashOperator(
     task_id='DBA_ROLE_PRIVS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DBA_ROLE_PRIVS	',
     run_as_user = 'daasuser',
     dag=dag,
)
IDENTITY_INVOICE_INFO =BashOperator(
     task_id='IDENTITY_INVOICE_INFO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IDENTITY_INVOICE_INFO	',
     run_as_user = 'daasuser',
     dag=dag,
)
DYA_DAILY_ACTIVATION =BashOperator(
     task_id='DYA_DAILY_ACTIVATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DYA_DAILY_ACTIVATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFS_SERVICE_CENTRE_SALES =BashOperator(
     task_id='IFS_SERVICE_CENTRE_SALES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFS_SERVICE_CENTRE_SALES	',
     run_as_user = 'daasuser',
     dag=dag,
)
STATE =BashOperator(
     task_id='STATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh STATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
VALID_USER =BashOperator(
     task_id='VALID_USER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh VALID_USER	',
     run_as_user = 'daasuser',
     dag=dag,
)
BYPASS_VENDOR =BashOperator(
     task_id='BYPASS_VENDOR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BYPASS_VENDOR	',
     run_as_user = 'daasuser',
     dag=dag,
)
OUTWARD =BashOperator(
     task_id='OUTWARD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh OUTWARD	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_PRIMARY_SALE =BashOperator(
     task_id='TAS_PRIMARY_SALE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAS_PRIMARY_SALE	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_ACCOUNT =BashOperator(
     task_id='IFSAPP_ACCOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_ACCOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_BIB_PAYMENT_REVERSAL_VW =BashOperator(
     task_id='MSO_BIB_PAYMENT_REVERSAL_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSO_BIB_PAYMENT_REVERSAL_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_CREDIT_INFO =BashOperator(
     task_id='CUSTOMER_CREDIT_INFO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_CREDIT_INFO	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_PLAN_AUTH =BashOperator(
     task_id='PAYMENT_PLAN_AUTH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_PLAN_AUTH	',
     run_as_user = 'daasuser',
     dag=dag,
)
PRICE_LIST =BashOperator(
     task_id='PRICE_LIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PRICE_LIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_SWAP =BashOperator(
     task_id='SIM_SWAP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIM_SWAP	',
     run_as_user = 'daasuser',
     dag=dag,
)
INACTIVE_DEVICES =BashOperator(
     task_id='INACTIVE_DEVICES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INACTIVE_DEVICES	',
     run_as_user = 'daasuser',
     dag=dag,
)
CODE_F =BashOperator(
     task_id='CODE_F' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CODE_F	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_INFO_COMM_METHOD =BashOperator(
     task_id='CUSTOMER_INFO_COMM_METHOD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_INFO_COMM_METHOD	',
     run_as_user = 'daasuser',
     dag=dag,
)
ENROLLMENT_REF =BashOperator(
     task_id='ENROLLMENT_REF' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ENROLLMENT_REF	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_CONNECT_USER_DETAILS =BashOperator(
     task_id='MTN_CONNECT_USER_DETAILS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTN_CONNECT_USER_DETAILS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_PRODUCT_MASTER =BashOperator(
     task_id='TAS_PRODUCT_MASTER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAS_PRODUCT_MASTER	',
     run_as_user = 'daasuser',
     dag=dag,
)
KYC_DEALER =BashOperator(
     task_id='KYC_DEALER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh KYC_DEALER	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_KPI_TARGET_VS_ACHIEVEMENT =BashOperator(
     task_id='TAS_KPI_TARGET_VS_ACHIEVEMENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAS_KPI_TARGET_VS_ACHIEVEMENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CODE_G =BashOperator(
     task_id='CODE_G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CODE_G	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_GATEWAY_AIRTIME =BashOperator(
     task_id='PAYMENT_GATEWAY_AIRTIME' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_GATEWAY_AIRTIME	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_GATEWAY =BashOperator(
     task_id='PAYMENT_GATEWAY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_GATEWAY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_CUST_AVAILABLE_CREDIT =BashOperator(
     task_id='MTN_CUST_AVAILABLE_CREDIT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTN_CUST_AVAILABLE_CREDIT	',
     run_as_user = 'daasuser',
     dag=dag,
)
INWARD =BashOperator(
     task_id='INWARD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INWARD	',
     run_as_user = 'daasuser',
     dag=dag,
)
CLM_WBO =BashOperator(
     task_id='CLM_WBO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CLM_WBO	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_BUDGET_YEAR_AMOUNT_UNION =BashOperator(
     task_id='IFSAPP_BUDGET_YEAR_AMOUNT_UNION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_BUDGET_YEAR_AMOUNT_UNION	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_CLOSING_STOCK_BALANCE =BashOperator(
     task_id='TAS_CLOSING_STOCK_BALANCE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAS_CLOSING_STOCK_BALANCE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MNP_LOCATION =BashOperator(
     task_id='MNP_LOCATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MNP_LOCATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
RETURN_MATERIAL_LINE =BashOperator(
     task_id='RETURN_MATERIAL_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RETURN_MATERIAL_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
QMATIC =BashOperator(
     task_id='QMATIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh QMATIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
PBM_PROBLEM_INVESTIGATION =BashOperator(
     task_id='PBM_PROBLEM_INVESTIGATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PBM_PROBLEM_INVESTIGATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
VAT_PERC =BashOperator(
     task_id='VAT_PERC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh VAT_PERC	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUST_ORD_CUSTOMER_ENT =BashOperator(
     task_id='CUST_ORD_CUSTOMER_ENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUST_ORD_CUSTOMER_ENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SUPPLIER_INFO =BashOperator(
     task_id='SUPPLIER_INFO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SUPPLIER_INFO	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_TERM =BashOperator(
     task_id='PAYMENT_TERM' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_TERM	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_GROUP =BashOperator(
     task_id='CUSTOMER_GROUP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_GROUP	',
     run_as_user = 'daasuser',
     dag=dag,
)
FND_USER =BashOperator(
     task_id='FND_USER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FND_USER	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_ADDRESS_GENERAL =BashOperator(
     task_id='PAYMENT_ADDRESS_GENERAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYMENT_ADDRESS_GENERAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
SALES_PRICE_LIST_PART =BashOperator(
     task_id='SALES_PRICE_LIST_PART' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SALES_PRICE_LIST_PART	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_RETAIL_OUTLETS =BashOperator(
     task_id='CB_RETAIL_OUTLETS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_RETAIL_OUTLETS	',
     run_as_user = 'daasuser',
     dag=dag,
)
CHANGE_REPORT_MTN =BashOperator(
     task_id='CHANGE_REPORT_MTN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CHANGE_REPORT_MTN	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_LOCATION_REGION =BashOperator(
     task_id='MTN_LOCATION_REGION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MTN_LOCATION_REGION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MASTER_DATA_VIEW =BashOperator(
     task_id='MASTER_DATA_VIEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MASTER_DATA_VIEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
DEALER_TYPE =BashOperator(
     task_id='DEALER_TYPE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DEALER_TYPE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CORPORATE_FORM =BashOperator(
     task_id='CORPORATE_FORM' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CORPORATE_FORM	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SIMSWAP_METRIC =BashOperator(
     task_id='ESM_SIMSWAP_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_SIMSWAP_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
RETAIL_SHOP =BashOperator(
     task_id='RETAIL_SHOP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RETAIL_SHOP	',
     run_as_user = 'daasuser',
     dag=dag,
)
VTU_KPI_METRICS =BashOperator(
     task_id='VTU_KPI_METRICS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh VTU_KPI_METRICS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SPON_PROVIDER_DTLS =BashOperator(
     task_id='SPON_PROVIDER_DTLS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SPON_PROVIDER_DTLS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_PGW_DAILY =BashOperator(
     task_id='MAPS_CORE_PGW_DAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_PGW_DAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER =BashOperator(
     task_id='PURCHASE_ORDER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PURCHASE_ORDER	',
     run_as_user = 'daasuser',
     dag=dag,
)
SUPPLIER_INFO_ADDRESS =BashOperator(
     task_id='SUPPLIER_INFO_ADDRESS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SUPPLIER_INFO_ADDRESS	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_INFO_TAB =BashOperator(
     task_id='CUSTOMER_INFO_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CUSTOMER_INFO_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_CUSTOMER_INFO_TAB =BashOperator(
     task_id='IFSAPP_CUSTOMER_INFO_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_CUSTOMER_INFO_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_CITY_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_CITY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_CITY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CC_ONLINE_ACTIVITY =BashOperator(
     task_id='CC_ONLINE_ACTIVITY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CC_ONLINE_ACTIVITY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CC_SURVEY =BashOperator(
     task_id='CC_SURVEY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CC_SURVEY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CC_AGENT_ACTIVITY =BashOperator(
     task_id='CC_AGENT_ACTIVITY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CC_AGENT_ACTIVITY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CANVASA =BashOperator(
     task_id='CANVASA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CANVASA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_PGW_WEEKLY =BashOperator(
     task_id='MAPS_CORE_PGW_WEEKLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_PGW_WEEKLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_W_BH =BashOperator(
     task_id='MAPS_3G_CN_W_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CN_W_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HSS_W =BashOperator(
     task_id='MAPS_CORE_HSS_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_HSS_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_BOARD_W =BashOperator(
     task_id='MAPS_4G_BOARD_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_BOARD_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MGW_W =BashOperator(
     task_id='MAPS_CORE_MGW_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MGW_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HLR_W =BashOperator(
     task_id='MAPS_CORE_HLR_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_HLR_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MSC_W =BashOperator(
     task_id='MAPS_CORE_MSC_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MSC_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_W =BashOperator(
     task_id='MAPS_3G_CN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
GUEST_PASS =BashOperator(
     task_id='GUEST_PASS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh GUEST_PASS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_W_BH =BashOperator(
     task_id='MAPS_2G_CN_W_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CN_W_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_ROUTE_W =BashOperator(
     task_id='MAPS_CORE_ROUTE_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_ROUTE_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MME_W =BashOperator(
     task_id='MAPS_CORE_MME_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MME_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_W_BH =BashOperator(
     task_id='MAPS_2G_CELL_W_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CELL_W_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_CN_W =BashOperator(
     task_id='MAPS_CORE_CN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_CN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_SWAP_SUCCESS_COUNT =BashOperator(
     task_id='SIM_SWAP_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIM_SWAP_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_REG_SUCCESS_COUNT =BashOperator(
     task_id='SIM_REG_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIM_REG_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIB_CREDIT_CONTROL_REPORT =BashOperator(
     task_id='BIB_CREDIT_CONTROL_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BIB_CREDIT_CONTROL_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
BULK_SMS =BashOperator(
     task_id='BULK_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BULK_SMS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_COUNTRY_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_COUNTRY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_COUNTRY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_COUNTRY_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_COUNTRY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_COUNTRY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_LGA_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_LGA_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_LGA_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_LICENSE =BashOperator(
     task_id='SMSC_LICENSE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMSC_LICENSE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_TERRITORY_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_TERRITORY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_TERRITORY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_STATE_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_STATE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_STATE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_STATE_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_STATE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_STATE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_CLUSTER_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_CLUSTER_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_CLUSTER_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_CLUSTER_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_CLUSTER_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_CLUSTER_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_TERRITORY_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_TERRITORY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_TERRITORY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_CITY_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_CITY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_CITY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE =BashOperator(
     task_id='SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
USSD_ERROR_CODE_BREAKDOWN =BashOperator(
     task_id='USSD_ERROR_CODE_BREAKDOWN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh USSD_ERROR_CODE_BREAKDOWN	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_SITE_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_SITE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_SITE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_LGA_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_LGA_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_LGA_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_REGION_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_REGION_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_DAILY_REGION_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_REGION_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_REGION_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_WEEKLY_REGION_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
COMPENSATION =BashOperator(
     task_id='COMPENSATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh COMPENSATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
EMM_DELIVERY_KPI =BashOperator(
     task_id='EMM_DELIVERY_KPI' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh EMM_DELIVERY_KPI	',
     run_as_user = 'daasuser',
     dag=dag,
)
APILOG =BashOperator(
     task_id='APILOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh APILOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_TOTAL_SUBMIT_SUCCESS_RATE =BashOperator(
     task_id='SMSC_TOTAL_SUBMIT_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMSC_TOTAL_SUBMIT_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
USSD_TRAFFIC_SUCCESS_RATE =BashOperator(
     task_id='USSD_TRAFFIC_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh USSD_TRAFFIC_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NG_USIM_STG =BashOperator(
     task_id='NG_USIM_STG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh NG_USIM_STG	',
     run_as_user = 'daasuser',
     dag=dag,
)
D_CONNECT_ACTIVATION_YYYYMM_LIVE =BashOperator(
     task_id='D_CONNECT_ACTIVATION_YYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh D_CONNECT_ACTIVATION_YYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_SITE_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_SITE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_SITE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_REGION_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_REGION_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_REGION_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_LGA_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_LGA_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_LGA_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_COUNTRY_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_COUNTRY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_COUNTRY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_CLUSTER_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_CLUSTER_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_CLUSTER_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_REPORT =BashOperator(
     task_id='WBS_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh WBS_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_W =BashOperator(
     task_id='MAPS_4G_CELL_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_CELL_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_D =BashOperator(
     task_id='MAPS_4G_CELL_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_CELL_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MNP_PORTING_BROADCAST =BashOperator(
     task_id='MNP_PORTING_BROADCAST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MNP_PORTING_BROADCAST	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_GGSN_W =BashOperator(
     task_id='MAPS_CORE_GGSN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_GGSN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_W =BashOperator(
     task_id='MAPS_CORE_SGSN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_SGSN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
DSA_SUCCESS_COUNT =BashOperator(
     task_id='DSA_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DSA_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_TERRITORY_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_TERRITORY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_TERRITORY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
TARIFF_TYPE =BashOperator(
     task_id='TARIFF_TYPE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TARIFF_TYPE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_ERROR_BREAKDOWN_PER_ACCOUNT =BashOperator(
     task_id='SMSC_ERROR_BREAKDOWN_PER_ACCOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMSC_ERROR_BREAKDOWN_PER_ACCOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MME_D =BashOperator(
     task_id='MAPS_CORE_MME_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MME_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MSC_D =BashOperator(
     task_id='MAPS_CORE_MSC_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MSC_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HSS_D =BashOperator(
     task_id='MAPS_CORE_HSS_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_HSS_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_GGSN_D =BashOperator(
     task_id='MAPS_CORE_GGSN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_GGSN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_D =BashOperator(
     task_id='MAPS_CORE_SGSN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_SGSN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MGW_D =BashOperator(
     task_id='MAPS_CORE_MGW_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MGW_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_ROUTE_D =BashOperator(
     task_id='MAPS_CORE_ROUTE_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_ROUTE_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYLOAD_DATA_AT_AREA_AND_BTS_ID =BashOperator(
     task_id='PAYLOAD_DATA_AT_AREA_AND_BTS_ID' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PAYLOAD_DATA_AT_AREA_AND_BTS_ID	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_CITY_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_CITY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_CITY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROVISIONING_LOG =BashOperator(
     task_id='PROVISIONING_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PROVISIONING_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
DPI_CDR =BashOperator(
     task_id='DPI_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DPI_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
SHOP_LOCATOR =BashOperator(
     task_id='SHOP_LOCATOR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SHOP_LOCATOR	',
     run_as_user = 'daasuser',
     dag=dag,
)
COSTS_FOR_PROFITABILITY =BashOperator(
     task_id='COSTS_FOR_PROFITABILITY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh COSTS_FOR_PROFITABILITY	',
     run_as_user = 'daasuser',
     dag=dag,
)
BILL_RUN_STATISTICS_TAB =BashOperator(
     task_id='BILL_RUN_STATISTICS_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BILL_RUN_STATISTICS_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MRKT_SIZING_NEXT_10_YRS =BashOperator(
     task_id='MRKT_SIZING_NEXT_10_YRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MRKT_SIZING_NEXT_10_YRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
COUNTRY_WIDE_PARAMETERS =BashOperator(
     task_id='COUNTRY_WIDE_PARAMETERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh COUNTRY_WIDE_PARAMETERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DIM_PACKAGE_NG =BashOperator(
     task_id='DIM_PACKAGE_NG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DIM_PACKAGE_NG	',
     run_as_user = 'daasuser',
     dag=dag,
)
ERM_10YRS =BashOperator(
     task_id='ERM_10YRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ERM_10YRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TECHNOLOGY_CELL_SPECIFICATION =BashOperator(
     task_id='TECHNOLOGY_CELL_SPECIFICATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TECHNOLOGY_CELL_SPECIFICATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
AREA_WIDE_PARAMETERS =BashOperator(
     task_id='AREA_WIDE_PARAMETERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AREA_WIDE_PARAMETERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DATA_QUALITY_CHECK_LOGS =BashOperator(
     task_id='DATA_QUALITY_CHECK_LOGS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DATA_QUALITY_CHECK_LOGS	',
     run_as_user = 'daasuser',
     dag=dag,
)
RECHARGE_SUCCESS_RATE =BashOperator(
     task_id='RECHARGE_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RECHARGE_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
INTERNATIONAL_ICX =BashOperator(
     task_id='INTERNATIONAL_ICX' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INTERNATIONAL_ICX	',
     run_as_user = 'daasuser',
     dag=dag,
)
AA_AD_BONUS =BashOperator(
     task_id='AA_AD_BONUS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AA_AD_BONUS	',
     run_as_user = 'daasuser',
     dag=dag,
)
AA_AD_BUNDLE =BashOperator(
     task_id='AA_AD_BUNDLE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AA_AD_BUNDLE	',
     run_as_user = 'daasuser',
     dag=dag,
)
AA_TARIFF_DATA =BashOperator(
     task_id='AA_TARIFF_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh AA_TARIFF_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_ACCURACY_STATISTICS =BashOperator(
     task_id='MSO_ACCURACY_STATISTICS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSO_ACCURACY_STATISTICS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_ZONE_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_ZONE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_ZONE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_M_DATA_BH =BashOperator(
     task_id='MAPS_2G_CN_M_DATA_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CN_M_DATA_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_STATE_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_STATE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MKT_MONTHLY_STATE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_M =BashOperator(
     task_id='MAPS_3G_CN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_3G_CN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
QERROR_NG =BashOperator(
     task_id='QERROR_NG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh QERROR_NG	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_DYA_METRIC =BashOperator(
     task_id='ESM_DYA_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_DYA_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
RBT_SUCCESS_COUNT =BashOperator(
     task_id='RBT_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh RBT_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_GPRS_FINAL =BashOperator(
     task_id='TAPOUT_GPRS_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPOUT_GPRS_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_VOICE_FINAL =BashOperator(
     task_id='TAPOUT_VOICE_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TAPOUT_VOICE_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
QRIOUS =BashOperator(
     task_id='QRIOUS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh QRIOUS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DIRECT_CONNECT_CCTP =BashOperator(
     task_id='DIRECT_CONNECT_CCTP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DIRECT_CONNECT_CCTP	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_VTU_VENDING_METRIC =BashOperator(
     task_id='ESM_VTU_VENDING_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ESM_VTU_VENDING_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HLR_M =BashOperator(
     task_id='MAPS_CORE_HLR_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_HLR_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_MONTHLY_BH =BashOperator(
     task_id='MAPS_2G_CELL_MONTHLY_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CELL_MONTHLY_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HSS_M =BashOperator(
     task_id='MAPS_CORE_HSS_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_HSS_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION =BashOperator(
     task_id='MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_ROUTE_M =BashOperator(
     task_id='MAPS_CORE_ROUTE_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_ROUTE_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_M =BashOperator(
     task_id='MAPS_4G_CELL_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_CELL_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_M_BH =BashOperator(
     task_id='MAPS_CORE_SGSN_M_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_SGSN_M_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_M_BH =BashOperator(
     task_id='MAPS_4G_CELL_M_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_CELL_M_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_M =BashOperator(
     task_id='MAPS_2G_CN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_2G_CN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_BOARD_M =BashOperator(
     task_id='MAPS_4G_BOARD_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_4G_BOARD_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_PGW_MONTHLY =BashOperator(
     task_id='MAPS_CORE_PGW_MONTHLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_PGW_MONTHLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MME_M =BashOperator(
     task_id='MAPS_CORE_MME_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MME_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_CN_M =BashOperator(
     task_id='MAPS_CORE_CN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_CN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_M =BashOperator(
     task_id='MAPS_CORE_SGSN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_SGSN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_GGSN_M =BashOperator(
     task_id='MAPS_CORE_GGSN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_GGSN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MSC_M =BashOperator(
     task_id='MAPS_CORE_MSC_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MSC_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MGW_M =BashOperator(
     task_id='MAPS_CORE_MGW_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MAPS_CORE_MGW_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
SW_CAPEX_FIXED =BashOperator(
     task_id='SW_CAPEX_FIXED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SW_CAPEX_FIXED	',
     run_as_user = 'daasuser',
     dag=dag,
)
HW_CAPEX_FIXED =BashOperator(
     task_id='HW_CAPEX_FIXED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HW_CAPEX_FIXED	',
     run_as_user = 'daasuser',
     dag=dag,
)
PENETRATION_RATIO_FIXED =BashOperator(
     task_id='PENETRATION_RATIO_FIXED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PENETRATION_RATIO_FIXED	',
     run_as_user = 'daasuser',
     dag=dag,
)
PSB_SYSTEM_ACCOUNT_BALANCES_REPORT =BashOperator(
     task_id='PSB_SYSTEM_ACCOUNT_BALANCES_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PSB_SYSTEM_ACCOUNT_BALANCES_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PSB_USER_ACCOUNT_BALANCES_REPORT =BashOperator(
     task_id='PSB_USER_ACCOUNT_BALANCES_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PSB_USER_ACCOUNT_BALANCES_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_IFSCONNECT_SALES_HIST =BashOperator(
     task_id='IFSAPP_IFSCONNECT_SALES_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_IFSCONNECT_SALES_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_SERVICE_CONNECT_STORE =BashOperator(
     task_id='IFSAPP_SERVICE_CONNECT_STORE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh IFSAPP_SERVICE_CONNECT_STORE	',
     run_as_user = 'daasuser',
     dag=dag,
)
ISP_LOGIN_IDS_LOOKUP =BashOperator(
     task_id='ISP_LOGIN_IDS_LOOKUP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh ISP_LOGIN_IDS_LOOKUP	',
     run_as_user = 'daasuser',
     dag=dag,
)
COMPENSATION_MON =BashOperator(
     task_id='COMPENSATION_MON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh COMPENSATION_MON	',
     run_as_user = 'daasuser',
     dag=dag,
)
DAILY_COMEBACK_RPT =BashOperator(
     task_id='DAILY_COMEBACK_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DAILY_COMEBACK_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
INVENTORY_TRANSACTION_HIST =BashOperator(
     task_id='INVENTORY_TRANSACTION_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh INVENTORY_TRANSACTION_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
PSB_REGISTRATIONS =BashOperator(
     task_id='PSB_REGISTRATIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh PSB_REGISTRATIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
FIN_LOG =BashOperator(
     task_id='FIN_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh FIN_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
LATEARRIVALREPORT =BashOperator(
     task_id='LATEARRIVALREPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh LATEARRIVALREPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_OPTOUT_AUTORENEWAL =BashOperator(
     task_id='HSDP_OPTOUT_AUTORENEWAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HSDP_OPTOUT_AUTORENEWAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_OPTIN_AUTORENEWAL =BashOperator(
     task_id='HSDP_OPTIN_AUTORENEWAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh HSDP_OPTIN_AUTORENEWAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIBDATA_FILTER_CS5_CCN_GPRS_MA =BashOperator(
     task_id='BIBDATA_FILTER_CS5_CCN_GPRS_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BIBDATA_FILTER_CS5_CCN_GPRS_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA =BashOperator(
     task_id='BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIBDATA_FILTER_CS5_CCN_VOICE_MA =BashOperator(
     task_id='BIBDATA_FILTER_CS5_CCN_VOICE_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh BIBDATA_FILTER_CS5_CCN_VOICE_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMF_DEVICE_MAP =BashOperator(
     task_id='SMF_DEVICE_MAP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SMF_DEVICE_MAP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SCHEDULES_LIVE_NEW =BashOperator(
     task_id='CB_SCHEDULES_LIVE_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CB_SCHEDULES_LIVE_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
SDP_API =BashOperator(
     task_id='SDP_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SDP_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
SITE_MAPPING =BashOperator(
     task_id='SITE_MAPPING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SITE_MAPPING	',
     run_as_user = 'daasuser',
     dag=dag,
)
OFFER_PLAN_INFORMATION =BashOperator(
     task_id='OFFER_PLAN_INFORMATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh OFFER_PLAN_INFORMATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_REVERSED_BILLING =BashOperator(
     task_id='MSO_REVERSED_BILLING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSO_REVERSED_BILLING	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIT_VAS_EVENT =BashOperator(
     task_id='SIT_VAS_EVENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIT_VAS_EVENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIT_USG_VOICE_OG =BashOperator(
     task_id='SIT_USG_VOICE_OG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SIT_USG_VOICE_OG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSC_CDR_TEMP =BashOperator(
     task_id='MSC_CDR_TEMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSC_CDR_TEMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
TT_MSO_1_ISP_LOGIN_IDS_LOOKUP =BashOperator(
     task_id='TT_MSO_1_ISP_LOGIN_IDS_LOOKUP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh TT_MSO_1_ISP_LOGIN_IDS_LOOKUP	',
     run_as_user = 'daasuser',
     dag=dag,
)
SERVICE_LIST =BashOperator(
     task_id='SERVICE_LIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh SERVICE_LIST	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_GPRS_AC = BashOperator(
     task_id='cs5_ccn_gprs_ac' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_GPRS_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_GPRS_MA = BashOperator(
     task_id='cs5_ccn_gprs_ma' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_GPRS_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_VOICE_AC = BashOperator(
     task_id='cs5_ccn_voice_ac' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_VOICE_AC	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS5_CCN_VOICE_MA = BashOperator(
     task_id='cs5_ccn_voice_ma' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS5_CCN_VOICE_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)

CS6_CCN_CDR = BashOperator(
     task_id='cs6_ccn_cdr' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh CS6_CCN_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)

DPI_CDR_HISTORICAL = BashOperator(
     task_id='dpi_cdr_historical' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DPI_CDR_HISTORICAL	',
     run_as_user = 'daasuser',
     dag=dag,
)

DpiCdrUnpack = BashOperator(
     task_id='dpicdrunpack' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh DpiCdrUnpack	',
     run_as_user = 'daasuser',
     dag=dag,
)

GGSN_CDR = BashOperator(
     task_id='ggsn_cdr' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh GGSN_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)

#LEA_MAPPING_MSC_DAAS = BashOperator(
#     task_id='lea_mapping_msc_daas' ,
#     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh LEA_MAPPING_MSC_DAAS	',
#     run_as_user = 'daasuser',
#     dag=dag,
#)


MSC_CDR = BashOperator(
     task_id='msc_cdr' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSC_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)

MSC_DAAS = BashOperator(
     task_id='msc_daas' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh MSC_DAAS	',
     run_as_user = 'daasuser',
     dag=dag,
)

WBS_PM_RATED_CDRS = BashOperator(
     task_id='wbs_pm_rated_cdrs' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs_back_logs.sh WBS_PM_RATED_CDRS	',
     run_as_user = 'daasuser',
     dag=dag,
)

END = BashOperator(
     task_id='end' ,
     bash_command='echo "end"	',
     run_as_user = 'daasuser',
     dag=dag,
)


START >> [SDP_DMP_DA,  SDP_DMP_AC,  SGSN_CDR,  MA_MONITOR,  CS5_CCN_GPRS_DA,  OFFER_DUMP,  DPI_CDR_NEW,  CS5_CCN_VOICE_DA,  CS5_AIR_REFILL_AC,  SMSC,  UC_DUMP,  NEWREG_BIOUPDT_POOL,  CS6_SDP_CDR,  CB_SERV_MAST_VIEW_LIVE,  UT_DUMP,  CS5_SDP_ACC_ADJ_MA,  CB_ACCOUNT_MASTER,  CS5_SDP_PAM_ALL,  DMC_DUMP_ALL,  CS5_CCN_SMS_AC,  UDC_DUMP,  CS6_AIR_CDR,  SDP_DMP_MA,  USSD_CDR,  CS5_CCN_SMS_MA,  HSDP_CDR,  NGVS_DUMP,  CS5_SDP_ACC_ADJ_AC,  GSM_SIMS_MASTER_LIVE,  IVR_SERVICE,  CS5_AIR_REFILL_MA,  CS5_SDP_ACC_ADJ_DA,  NEWREG_BIOUPDT_POOL_WEEKLY,  LOCATION,  DSLCUSTOMERVERSIONED,  CIS_CDR,  NGVS_CDR,  CDR_DATA,  CUSTOMEREVENTSMSG,  CS5_AIR_ADJ_MA,  CS5_AIR_REFILL_DA,  CS5_CCN_SMS_DA,  FLYTXT_CAMPAIGN_EVENTS_DATA,  NGVS_DUMP_20190325,  CB_SERV_MAST_VIEW_LIVE_INC,  MVAS_DND_MSISDN_REPORT,  CDR_SUBSCRIPTION,  NEWREG_BIOUPDT_POOL_LIVE,  HSDP_RENEWAL_BASE,  RECON,  SUBSCRIBER_TRANSACTIONS_CDR,  HSDP_SUMP_PRESTO,  ERS_VEND_NEW,  ERS_VEND,  RECHARGE,  LBN,  EDW_REPORT,  CS5_SDP_ACC_ADJ_MA_TMP,  DIGITAL_FOOTPRINT_NEW,  HSDP_DOL_LOG,  MFS_REGISTRATIONS,  MFS_ACQUISITION,  CS5_AIR_ADJ_DA,  SMART_APP_CDR,  WBS_CLIENT_DAARS_TAPIN,  CLIENT_ACTIVITY_LOG,  SAG_API,  MFS_ACCOUNTS,  CB_SERV_MAST_VIEW,  CDR_VOICE,  WBS_CLIENT_DAARS_TAPOUT,  CS5_VTU_DUMP,  TAPIN_GPRS,  MYMTNAPP,  SIS_TOKEN,  AVAYA_IVR,  SMS_ACTIVATION_REQUEST_LIVE,  CUSTEVENTSMSGDAILY,  TBL_IMEI_REGISTRATION_DTLS_VW,  PROFILE_WEEK,  PROFIL_BDAIL,  PROFIL_ADAIL,  HUAMSC_DAAS_STG,  IFSAPP_MANUF_FILE_DETAIL,  CDR_RECHARGE,  NEWREG_BIOUPDT_POOL_DAILY,  FLYTXT_INBOUND_EVENTS_DATA,  REVENUE,  CALL_REASON,  MFS_ACTIVE_SUBSCRIBERS,  QRIOS_TRANSACTIONS,  NAS_LISTING,  USG_REALTIME,  USG_VOICE_IC,  UPC_SUBSCRIPTION,  MAPS_3G_CELL_D,  USG_DATA_SMS,  IVR_DATA,  TAPIN_VOICE,  SMS_ACTIVATION_REQUEST_KPI,  CB_SUBS_POS_SERVICES,  ABLT_GSM_STARTER_PACK,  FCT_FIN_INTERCONNECT,  QUARANTINED_MSISDN_BIB_LIVE,  HISTORY_LOG_ATTRIBUTE,  SRM_REQUEST,  AUDIT_LOGS,  MAPS_2G_CELL_D,  SMS_ACTIVATION_VW,  AVAYA_CLID,  TAPOUT_GPRS,  SMS_ACTIVATION_REQUEST_JOIN,  MSO_PROCESS_AVG_TIME,  CDR_SMS,  MFS_AGENT_COMMISSION,  EVD_TRANSACTIONS,  MAPS3G,  BUNDLE4U_VOICE,  DUMP_SHARESELL,  MOBILE_MONEY,  CS5_SDP_LCY_CLR_DA,  SERV_STATUS_SEAMFIX_REF,  BSL_LISTING_FILES,  CLAWBACK_RPT,  CB_SCHEDULES,  AGENT_USER_BIB_LIVE,  MULTIPLE_REG_BIB,  BILL_SUMMARY_REP_MON,  WBS_BIB_REPORT,  MAPS2G,  BALANCES,  D_DIRECT_EVENT_YYYYMM_LIVE,  INV_SPECIFIC_PAYMENT_VW,  XAAS_DAILY,  IFSAPP_INVENTORY_TRANSACTION_HIS,  REFILL_EVENT,  ALL_STMT_SERVICE_OPEN_CRED_VW,  AGENT_USER_BIB,  EXCESS_PAYMENT_RPT,  APLIMAN_OBD,  TRANSACTING_AGENT,  CALL_REASON_MONTHLY,  CB_POS_TRANSACTIONS,  DEVICE_MAPPER_LIVE,  FINANCIAL_LOG,  PACK_SUB_EVNT,  IFSAPP_PRO_BUDGET_COMMITMENTS,  TAPOUT_VOICE,  SMSC_TOTAL_DELIVERY_SUCCESS_RATE,  OTP_STATUS,  EVD_STOCK_LEVEL,  ALL_STMT_SERVICE_OPEN_DEBT_VW,  BUNDLE4U_GPRS,  SMS_ACTIVATION_REQUEST,  SESSION,  MTNBIB_PALLET_ALLSKYYYYMM_LIVE,  CREDIT_INFORMATION_MON,  FLYTXT_LATCH_DUMP,  LEDGER_ITEM_TAB,  NAS_LISTING_DIRC,  CUG_ACCESS_FEE_BASE_VW,  FINANCIALLOG,  HISTORY_LOG,  MULTIPLE_REG_BIB_LIVE,  INPUTADAPTERSTATSMSG_TEXT,  MNP,  SMART_APP_NEW_USERS,  MTNBIB_SHIPPER_ALLSKYYYYMM_LIVE,  IFSAPP_CUSTOMER_ORDER_LINE,  CGW_API,  ALL_STMT_ACCOUNT_OPEN_INV_VW,  CUSTOMER_ORDER_HISTORY,  SMS_INCENTIVE_IMEI_VW,  SIM_TRANSACTIONS,  IFSAPP_PURCHASE_ORDER_LINE_TAB,  TAPIN_VOICE_FINAL,  GEN_LED_VOUCHER,  IB_API,  MFS_STOCK_LEVEL,  MAPS_2G_CELL_WEEKLY,  MTN_PR_DETAILS,  MFS_AUDIT_LOG,  MAPS_3G_CELL_D_BH,  PAYMENT_TAB,  HPD_HELP_DESK,  BUDGET_PERIOD_AMOUNT,  ACCOUNTING_BALANCE_TAB,  INVENTORY_PART_IN_STOCK,  CUSTOMER_ORDER_LINE_TAB,  MAN_SUPP_INVOICE_ITEM,  RBT_EVENT,  MTN_OVERVIEW_COMM_RPT,  PAYMENT_REPORT_ALL,  EDW_SIM_SWAP,  TAPIN_GPRS_FINAL,  PURCHASE_REQ_LINE,  CUSTOMER_ORDER_TAB,  PURCHASE_ORDER_LINE,  MAPS_3G_CELL_M,  KM_USER,  MSO_MNP_NUMBER_SUMMARY,  MAN_SUPP_INVOICE,  MAPS_INV_CELL,  PURCHASE_RECEIPT_TAB,  MAPS4G,  TAX_ITEM_QRY,  BLACKLIST_HISTORY,  IFSAPP_MTN_RECEIPT_RECONC_RPT,  BUDGET_YEAR_AMOUNT,  SMART_APP_DOWNLOAD,  NEWREG_BIOUPDT_POOL_DAILY_TMP,  MAPS_3G_CELL_M_BH,  BSL_LISTING,  CB_SCHEDULES_LIVE,  DFS_LISTING,  ECW_TRANSACTION,  KM_USER_ROLE,  NODE,  NODE_ASSIGNMENT,  PURCHASE_ORDER_HIST,  PURCH_REQ_APPROVAL,  PURCHASE_ORDER_APPROVAL,  DBA_TAB_PRIVS,  DAILY_BVN_LINKING,  TBL_DATA_AGENT_REGISTRATION_VW,  PREPAID_PAYMENTS_LOG,  MKT_DAILY_SITE_AH_REPORT,  CUG_ACCESS_FEES,  TBL_DATA_AGENT_REGISTRATION_VW_LIVE,  TOKEN_REPORT,  ESM_EXTRATIME_METRIC,  ESM_SHARENSELL_METRICS,  SPONSORED_DATA_MA,  CS5_SDP_VVE_CLR_DA,  MAPS_4G_BOARD_D,  ESM_SUB_METRIC,  SME_REPORTS,  HOURLY_RECHARGES_CHANNEL,  ESM_SIM_REG_METRIC,  PORT_IN_OUT,  SPONSORED_DATA_DA,  MAPS_CORE_HLR_D,  MAPS_2G_CN_D_BH,  ESM_PMT_METRIC,  DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,  MAPS_CORE_CN_D,  MAPS_3G_CN_D_BH,  ESM_REFILL_METRIC,  SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,  ESM_UNSUB_METRIC,  MAPS_3G_CN_D,  IFS_SALES_HIST,  PRE_BSL_COUNTS,  TAS_DSR_ADHERENCE,  FND_USER_ROLE,  IFSAPP_BUDGET_COMM_COST_VAR,  CUSTOMER_DETAILS,  DBA_ROLE_PRIVS,  IDENTITY_INVOICE_INFO,  DYA_DAILY_ACTIVATION,  IFS_SERVICE_CENTRE_SALES,  STATE,  VALID_USER,  BYPASS_VENDOR,  OUTWARD,  TAS_PRIMARY_SALE,  IFSAPP_ACCOUNT,  MSO_BIB_PAYMENT_REVERSAL_VW,  CUSTOMER_CREDIT_INFO,  PAYMENT_PLAN_AUTH,  PRICE_LIST,  SIM_SWAP,  INACTIVE_DEVICES,  CODE_F,  CUSTOMER_INFO_COMM_METHOD,  ENROLLMENT_REF,  MTN_CONNECT_USER_DETAILS,  TAS_PRODUCT_MASTER,  KYC_DEALER,  TAS_KPI_TARGET_VS_ACHIEVEMENT,  CODE_G,  PAYMENT_GATEWAY_AIRTIME,  PAYMENT_GATEWAY,  MTN_CUST_AVAILABLE_CREDIT,  INWARD,  CLM_WBO,  IFSAPP_BUDGET_YEAR_AMOUNT_UNION,  TAS_CLOSING_STOCK_BALANCE,  MNP_LOCATION,  RETURN_MATERIAL_LINE,  QMATIC,  PBM_PROBLEM_INVESTIGATION,  VAT_PERC,  CUST_ORD_CUSTOMER_ENT,  SUPPLIER_INFO,  PAYMENT_TERM,  CUSTOMER_GROUP,  FND_USER,  PAYMENT_ADDRESS_GENERAL,  SALES_PRICE_LIST_PART,  CB_RETAIL_OUTLETS,  CHANGE_REPORT_MTN,  MTN_LOCATION_REGION,  MASTER_DATA_VIEW,  DEALER_TYPE,  CORPORATE_FORM,  ESM_SIMSWAP_METRIC,  RETAIL_SHOP,  VTU_KPI_METRICS,  SPON_PROVIDER_DTLS,  MAPS_CORE_PGW_DAILY,  PURCHASE_ORDER,  SUPPLIER_INFO_ADDRESS,  CUSTOMER_INFO_TAB,  IFSAPP_CUSTOMER_INFO_TAB,  MKT_DAILY_CITY_AH_REPORT,  CC_ONLINE_ACTIVITY,  CC_SURVEY,  CC_AGENT_ACTIVITY,  CANVASA,  MAPS_CORE_PGW_WEEKLY,  MAPS_3G_CN_W_BH,  MAPS_CORE_HSS_W,  MAPS_4G_BOARD_W,  MAPS_CORE_MGW_W,  MAPS_CORE_HLR_W,  MAPS_CORE_MSC_W,  MAPS_3G_CN_W,  GUEST_PASS,  MAPS_2G_CN_W_BH,  MAPS_CORE_ROUTE_W,  MAPS_CORE_MME_W,  MAPS_2G_CELL_W_BH,  MAPS_CORE_CN_W,  SIM_SWAP_SUCCESS_COUNT,  SIM_REG_SUCCESS_COUNT,  BIB_CREDIT_CONTROL_REPORT,  BULK_SMS,  MKT_DAILY_COUNTRY_AH_REPORT,  MKT_WEEKLY_COUNTRY_AH_REPORT,  MKT_WEEKLY_LGA_AH_REPORT,  SMSC_LICENSE,  MKT_WEEKLY_TERRITORY_AH_REPORT,  MKT_DAILY_STATE_AH_REPORT,  MKT_WEEKLY_STATE_AH_REPORT,  MKT_WEEKLY_CLUSTER_AH_REPORT,  MKT_DAILY_CLUSTER_AH_REPORT,  MKT_DAILY_TERRITORY_AH_REPORT,  MKT_WEEKLY_CITY_AH_REPORT,  SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,  USSD_ERROR_CODE_BREAKDOWN,  MKT_WEEKLY_SITE_AH_REPORT,  MKT_DAILY_LGA_AH_REPORT,  MKT_DAILY_REGION_AH_REPORT,  MKT_WEEKLY_REGION_AH_REPORT,  COMPENSATION,  EMM_DELIVERY_KPI,  APILOG,  SMSC_TOTAL_SUBMIT_SUCCESS_RATE,  USSD_TRAFFIC_SUCCESS_RATE,  NG_USIM_STG,  D_CONNECT_ACTIVATION_YYYYMM_LIVE,  MKT_MONTHLY_SITE_AH_REPORT,  MKT_MONTHLY_REGION_AH_REPORT,  MKT_MONTHLY_LGA_AH_REPORT,  MKT_MONTHLY_COUNTRY_AH_REPORT,  MKT_MONTHLY_CLUSTER_AH_REPORT,  WBS_REPORT,  MAPS_4G_CELL_W,  MAPS_4G_CELL_D,  MNP_PORTING_BROADCAST,  MAPS_CORE_GGSN_W,  MAPS_CORE_SGSN_W,  DSA_SUCCESS_COUNT,  MKT_MONTHLY_TERRITORY_AH_REPORT,  TARIFF_TYPE,  SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,  MAPS_CORE_MME_D,  MAPS_CORE_MSC_D,  MAPS_CORE_HSS_D,  MAPS_CORE_GGSN_D,  MAPS_CORE_SGSN_D,  MAPS_CORE_MGW_D,  MAPS_CORE_ROUTE_D,  PAYLOAD_DATA_AT_AREA_AND_BTS_ID,  MKT_MONTHLY_CITY_AH_REPORT,  PROVISIONING_LOG,  DPI_CDR,  SHOP_LOCATOR,  COSTS_FOR_PROFITABILITY,  BILL_RUN_STATISTICS_TAB,  MRKT_SIZING_NEXT_10_YRS,  COUNTRY_WIDE_PARAMETERS,  DIM_PACKAGE_NG,  ERM_10YRS,  TECHNOLOGY_CELL_SPECIFICATION,  AREA_WIDE_PARAMETERS,  DATA_QUALITY_CHECK_LOGS,  RECHARGE_SUCCESS_RATE,  INTERNATIONAL_ICX,  AA_AD_BONUS,  AA_AD_BUNDLE,  AA_TARIFF_DATA,  MSO_ACCURACY_STATISTICS,  MKT_MONTHLY_ZONE_AH_REPORT,  MAPS_2G_CN_M_DATA_BH,  MKT_MONTHLY_STATE_AH_REPORT,  MAPS_3G_CN_M,  QERROR_NG,  ESM_DYA_METRIC,  RBT_SUCCESS_COUNT,  TAPOUT_GPRS_FINAL,  TAPOUT_VOICE_FINAL,  QRIOUS,  DIRECT_CONNECT_CCTP,  ESM_VTU_VENDING_METRIC,  MAPS_CORE_HLR_M,  MAPS_2G_CELL_MONTHLY_BH,  MAPS_CORE_HSS_M,  MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION,  MAPS_CORE_ROUTE_M,  MAPS_4G_CELL_M,  MAPS_CORE_SGSN_M_BH,  MAPS_4G_CELL_M_BH,  MAPS_2G_CN_M,  MAPS_4G_BOARD_M,  MAPS_CORE_PGW_MONTHLY,  MAPS_CORE_MME_M,  MAPS_CORE_CN_M,  MAPS_CORE_SGSN_M,  MAPS_CORE_GGSN_M,  MAPS_CORE_MSC_M,  MAPS_CORE_MGW_M,  SW_CAPEX_FIXED,  HW_CAPEX_FIXED,  PENETRATION_RATIO_FIXED,  PSB_SYSTEM_ACCOUNT_BALANCES_REPORT,  PSB_USER_ACCOUNT_BALANCES_REPORT,  IFSAPP_IFSCONNECT_SALES_HIST,  IFSAPP_SERVICE_CONNECT_STORE,  ISP_LOGIN_IDS_LOOKUP,  COMPENSATION_MON,  DAILY_COMEBACK_RPT,  INVENTORY_TRANSACTION_HIST,  PSB_REGISTRATIONS,  FIN_LOG,  LATEARRIVALREPORT,  HSDP_OPTOUT_AUTORENEWAL,  HSDP_OPTIN_AUTORENEWAL,  BIBDATA_FILTER_CS5_CCN_GPRS_MA,  BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA,  BIBDATA_FILTER_CS5_CCN_VOICE_MA,  SMF_DEVICE_MAP,  CB_SCHEDULES_LIVE_NEW,  SDP_API,  SITE_MAPPING,  OFFER_PLAN_INFORMATION,  MSO_REVERSED_BILLING,  SIT_VAS_EVENT,  SIT_USG_VOICE_OG,  MSC_CDR_TEMP,  TT_MSO_1_ISP_LOGIN_IDS_LOOKUP,  SERVICE_LIST,  CS5_CCN_GPRS_AC,  CS5_CCN_GPRS_MA,  CS5_CCN_VOICE_AC,  CS5_CCN_VOICE_MA,  CS6_CCN_CDR,  DPI_CDR_HISTORICAL,  DpiCdrUnpack,  GGSN_CDR,  MSC_CDR,  MSC_DAAS,  WBS_PM_RATED_CDRS] >> END
