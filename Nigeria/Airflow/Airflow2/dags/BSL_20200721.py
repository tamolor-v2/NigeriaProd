import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = yesterday.strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=2)
hour_run = hour_run_date.strftime('%H')

list_feed = 'BUNDLE4U_GPRS,BUNDLE4U_VOICE,CS5_AIR_ADJ_MA,CS5_AIR_REFILL_MA,CS5_CCN_GPRS_MA,CS5_CCN_SMS_MA,CS5_CCN_VOICE_MA,CS5_SDP_ACC_ADJ_MA,DPI_CDR,MSC_CDR  '
feed_map = {'bundle4u_gprs': 1, 'bundle4u_voice': 1, 'cs5_air_adj_ma': 1, 'cs5_air_refill_ma': 1, 'cs5_ccn_gprs_ma': 1, 'cs5_ccn_sms_ma': 1, 'cs5_ccn_voice_ma': 1, 'cs5_sdp_acc_adj_ma': 1, 'dpi_cdr': 1, 'msc_cdr': 1}


#pathcsv = '/nas/share05/tools/DQ/status2/'
#pathCheckExtract = '/nas/share05/FlareProd/Run/Bsl/DataExtract/ExportedDataStatus'
phases = ['Extract_','Dedup2_','PCF_','faile_']
#list_Container = "dim_package_ng vp_hsdp_lookup mnp_porting_broadcast shortcodes_number_prefix dim_wallet_2 sp_qrios_product hsdp_lookup vp_dim_cell dim_prefix dim_subscriber_type dim_wallet_ng operators nigeria_dim_wallets_ng uc_data_exclude"
#topic_kafka = 'flare_jtm_kafka_in_5'
#email = "test@ligadata.com"
working_dir='/mnt/beegfs_bsl/Deployment/DEV/scripts/BslDriver'

pathCheckExtract = Variable.get("pathCheckExtract", deserialize_json=True)
list_Container = Variable.get("list_Container", deserialize_json=True)
topic_kafka = Variable.get("topic_kafka", deserialize_json=True)
Email = Variable.get("Email", deserialize_json=True)
sleeptime = Variable.get("sleeptime", deserialize_json=True)
ValidationCode= Variable.get("ValidationCode", deserialize_json=True)
BSLCode= Variable.get("BSLCode", deserialize_json=True)
KafkaCheckcommand= Variable.get("KafkaCheckcommand", deserialize_json=True)
extract_command= Variable.get("extract_command", deserialize_json=True)
CheckExtract= Variable.get("CheckExtract", deserialize_json=True)
ContainerCheckcomands= Variable.get("ContainerCheckcomands", deserialize_json=True)
dedup_command= Variable.get("dedup_command", deserialize_json=True)
dedup_command_2=Variable.get("dedup_command_2", deserialize_json=True)
dedup_command_dpi_cdr=Variable.get("dedup_command_dpi_cdr", deserialize_json=True)
pcf_command= Variable.get("pcf_command", deserialize_json=True)
PCFCheck_command= Variable.get("PCFCheck_command", deserialize_json=True)
list_feed_ext=Variable.get("ExtractGroup2A", deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date': datetime(2019,12,3),
    'email': ['saleh@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('BSL_Test_20200721', default_args=default_args, catchup=False,schedule_interval=None)



feedname_Task_v7 = 'bundle4u_gprs,bundle4u_voice,cs5_air_adj_ma,cs5_air_refill_ma,cs5_ccn_gprs_ma,cs5_ccn_sms_ma,cs5_ccn_voice_ma,cs5_sdp_acc_adj_ma,dpi_cdr,msc_cdr'

Extract_Task_v7 = BashOperator(
    task_id='Extract_Task_v7',
    bash_command = extract_command.format(d_1,list_feed_ext),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

CheckExtractTask_v7 = BashOperator(
    task_id='CheckExtract_Task_v7',
    bash_command = CheckExtract.format(d_1,pathCheckExtract),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

Extract_Task_v7 >> CheckExtractTask_v7 

feedname_bundle4u_gprs = 'bundle4u_gprs'.upper()
feedname2_bundle4u_gprs = 'bundle4u_gprs'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_bundle4u_gprs = BashOperator(
    task_id='PCF_bundle4u_gprs',
    bash_command= pcf_command.format(d_1,feedname_bundle4u_gprs),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_bundle4u_gprs = BashOperator(
    task_id='Dedup_bundle4u_gprs',
    bash_command= dedup_command.format(d_1, feedname_bundle4u_gprs, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_bundle4u_gprs = BashOperator(
    task_id='PCFCheck_bundle4u_gprs',
    bash_command=PCFCheck_command.format(feedname_bundle4u_gprs,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_bundle4u_gprs = BashOperator(
    task_id='Validation_bundle4u_gprs',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_bundle4u_gprs),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_bundle4u_gprs >> PCFCheck_bundle4u_gprs  >> Dedup_bundle4u_gprs >> [Extract_Task_v7 , Validation_bundle4u_gprs]



feedname_bundle4u_voice = 'bundle4u_voice'.upper()
feedname2_bundle4u_voice = 'bundle4u_voice'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_bundle4u_voice = BashOperator(
    task_id='PCF_bundle4u_voice',
    bash_command= pcf_command.format(d_1,feedname_bundle4u_voice),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_bundle4u_voice = BashOperator(
    task_id='Dedup_bundle4u_voice',
    bash_command= dedup_command.format(d_1, feedname_bundle4u_voice, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_bundle4u_voice = BashOperator(
    task_id='PCFCheck_bundle4u_voice',
    bash_command=PCFCheck_command.format(feedname_bundle4u_voice,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_bundle4u_voice = BashOperator(
    task_id='Validation_bundle4u_voice',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_bundle4u_voice),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_bundle4u_voice >> PCFCheck_bundle4u_voice  >> Dedup_bundle4u_voice >> [Extract_Task_v7 , Validation_bundle4u_voice]



feedname_cs5_air_adj_ma = 'cs5_air_adj_ma'.upper()
feedname2_cs5_air_adj_ma = 'cs5_air_adj_ma'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_cs5_air_adj_ma = BashOperator(
    task_id='PCF_cs5_air_adj_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_adj_ma),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_cs5_air_adj_ma = BashOperator(
    task_id='Dedup_cs5_air_adj_ma',
    bash_command= dedup_command.format(d_1, feedname_cs5_air_adj_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_cs5_air_adj_ma = BashOperator(
    task_id='PCFCheck_cs5_air_adj_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_air_adj_ma,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_cs5_air_adj_ma = BashOperator(
    task_id='Validation_cs5_air_adj_ma',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_cs5_air_adj_ma),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_cs5_air_adj_ma >> PCFCheck_cs5_air_adj_ma  >> Dedup_cs5_air_adj_ma >> [Extract_Task_v7 , Validation_cs5_air_adj_ma]



feedname_cs5_air_refill_ma = 'cs5_air_refill_ma'.upper()
feedname2_cs5_air_refill_ma = 'cs5_air_refill_ma'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_cs5_air_refill_ma = BashOperator(
    task_id='PCF_cs5_air_refill_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_refill_ma),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_cs5_air_refill_ma = BashOperator(
    task_id='Dedup_cs5_air_refill_ma',
    bash_command= dedup_command.format(d_1, feedname_cs5_air_refill_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_cs5_air_refill_ma = BashOperator(
    task_id='PCFCheck_cs5_air_refill_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_air_refill_ma,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_cs5_air_refill_ma = BashOperator(
    task_id='Validation_cs5_air_refill_ma',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_cs5_air_refill_ma),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_cs5_air_refill_ma >> PCFCheck_cs5_air_refill_ma  >> Dedup_cs5_air_refill_ma >> [Extract_Task_v7 , Validation_cs5_air_refill_ma]



feedname_cs5_ccn_gprs_ma = 'cs5_ccn_gprs_ma'.upper()
feedname2_cs5_ccn_gprs_ma = 'cs5_ccn_gprs_ma'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_cs5_ccn_gprs_ma = BashOperator(
    task_id='PCF_cs5_ccn_gprs_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_gprs_ma),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_cs5_ccn_gprs_ma = BashOperator(
    task_id='Dedup_cs5_ccn_gprs_ma',
    bash_command= dedup_command_2.format(d_1, feedname_cs5_ccn_gprs_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_cs5_ccn_gprs_ma = BashOperator(
    task_id='PCFCheck_cs5_ccn_gprs_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_gprs_ma,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_cs5_ccn_gprs_ma = BashOperator(
    task_id='Validation_cs5_ccn_gprs_ma',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_cs5_ccn_gprs_ma),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_cs5_ccn_gprs_ma >> PCFCheck_cs5_ccn_gprs_ma  >> Dedup_cs5_ccn_gprs_ma >> [Extract_Task_v7 , Validation_cs5_ccn_gprs_ma]



feedname_cs5_ccn_sms_ma = 'cs5_ccn_sms_ma'.upper()
feedname2_cs5_ccn_sms_ma = 'cs5_ccn_sms_ma'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_cs5_ccn_sms_ma = BashOperator(
    task_id='PCF_cs5_ccn_sms_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_sms_ma),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_cs5_ccn_sms_ma = BashOperator(
    task_id='Dedup_cs5_ccn_sms_ma',
    bash_command= dedup_command_2.format(d_1, feedname_cs5_ccn_sms_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_cs5_ccn_sms_ma = BashOperator(
    task_id='PCFCheck_cs5_ccn_sms_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_sms_ma,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_cs5_ccn_sms_ma = BashOperator(
    task_id='Validation_cs5_ccn_sms_ma',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_cs5_ccn_sms_ma),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_cs5_ccn_sms_ma >> PCFCheck_cs5_ccn_sms_ma  >> Dedup_cs5_ccn_sms_ma >> [Extract_Task_v7 , Validation_cs5_ccn_sms_ma]



feedname_cs5_ccn_voice_ma = 'cs5_ccn_voice_ma'.upper()
feedname2_cs5_ccn_voice_ma = 'cs5_ccn_voice_ma'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_cs5_ccn_voice_ma = BashOperator(
    task_id='PCF_cs5_ccn_voice_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_voice_ma),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_cs5_ccn_voice_ma = BashOperator(
    task_id='Dedup_cs5_ccn_voice_ma',
    bash_command= dedup_command_2.format(d_1, feedname_cs5_ccn_voice_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_cs5_ccn_voice_ma = BashOperator(
    task_id='PCFCheck_cs5_ccn_voice_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_voice_ma,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_cs5_ccn_voice_ma = BashOperator(
    task_id='Validation_cs5_ccn_voice_ma',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_cs5_ccn_voice_ma),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_cs5_ccn_voice_ma >> PCFCheck_cs5_ccn_voice_ma  >> Dedup_cs5_ccn_voice_ma >> [Extract_Task_v7 , Validation_cs5_ccn_voice_ma]



feedname_cs5_sdp_acc_adj_ma = 'cs5_sdp_acc_adj_ma'.upper()
feedname2_cs5_sdp_acc_adj_ma = 'cs5_sdp_acc_adj_ma'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='PCF_cs5_sdp_acc_adj_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_sdp_acc_adj_ma),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='Dedup_cs5_sdp_acc_adj_ma',
    bash_command= dedup_command.format(d_1, feedname_cs5_sdp_acc_adj_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='PCFCheck_cs5_sdp_acc_adj_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_sdp_acc_adj_ma,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='Validation_cs5_sdp_acc_adj_ma',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_cs5_sdp_acc_adj_ma),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_cs5_sdp_acc_adj_ma >> PCFCheck_cs5_sdp_acc_adj_ma  >> Dedup_cs5_sdp_acc_adj_ma >> [Extract_Task_v7 , Validation_cs5_sdp_acc_adj_ma]



feedname_dpi_cdr = 'dpi_cdr'.upper()
feedname2_dpi_cdr = 'dpi_cdr'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_dpi_cdr = BashOperator(
    task_id='PCF_dpi_cdr',
    bash_command= pcf_command.format(d_1,feedname_dpi_cdr),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_dpi_cdr = BashOperator(
    task_id='Dedup_dpi_cdr',
    bash_command= dedup_command_dpi_cdr.format(d_1, ' dpi_cdr_00,dpi_cdr_01,dpi_cdr_02,dpi_cdr_03,dpi_cdr_04,dpi_cdr_05,dpi_cdr_06,dpi_cdr_07,dpi_cdr_08,dpi_cdr_09,dpi_cdr_10,dpi_cdr_11,dpi_cdr_12,dpi_cdr_13,dpi_cdr_14,dpi_cdr_15,dpi_cdr_16,dpi_cdr_17,dpi_cdr_18,dpi_cdr_19,dpi_cdr_20,dpi_cdr_21,dpi_cdr_22,dpi_cdr_23', 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_dpi_cdr = BashOperator(
    task_id='PCFCheck_dpi_cdr',
    bash_command=PCFCheck_command.format(feedname_dpi_cdr,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_dpi_cdr = BashOperator(
    task_id='Validation_dpi_cdr',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_dpi_cdr),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_dpi_cdr >> PCFCheck_dpi_cdr  >> Dedup_dpi_cdr >> [Extract_Task_v7 , Validation_dpi_cdr]



feedname_msc_cdr = 'msc_cdr'.upper()
feedname2_msc_cdr = 'msc_cdr'.lower()
ExtractGroup_Task_v7 = 'Task_v7'



PCF_msc_cdr = BashOperator(
    task_id='PCF_msc_cdr',
    bash_command= pcf_command.format(d_1,feedname_msc_cdr),
    dag=dag,
    queue='edge01002',	
    run_as_user='daasuser'
)

Dedup_msc_cdr = BashOperator(
    task_id='Dedup_msc_cdr',
    bash_command= dedup_command_2.format(d_1, feedname_msc_cdr, 'false'),
    dag=dag,
    run_as_user='daasuser'
)


PCFCheck_msc_cdr = BashOperator(
    task_id='PCFCheck_msc_cdr',
    bash_command=PCFCheck_command.format(feedname_msc_cdr,run_date,sleeptime),
    dag=dag,
	queue='edge01002',	
    run_as_user='daasuser'
)

Validation_msc_cdr = BashOperator(
    task_id='Validation_msc_cdr',
    bash_command = 'bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh {0} {1} {1}.conf  true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_{0}_{1}.txt  '.format(d_1,feedname_msc_cdr),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)
PCF_msc_cdr >> PCFCheck_msc_cdr  >> Dedup_msc_cdr >> [Extract_Task_v7 , Validation_msc_cdr]

