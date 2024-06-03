import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators import BashOperator,PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator

list_feed = 'BUNDLE4U_GPRS,BUNDLE4U_VOICE,CS5_AIR_ADJ_MA,CS5_AIR_REFILL_MA,CS5_CCN_GPRS_MA,CS5_CCN_SMS_MA,CS5_CCN_VOICE_MA,CS5_SDP_ACC_ADJ_MA,DPI_CDR,MSC_CDR'

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = yesterday.strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=2)
hour_run = hour_run_date.strftime('%H')

phases = ['Extract_','Dedup2_','PCF_','faile_']
working_dir='/mnt/beegfs_bsl/Deployment/DEV/scripts/BslDriver'

pathStatus = Variable.get("pathStatus", deserialize_json=True)
pathCheckExtract = Variable.get("pathCheckExtract", deserialize_json=True)
list_Container = Variable.get("list_Container", deserialize_json=True)
topic_kafka = Variable.get("topic_kafka", deserialize_json=True)
Email = Variable.get("Email", deserialize_json=True)
sleeptime = Variable.get("sleeptime", deserialize_json=True)
ValidationCode= Variable.get("ValidationBSLGroup", deserialize_json=True)
BSLCode= Variable.get("BSLCode", deserialize_json=True)
KafkaCheckcommand= Variable.get("KafkaCheckcommand", deserialize_json=True)
validationDumpcommand= Variable.get("validationDumpcommand", deserialize_json=True)
extract_command= Variable.get("extract_command", deserialize_json=True)
CheckExtract= Variable.get("CheckExtract", deserialize_json=True)
ContainerCheckcomands= Variable.get("ContainerCheckcomands", deserialize_json=True)
dedup_command= Variable.get("dedup_command", deserialize_json=True)
pcf_command= Variable.get("pcf_command", deserialize_json=True)
PCFCheck_command= Variable.get("PCFCheck_command", deserialize_json=True)
branchScript= Variable.get("branchScript", deserialize_json=True)
list_feed_ext = Variable.get("ExtractGroup1", deserialize_json=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,12,3),
    'email': ['m.abdin@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def task(i,feedname,ExtractGroup):
    result = phases[3]+ feedname
    if i == '0' :
        result = phases[0]+ ExtractGroup
    elif i == '1' :
        result = phases[1]+ feedname
    elif i == '2' :
        result = phases[2]+ feedname
    elif i == '3' :
        result = phases[3]+ feedname
    return result

def taskDumps(i,feedname,ExtractGroup):
    result = phases[3]+ feedname
    if i == '0' :
        result = ExtractGroup
    elif i == '1' :
        result = phases[1]+ feedname
    elif i == '2' :
        result = phases[2]+ feedname
    elif i == '3' :
        result = phases[3]+ feedname
    return result

def readcsv (path,feedname,ExtractGroup):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = task (line[0],feedname,ExtractGroup)
    return result
    
def readcsvdump (path,feedname,ExtractGroup):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = taskDumps (line[0],feedname,ExtractGroup)
    return result

dag = DAG('BSL_Group_2B', default_args=default_args, catchup=False, schedule_interval= None)

#ValidationCode = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh {0} all ValidationTool_BSL_ALL.conf true 2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log__Airflow_{1}.txt'
logging.info(ValidationCode)

#BSLCode = 'java -jar {0}/BslDriver-0.0.2.jar -cf {0}/ProdConfig.properties --group "BSL~VALIDATION|{1}" --bslOpt "startingPoint=1;endPoint=16;skipSteps=1;aggr=daily;isIntradayProcessing=true" -wd {0} --dataExtractionOpt "buckets=2100;feeds=all;ef=subretaindata_customersubject,subretaindata_not_sync,subretaindata"'.format(working_dir,d_1)
logging.info(BSLCode)

#KafkaCheckcommand = 'python /nas/share05/tools/DQ/airflow_tools/Kafka_Check.py. -t {0}'
logging.info(KafkaCheckcommand)

#validationDumpcommand = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh {0} all BSL_DUMPS.conf true 2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log__Airflow_{1}.txt'
logging.info(validationDumpcommand)

#extract_command = '/opt/alluxio-1.8.0-hadoop-2.9/bin/alluxio fs rm -R /bsl/FlareData/ExportedSubData/* ; spark-submit --driver-java-options "-Dalluxio.master.hostname=master01004.mtn.com" --queue bsl --verbose --conf spark.hadoop.fs.permissions.umask-mode=000 --conf "spark.ui.enabled=true" --master yarn --driver-memory 10G --executor-memory 10G --num-executors 80 --executor-cores 4 --class com.kamanja.ingest.Hive2HDFS /nas/share05/FlareProd/Run/Bsl/DataExtract/libs/dump-hive_2.10-0.0.3-SNAPSHOT.jar -d {0}-{0} -f all -ef subretaindata_customersubject,subretaindata_not_sync,subretaindata -t "alluxio://master01004.mtn.com:19998/bsl/FlareData/ExportedSubData" -c /nas/share05/FlareProd/Run/Bsl/DataExtract/conf/application_new.conf -l WARN -b 2100 -mwd /mnt/beegfs_bsl/extractHive/Email -mhn mailheader -msn sendEmail.sh -msa edge01002 -v -mv "file:///mnt/beegfs_bsl/FlareData/ExportedSubData" --status-path /nas/share05/FlareProd/Run/Bsl/DataExtract/ExportedDataStatus 2>&1 | tee /nas/share05/FlareProd/logs/Bsl/DataExtract/teelogs/Extract_Run_$(date +%Y%m%d_%s).txt'
logging.info(extract_command)

#CheckExtract = 'bash /nas/share05/tools/DQ/airflow_tools/CheckExtract.sh {0} {1}'
logging.info(CheckExtract)

#ContainerCheckcomands = 'pyhton /nas/share05/tools/DQ/airflow_tools/Hbase_Container.py -lc {0}'
logging.info(ContainerCheckcomands)

#dedup_command="bash /nas/share05/tools/DQ/airflow_tools/Dedup_part.sh {0} {1} {2}"

#pcf_command = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.5.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f {1} --logQueries --ignoreDotFilesCheck -p 1 --move"

#PCFCheck_command = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname {0} -date {1} -sleeptime {2}" 

#branchScript = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}"

#BSL_Check_command = "python /mnt/beegfs_bsl/tools/PY/Bsl_Check/scripts/Bsl_Check.py"

Validation = BashOperator(
task_id='Validation',
bash_command= ValidationCode.format(d_1,dayStr,list_feed),
trigger_rule='all_success',
run_as_user='daasuser',
priority_weight = 100,
dag=dag,
)


feedname_Group_2 = 'bundle4u_gprs,bundle4u_voice,cs5_air_adj_ma,cs5_air_refill_ma,cs5_ccn_gprs_ma,cs5_ccn_sms_ma,cs5_ccn_voice_ma,cs5_sdp_acc_adj_ma,dpi_cdr,msc_cdr'

Extract_Group_2 = BashOperator(
    task_id='Extract_Group_2',
    bash_command = extract_command.format(d_1,list_feed_ext),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

CheckExtractGroup_2 = BashOperator(
    task_id='CheckExtract_Group_2',
    bash_command = CheckExtract.format(d_1,pathCheckExtract),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

Extract_Group_2 >> CheckExtractGroup_2

feedname_bundle4u_gprs = 'bundle4u_gprs'.upper()
feedname2_bundle4u_gprs = 'bundle4u_gprs'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_bundle4u_gprs():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_bundle4u_gprs,feedname2_bundle4u_gprs)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_bundle4u_gprs,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_bundle4u_gprs, ExtractGroup_Group_2)
    logging.info('branch_bundle4u_gprs' + x)
    return x

Branching_bundle4u_gprs = BranchPythonOperator(
    task_id='branchid_bundle4u_gprs',
    python_callable=branch_bundle4u_gprs,
    dag=dag,
    run_as_user='daasuser'
)

PCF_bundle4u_gprs = BashOperator(
    task_id='PCF_bundle4u_gprs',
    bash_command= pcf_command.format(d_1,feedname_bundle4u_gprs),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_bundle4u_gprs = BashOperator(
    task_id='Dedup_bundle4u_gprs',
    bash_command= dedup_command.format(run_date, feedname_bundle4u_gprs, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_bundle4u_gprs = BashOperator(
    task_id='Dedup2_bundle4u_gprs',
    bash_command= dedup_command.format(run_date, feedname_bundle4u_gprs, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_bundle4u_gprs = BashOperator(
    task_id='PCFCheck_bundle4u_gprs',
    bash_command=PCFCheck_command.format(feedname_bundle4u_gprs,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_bundle4u_gprs = EmailOperator(
    task_id='faile_bundle4u_gprs',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_bundle4u_gprs),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_bundle4u_gprs >> PCF_bundle4u_gprs >> PCFCheck_bundle4u_gprs  >> Dedup_bundle4u_gprs >> Extract_Group_2
Validation >> Branching_bundle4u_gprs >> Extract_Group_2
Validation >> Branching_bundle4u_gprs >> Dedup2_bundle4u_gprs >> Extract_Group_2
Validation >> Branching_bundle4u_gprs >> faile_bundle4u_gprs


feedname_bundle4u_voice = 'bundle4u_voice'.upper()
feedname2_bundle4u_voice = 'bundle4u_voice'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_bundle4u_voice():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_bundle4u_voice,feedname2_bundle4u_voice)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_bundle4u_voice,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_bundle4u_voice, ExtractGroup_Group_2)
    logging.info('branch_bundle4u_voice' + x)
    return x

Branching_bundle4u_voice = BranchPythonOperator(
    task_id='branchid_bundle4u_voice',
    python_callable=branch_bundle4u_voice,
    dag=dag,
    run_as_user='daasuser'
)

PCF_bundle4u_voice = BashOperator(
    task_id='PCF_bundle4u_voice',
    bash_command= pcf_command.format(d_1,feedname_bundle4u_voice),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_bundle4u_voice = BashOperator(
    task_id='Dedup_bundle4u_voice',
    bash_command= dedup_command.format(run_date, feedname_bundle4u_voice, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_bundle4u_voice = BashOperator(
    task_id='Dedup2_bundle4u_voice',
    bash_command= dedup_command.format(run_date, feedname_bundle4u_voice, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_bundle4u_voice = BashOperator(
    task_id='PCFCheck_bundle4u_voice',
    bash_command=PCFCheck_command.format(feedname_bundle4u_voice,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_bundle4u_voice = EmailOperator(
    task_id='faile_bundle4u_voice',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_bundle4u_voice),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_bundle4u_voice >> PCF_bundle4u_voice >> PCFCheck_bundle4u_voice  >> Dedup_bundle4u_voice >> Extract_Group_2
Validation >> Branching_bundle4u_voice >> Extract_Group_2
Validation >> Branching_bundle4u_voice >> Dedup2_bundle4u_voice >> Extract_Group_2
Validation >> Branching_bundle4u_voice >> faile_bundle4u_voice


feedname_cs5_air_adj_ma = 'cs5_air_adj_ma'.upper()
feedname2_cs5_air_adj_ma = 'cs5_air_adj_ma'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_cs5_air_adj_ma():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_cs5_air_adj_ma,feedname2_cs5_air_adj_ma)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_air_adj_ma,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_air_adj_ma, ExtractGroup_Group_2)
    logging.info('branch_cs5_air_adj_ma' + x)
    return x

Branching_cs5_air_adj_ma = BranchPythonOperator(
    task_id='branchid_cs5_air_adj_ma',
    python_callable=branch_cs5_air_adj_ma,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_air_adj_ma = BashOperator(
    task_id='PCF_cs5_air_adj_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_adj_ma),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_air_adj_ma = BashOperator(
    task_id='Dedup_cs5_air_adj_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_adj_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_air_adj_ma = BashOperator(
    task_id='Dedup2_cs5_air_adj_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_adj_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_air_adj_ma = BashOperator(
    task_id='PCFCheck_cs5_air_adj_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_air_adj_ma,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_air_adj_ma = EmailOperator(
    task_id='faile_cs5_air_adj_ma',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_air_adj_ma),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_cs5_air_adj_ma >> PCF_cs5_air_adj_ma >> PCFCheck_cs5_air_adj_ma  >> Dedup_cs5_air_adj_ma >> Extract_Group_2
Validation >> Branching_cs5_air_adj_ma >> Extract_Group_2
Validation >> Branching_cs5_air_adj_ma >> Dedup2_cs5_air_adj_ma >> Extract_Group_2
Validation >> Branching_cs5_air_adj_ma >> faile_cs5_air_adj_ma


feedname_cs5_air_refill_ma = 'cs5_air_refill_ma'.upper()
feedname2_cs5_air_refill_ma = 'cs5_air_refill_ma'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_cs5_air_refill_ma():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_cs5_air_refill_ma,feedname2_cs5_air_refill_ma)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_air_refill_ma,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_air_refill_ma, ExtractGroup_Group_2)
    logging.info('branch_cs5_air_refill_ma' + x)
    return x

Branching_cs5_air_refill_ma = BranchPythonOperator(
    task_id='branchid_cs5_air_refill_ma',
    python_callable=branch_cs5_air_refill_ma,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_air_refill_ma = BashOperator(
    task_id='PCF_cs5_air_refill_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_refill_ma),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_air_refill_ma = BashOperator(
    task_id='Dedup_cs5_air_refill_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_refill_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_air_refill_ma = BashOperator(
    task_id='Dedup2_cs5_air_refill_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_refill_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_air_refill_ma = BashOperator(
    task_id='PCFCheck_cs5_air_refill_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_air_refill_ma,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_air_refill_ma = EmailOperator(
    task_id='faile_cs5_air_refill_ma',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_air_refill_ma),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_cs5_air_refill_ma >> PCF_cs5_air_refill_ma >> PCFCheck_cs5_air_refill_ma  >> Dedup_cs5_air_refill_ma >> Extract_Group_2
Validation >> Branching_cs5_air_refill_ma >> Extract_Group_2
Validation >> Branching_cs5_air_refill_ma >> Dedup2_cs5_air_refill_ma >> Extract_Group_2
Validation >> Branching_cs5_air_refill_ma >> faile_cs5_air_refill_ma


feedname_cs5_ccn_gprs_ma = 'cs5_ccn_gprs_ma'.upper()
feedname2_cs5_ccn_gprs_ma = 'cs5_ccn_gprs_ma'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_cs5_ccn_gprs_ma():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_cs5_ccn_gprs_ma,feedname2_cs5_ccn_gprs_ma)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_gprs_ma,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_gprs_ma, ExtractGroup_Group_2)
    logging.info('branch_cs5_ccn_gprs_ma' + x)
    return x

Branching_cs5_ccn_gprs_ma = BranchPythonOperator(
    task_id='branchid_cs5_ccn_gprs_ma',
    python_callable=branch_cs5_ccn_gprs_ma,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_gprs_ma = BashOperator(
    task_id='PCF_cs5_ccn_gprs_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_gprs_ma),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_gprs_ma = BashOperator(
    task_id='Dedup_cs5_ccn_gprs_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_gprs_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_gprs_ma = BashOperator(
    task_id='Dedup2_cs5_ccn_gprs_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_gprs_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_gprs_ma = BashOperator(
    task_id='PCFCheck_cs5_ccn_gprs_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_gprs_ma,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_gprs_ma = EmailOperator(
    task_id='faile_cs5_ccn_gprs_ma',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_gprs_ma),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_cs5_ccn_gprs_ma >> PCF_cs5_ccn_gprs_ma >> PCFCheck_cs5_ccn_gprs_ma  >> Dedup_cs5_ccn_gprs_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_gprs_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_gprs_ma >> Dedup2_cs5_ccn_gprs_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_gprs_ma >> faile_cs5_ccn_gprs_ma


feedname_cs5_ccn_sms_ma = 'cs5_ccn_sms_ma'.upper()
feedname2_cs5_ccn_sms_ma = 'cs5_ccn_sms_ma'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_cs5_ccn_sms_ma():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_cs5_ccn_sms_ma,feedname2_cs5_ccn_sms_ma)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_sms_ma,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_sms_ma, ExtractGroup_Group_2)
    logging.info('branch_cs5_ccn_sms_ma' + x)
    return x

Branching_cs5_ccn_sms_ma = BranchPythonOperator(
    task_id='branchid_cs5_ccn_sms_ma',
    python_callable=branch_cs5_ccn_sms_ma,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_sms_ma = BashOperator(
    task_id='PCF_cs5_ccn_sms_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_sms_ma),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_sms_ma = BashOperator(
    task_id='Dedup_cs5_ccn_sms_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_sms_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_sms_ma = BashOperator(
    task_id='Dedup2_cs5_ccn_sms_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_sms_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_sms_ma = BashOperator(
    task_id='PCFCheck_cs5_ccn_sms_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_sms_ma,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_sms_ma = EmailOperator(
    task_id='faile_cs5_ccn_sms_ma',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_sms_ma),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_cs5_ccn_sms_ma >> PCF_cs5_ccn_sms_ma >> PCFCheck_cs5_ccn_sms_ma  >> Dedup_cs5_ccn_sms_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_sms_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_sms_ma >> Dedup2_cs5_ccn_sms_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_sms_ma >> faile_cs5_ccn_sms_ma


feedname_cs5_ccn_voice_ma = 'cs5_ccn_voice_ma'.upper()
feedname2_cs5_ccn_voice_ma = 'cs5_ccn_voice_ma'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_cs5_ccn_voice_ma():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_cs5_ccn_voice_ma,feedname2_cs5_ccn_voice_ma)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_voice_ma,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_voice_ma, ExtractGroup_Group_2)
    logging.info('branch_cs5_ccn_voice_ma' + x)
    return x

Branching_cs5_ccn_voice_ma = BranchPythonOperator(
    task_id='branchid_cs5_ccn_voice_ma',
    python_callable=branch_cs5_ccn_voice_ma,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_voice_ma = BashOperator(
    task_id='PCF_cs5_ccn_voice_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_voice_ma),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_voice_ma = BashOperator(
    task_id='Dedup_cs5_ccn_voice_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_voice_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_voice_ma = BashOperator(
    task_id='Dedup2_cs5_ccn_voice_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_voice_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_voice_ma = BashOperator(
    task_id='PCFCheck_cs5_ccn_voice_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_voice_ma,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_voice_ma = EmailOperator(
    task_id='faile_cs5_ccn_voice_ma',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_voice_ma),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_cs5_ccn_voice_ma >> PCF_cs5_ccn_voice_ma >> PCFCheck_cs5_ccn_voice_ma  >> Dedup_cs5_ccn_voice_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_voice_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_voice_ma >> Dedup2_cs5_ccn_voice_ma >> Extract_Group_2
Validation >> Branching_cs5_ccn_voice_ma >> faile_cs5_ccn_voice_ma


feedname_cs5_sdp_acc_adj_ma = 'cs5_sdp_acc_adj_ma'.upper()
feedname2_cs5_sdp_acc_adj_ma = 'cs5_sdp_acc_adj_ma'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_cs5_sdp_acc_adj_ma():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_cs5_sdp_acc_adj_ma,feedname2_cs5_sdp_acc_adj_ma)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_sdp_acc_adj_ma,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_sdp_acc_adj_ma, ExtractGroup_Group_2)
    logging.info('branch_cs5_sdp_acc_adj_ma' + x)
    return x

Branching_cs5_sdp_acc_adj_ma = BranchPythonOperator(
    task_id='branchid_cs5_sdp_acc_adj_ma',
    python_callable=branch_cs5_sdp_acc_adj_ma,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='PCF_cs5_sdp_acc_adj_ma',
    bash_command= pcf_command.format(d_1,feedname_cs5_sdp_acc_adj_ma),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='Dedup_cs5_sdp_acc_adj_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_sdp_acc_adj_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='Dedup2_cs5_sdp_acc_adj_ma',
    bash_command= dedup_command.format(run_date, feedname_cs5_sdp_acc_adj_ma, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_sdp_acc_adj_ma = BashOperator(
    task_id='PCFCheck_cs5_sdp_acc_adj_ma',
    bash_command=PCFCheck_command.format(feedname_cs5_sdp_acc_adj_ma,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_sdp_acc_adj_ma = EmailOperator(
    task_id='faile_cs5_sdp_acc_adj_ma',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_sdp_acc_adj_ma),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_cs5_sdp_acc_adj_ma >> PCF_cs5_sdp_acc_adj_ma >> PCFCheck_cs5_sdp_acc_adj_ma  >> Dedup_cs5_sdp_acc_adj_ma >> Extract_Group_2
Validation >> Branching_cs5_sdp_acc_adj_ma >> Extract_Group_2
Validation >> Branching_cs5_sdp_acc_adj_ma >> Dedup2_cs5_sdp_acc_adj_ma >> Extract_Group_2
Validation >> Branching_cs5_sdp_acc_adj_ma >> faile_cs5_sdp_acc_adj_ma


feedname_dpi_cdr = 'dpi_cdr'.upper()
feedname2_dpi_cdr = 'dpi_cdr'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_dpi_cdr():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_dpi_cdr,feedname2_dpi_cdr)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_dpi_cdr,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_dpi_cdr, ExtractGroup_Group_2)
    logging.info('branch_dpi_cdr' + x)
    return x

Branching_dpi_cdr = BranchPythonOperator(
    task_id='branchid_dpi_cdr',
    python_callable=branch_dpi_cdr,
    dag=dag,
    run_as_user='daasuser'
)

PCF_dpi_cdr = BashOperator(
    task_id='PCF_dpi_cdr',
    bash_command= pcf_command.format(d_1,feedname_dpi_cdr),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_dpi_cdr = BashOperator(
    task_id='Dedup_dpi_cdr',
    bash_command= dedup_command.format(run_date, feedname_dpi_cdr, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_dpi_cdr = BashOperator(
    task_id='Dedup2_dpi_cdr',
    bash_command= dedup_command.format(run_date, feedname_dpi_cdr, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_dpi_cdr = BashOperator(
    task_id='PCFCheck_dpi_cdr',
    bash_command=PCFCheck_command.format(feedname_dpi_cdr,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_dpi_cdr = EmailOperator(
    task_id='faile_dpi_cdr',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_dpi_cdr),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_dpi_cdr >> PCF_dpi_cdr >> PCFCheck_dpi_cdr  >> Dedup_dpi_cdr >> Extract_Group_2
Validation >> Branching_dpi_cdr >> Extract_Group_2
Validation >> Branching_dpi_cdr >> Dedup2_dpi_cdr >> Extract_Group_2
Validation >> Branching_dpi_cdr >> faile_dpi_cdr


feedname_msc_cdr = 'msc_cdr'.upper()
feedname2_msc_cdr = 'msc_cdr'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_msc_cdr():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_msc_cdr,feedname2_msc_cdr)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_msc_cdr,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_msc_cdr, ExtractGroup_Group_2)
    logging.info('branch_msc_cdr' + x)
    return x

Branching_msc_cdr = BranchPythonOperator(
    task_id='branchid_msc_cdr',
    python_callable=branch_msc_cdr,
    dag=dag,
    run_as_user='daasuser'
)

PCF_msc_cdr = BashOperator(
    task_id='PCF_msc_cdr',
    bash_command= pcf_command.format(d_1,feedname_msc_cdr),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_msc_cdr = BashOperator(
    task_id='Dedup_msc_cdr',
    bash_command= dedup_command.format(run_date, feedname_msc_cdr, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_msc_cdr = BashOperator(
    task_id='Dedup2_msc_cdr',
    bash_command= dedup_command.format(run_date, feedname_msc_cdr, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_msc_cdr = BashOperator(
    task_id='PCFCheck_msc_cdr',
    bash_command=PCFCheck_command.format(feedname_msc_cdr,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_msc_cdr = EmailOperator(
    task_id='faile_msc_cdr',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_msc_cdr),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_msc_cdr >> PCF_msc_cdr >> PCFCheck_msc_cdr  >> Dedup_msc_cdr >> Extract_Group_2
Validation >> Branching_msc_cdr >> Extract_Group_2
Validation >> Branching_msc_cdr >> Dedup2_msc_cdr >> Extract_Group_2
Validation >> Branching_msc_cdr >> faile_msc_cdr
