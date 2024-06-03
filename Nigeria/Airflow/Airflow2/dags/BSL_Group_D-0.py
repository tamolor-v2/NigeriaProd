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
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator

list_feed = 'UDC_DUMP,DMC_DUMP_ALL,MVAS_DND_MSISDN_REPORT'

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = yesterday.strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=4)
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
    'owner': 'BSL',
    'depends_on_past':False,
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

dag = DAG('BSL_Group_D-0', default_args=default_args, catchup=False, schedule_interval= "0 17 * * *")

#ValidationCode = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh {0} all ValidationTool_BSL_ALL.conf true 2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log__Airflow_{1}.txt'
logging.info(ValidationCode)

#BSLCode = 'java -Dlog4j2.formatMsgNoLookups=true -jar {0}/BslDriver-0.0.2.jar -cf {0}/ProdConfig.properties --group "BSL~VALIDATION|{1}" --bslOpt "startingPoint=1;endPoint=16;skipSteps=1;aggr=daily;isIntradayProcessing=true" -wd {0} --dataExtractionOpt "buckets=2100;feeds=all;ef=subretaindata_customersubject,subretaindata_not_sync,subretaindata"'.format(working_dir,d_1)
logging.info(BSLCode)

#KafkaCheckcommand = 'python /nas/share05/tools/DQ/airflow_tools/Kafka_Check.py. -t {0}'
logging.info(KafkaCheckcommand)

#validationDumpcommand = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh {0} all BSL_DUMPS.conf true 2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log__Airflow_{1}.txt'
logging.info(validationDumpcommand)

#extract_command = '/opt/alluxio-1.8.0-hadoop-2.9/bin/alluxio fs rm -R /bsl/FlareData/ExportedSubData/* ; spark-submit --driver-java -Dlog4j2.formatMsgNoLookups=true-options "-Dalluxio.master.hostname=master01004.mtn.com" --queue bsl --verbose --conf spark.hadoop.fs.permissions.umask-mode=000 --conf "spark.ui.enabled=true" --master yarn --driver-memory 10G --executor-memory 10G --num-executors 80 --executor-cores 4 --class com.kamanja.ingest.Hive2HDFS /nas/share05/FlareProd/Run/Bsl/DataExtract/libs/dump-hive_2.10-0.0.3-SNAPSHOT.jar -d {0}-{0} -f all -ef subretaindata_customersubject,subretaindata_not_sync,subretaindata -t "alluxio://master01004.mtn.com:19998/bsl/FlareData/ExportedSubData" -c /nas/share05/FlareProd/Run/Bsl/DataExtract/conf/application_new.conf -l WARN -b 2100 -mwd /mnt/beegfs_bsl/extractHive/Email -mhn mailheader -msn sendEmail.sh -msa edge01002 -v -mv "file:///mnt/beegfs_bsl/FlareData/ExportedSubData" --status-path /nas/share05/FlareProd/Run/Bsl/DataExtract/ExportedDataStatus 2>&1 | tee /nas/share05/FlareProd/logs/Bsl/DataExtract/teelogs/Extract_Run_$(date +%Y%m%d_%s).txt'
logging.info(extract_command)

#CheckExtract = 'bash /nas/share05/tools/DQ/airflow_tools/CheckExtract.sh {0} {1}'
logging.info(CheckExtract)

#ContainerCheckcomands = 'pyhton /nas/share05/tools/DQ/airflow_tools/Hbase_Container.py -lc {0}'
logging.info(ContainerCheckcomands)

#dedup_command="bash /nas/share05/tools/DQ/airflow_tools/Dedup_part.sh {0} {1} {2}"

#pcf_command = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.5.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f {1} --logQueries --ignoreDotFilesCheck -p 1 --move"

#PCFCheck_command = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname {0} -date {1} -sleeptime {2}" 

#branchScript = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}"

#BSL_Check_command = "python /mnt/beegfs_bsl/tools/PY/Bsl_Check/scripts/Bsl_Check.py"

Validation = BashOperator(
task_id='Validation',
bash_command= ValidationCode.format(run_date,dayStr,list_feed),
trigger_rule='all_success',
run_as_user='daasuser',
priority_weight = 100,
dag=dag,
)


feedname_Group_1 = 'udc_dump,dmc_dump_all,mvas_dnd_msisdn_report'

Extract_Group_1 = BashOperator(
    task_id='Extract_Group_1',
    bash_command = extract_command.format(run_date,list_feed_ext),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

CheckExtractGroup_1 = BashOperator(
    task_id='CheckExtract_Group_1',
    bash_command = CheckExtract.format(run_date,pathCheckExtract),
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

Extract_Group_1 >> CheckExtractGroup_1

feedname_udc_dump = 'udc_dump'.upper()
feedname2_udc_dump = 'udc_dump'.lower()
ExtractGroup_Group_1 = 'Group_1'


def branch_udc_dump():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_udc_dump,feedname2_udc_dump)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_udc_dump,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_udc_dump, ExtractGroup_Group_1)
    logging.info('branch_udc_dump' + x)
    return x

Branching_udc_dump = BranchPythonOperator(
    task_id='branchid_udc_dump',
    python_callable=branch_udc_dump,
    dag=dag,
    run_as_user='daasuser'
)

PCF_udc_dump = BashOperator(
    task_id='PCF_udc_dump',
    bash_command= pcf_command.format(run_date,feedname_udc_dump),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_udc_dump = BashOperator(
    task_id='Dedup_udc_dump',
    bash_command= dedup_command.format(run_date, feedname_udc_dump, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_udc_dump = BashOperator(
    task_id='Dedup2_udc_dump',
    bash_command= dedup_command.format(run_date, feedname_udc_dump, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_udc_dump = BashOperator(
    task_id='PCFCheck_udc_dump',
    bash_command=PCFCheck_command.format(feedname_udc_dump,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_udc_dump = EmailOperator(
    task_id='faile_udc_dump',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_udc_dump),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_udc_dump >> PCF_udc_dump >> PCFCheck_udc_dump  >> Dedup_udc_dump >> Extract_Group_1
Validation >> Branching_udc_dump >> Extract_Group_1
Validation >> Branching_udc_dump >> Dedup2_udc_dump >> Extract_Group_1
Validation >> Branching_udc_dump >> faile_udc_dump


feedname_dmc_dump_all = 'dmc_dump_all'.upper()
feedname2_dmc_dump_all = 'dmc_dump_all'.lower()
ExtractGroup_Group_1 = 'Group_1'


def branch_dmc_dump_all():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_dmc_dump_all,feedname2_dmc_dump_all)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_dmc_dump_all,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_dmc_dump_all, ExtractGroup_Group_1)
    logging.info('branch_dmc_dump_all' + x)
    return x

Branching_dmc_dump_all = BranchPythonOperator(
    task_id='branchid_dmc_dump_all',
    python_callable=branch_dmc_dump_all,
    dag=dag,
    run_as_user='daasuser'
)

PCF_dmc_dump_all = BashOperator(
    task_id='PCF_dmc_dump_all',
    bash_command= pcf_command.format(run_date,feedname_dmc_dump_all),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_dmc_dump_all = BashOperator(
    task_id='Dedup_dmc_dump_all',
    bash_command= dedup_command.format(run_date, feedname_dmc_dump_all, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_dmc_dump_all = BashOperator(
    task_id='Dedup2_dmc_dump_all',
    bash_command= dedup_command.format(run_date, feedname_dmc_dump_all, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_dmc_dump_all = BashOperator(
    task_id='PCFCheck_dmc_dump_all',
    bash_command=PCFCheck_command.format(feedname_dmc_dump_all,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_dmc_dump_all = EmailOperator(
    task_id='faile_dmc_dump_all',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_dmc_dump_all),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_dmc_dump_all >> PCF_dmc_dump_all >> PCFCheck_dmc_dump_all  >> Dedup_dmc_dump_all >> Extract_Group_1
Validation >> Branching_dmc_dump_all >> Extract_Group_1
Validation >> Branching_dmc_dump_all >> Dedup2_dmc_dump_all >> Extract_Group_1
Validation >> Branching_dmc_dump_all >> faile_dmc_dump_all


feedname_mvas_dnd_msisdn_report = 'mvas_dnd_msisdn_report'.upper()
feedname2_mvas_dnd_msisdn_report = 'mvas_dnd_msisdn_report'.lower()
ExtractGroup_Group_1 = 'Group_1'


def branch_mvas_dnd_msisdn_report():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,run_date,feedname2_mvas_dnd_msisdn_report,feedname2_mvas_dnd_msisdn_report)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_mvas_dnd_msisdn_report,run_date,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_mvas_dnd_msisdn_report, ExtractGroup_Group_1)
    logging.info('branch_mvas_dnd_msisdn_report' + x)
    return x

Branching_mvas_dnd_msisdn_report = BranchPythonOperator(
    task_id='branchid_mvas_dnd_msisdn_report',
    python_callable=branch_mvas_dnd_msisdn_report,
    dag=dag,
    run_as_user='daasuser'
)

PCF_mvas_dnd_msisdn_report = BashOperator(
    task_id='PCF_mvas_dnd_msisdn_report',
    bash_command= pcf_command.format(run_date,feedname_mvas_dnd_msisdn_report),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_mvas_dnd_msisdn_report = BashOperator(
    task_id='Dedup_mvas_dnd_msisdn_report',
    bash_command= dedup_command.format(run_date, feedname_mvas_dnd_msisdn_report, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_mvas_dnd_msisdn_report = BashOperator(
    task_id='Dedup2_mvas_dnd_msisdn_report',
    bash_command= dedup_command.format(run_date, feedname_mvas_dnd_msisdn_report, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_mvas_dnd_msisdn_report = BashOperator(
    task_id='PCFCheck_mvas_dnd_msisdn_report',
    bash_command=PCFCheck_command.format(feedname_mvas_dnd_msisdn_report,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_mvas_dnd_msisdn_report = EmailOperator(
    task_id='faile_mvas_dnd_msisdn_report',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_mvas_dnd_msisdn_report),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_mvas_dnd_msisdn_report >> PCF_mvas_dnd_msisdn_report >> PCFCheck_mvas_dnd_msisdn_report  >> Dedup_mvas_dnd_msisdn_report >> Extract_Group_1
Validation >> Branching_mvas_dnd_msisdn_report >> Extract_Group_1
Validation >> Branching_mvas_dnd_msisdn_report >> Dedup2_mvas_dnd_msisdn_report >> Extract_Group_1
Validation >> Branching_mvas_dnd_msisdn_report >> faile_mvas_dnd_msisdn_report
