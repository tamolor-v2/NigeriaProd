import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = Variable.get("rerunDate6", deserialize_json=True)
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=2)
hour_run = hour_run_date.strftime('%H')
sleeptime= 2*1000*60

pathcsv = '/nas/share05/tools/DQ2/status2/'
phases = ['Success','Dedup2_','PCF_','faile_']
email = ['y.bloukh@ligadata.com','support@ligadata.com','k.musallam@ligadata.com','t.olorunfemi@ligadata.com','bmustafa@ligadata.com']


default_args = {
    'owner': 'daasuser',
    'depends_on_past':False,
    'start_date': datetime(2019,12,8),
    'email': ['y.bloukh@ligadata.com','support@ligadata.com','k.musallam@ligadata.com','t.olorunfemi@ligadata.com','bmustafa@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def task(i,feedname):
    result = phases[3]+ feedname
    if i == '0' :
        result = phases[0]
    elif i == '1' :
        result = phases[1]+ feedname
    elif i == '2' :
        result = phases[2]+ feedname
    elif i == '3' :
        result = phases[3]+ feedname
    return result


def readcsv (path,feedname):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = task (line[0],feedname)
    return result

dag = DAG('New_Automation_Group_6', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group6 -c config.json' %(d_1)
logging.info(ValidationCode)


Validation = BashOperator(
task_id='Validation',
bash_command= ValidationCode,
trigger_rule='all_success',
run_as_user='daasuser',
dag=dag,
priority_weight=100
)

Success = EmailOperator(
    task_id='Success',
    to=email,
    subject='Airflow Success for feed',
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation_End = BashOperator(
task_id='Validation_End',
bash_command= ValidationCode,
trigger_rule='all_success',
run_as_user='daasuser',
dag=dag,
priority_weight=100
)
Validation_End >> Success


feedname_MTN_ONLINE = 'MTN_ONLINE'.upper()
feedname2_MTN_ONLINE = 'MTN_ONLINE'.lower()

dedup_command_MTN_ONLINE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MTN_ONLINE)
logging.info(dedup_command_MTN_ONLINE) 

pcf_command_MTN_ONLINE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MTN_ONLINE)
logging.info(pcf_command_MTN_ONLINE)

PCFCheck_command_MTN_ONLINE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MTN_ONLINE,run_date,sleeptime)
logging.info(PCFCheck_command_MTN_ONLINE)

branchScript_MTN_ONLINE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MTN_ONLINE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MTN_ONLINE)

def branch_MTN_ONLINE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MTN_ONLINE,feedname2_MTN_ONLINE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MTN_ONLINE)
    x = readcsv(filepath,feedname2_MTN_ONLINE)
    logging.info('branch_MTN_ONLINE' + x)
    return x

Branching_MTN_ONLINE = BranchPythonOperator(
    task_id='branchid_MTN_ONLINE',
    python_callable=branch_MTN_ONLINE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MTN_ONLINE = BashOperator(
    task_id='PCF_MTN_ONLINE',
    bash_command= pcf_command_MTN_ONLINE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MTN_ONLINE = BashOperator(
    task_id='Dedup_MTN_ONLINE',
    bash_command= dedup_command_MTN_ONLINE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MTN_ONLINE = BashOperator(
    task_id='Dedup2_MTN_ONLINE',
    bash_command= dedup_command_MTN_ONLINE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MTN_ONLINE = BashOperator(
    task_id='PCFCheck_MTN_ONLINE',
    bash_command=PCFCheck_command_MTN_ONLINE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MTN_ONLINE = EmailOperator(
    task_id='faile_MTN_ONLINE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MTN_ONLINE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MTN_ONLINE: PythonOperator = PythonOperator(task_id="waitForFlush_MTN_ONLINE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MTN_ONLINE >> PCF_MTN_ONLINE >> PCFCheck_MTN_ONLINE >> delay_python_task_MTN_ONLINE >> Dedup_MTN_ONLINE >> Validation_End 
Branching_MTN_ONLINE >> Dedup2_MTN_ONLINE >> Validation_End
Branching_MTN_ONLINE >> faile_MTN_ONLINE
Branching_MTN_ONLINE >> Validation_End


feedname_BEEP_CALL_SERVICE = 'BEEP_CALL_SERVICE'.upper()
feedname2_BEEP_CALL_SERVICE = 'BEEP_CALL_SERVICE'.lower()

dedup_command_BEEP_CALL_SERVICE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_BEEP_CALL_SERVICE)
logging.info(dedup_command_BEEP_CALL_SERVICE) 

pcf_command_BEEP_CALL_SERVICE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_BEEP_CALL_SERVICE)
logging.info(pcf_command_BEEP_CALL_SERVICE)

PCFCheck_command_BEEP_CALL_SERVICE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BEEP_CALL_SERVICE,run_date,sleeptime)
logging.info(PCFCheck_command_BEEP_CALL_SERVICE)

branchScript_BEEP_CALL_SERVICE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BEEP_CALL_SERVICE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BEEP_CALL_SERVICE)

def branch_BEEP_CALL_SERVICE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BEEP_CALL_SERVICE,feedname2_BEEP_CALL_SERVICE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BEEP_CALL_SERVICE)
    x = readcsv(filepath,feedname2_BEEP_CALL_SERVICE)
    logging.info('branch_BEEP_CALL_SERVICE' + x)
    return x

Branching_BEEP_CALL_SERVICE = BranchPythonOperator(
    task_id='branchid_BEEP_CALL_SERVICE',
    python_callable=branch_BEEP_CALL_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BEEP_CALL_SERVICE = BashOperator(
    task_id='PCF_BEEP_CALL_SERVICE',
    bash_command= pcf_command_BEEP_CALL_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_BEEP_CALL_SERVICE = BashOperator(
    task_id='Dedup_BEEP_CALL_SERVICE',
    bash_command= dedup_command_BEEP_CALL_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_BEEP_CALL_SERVICE = BashOperator(
    task_id='Dedup2_BEEP_CALL_SERVICE',
    bash_command= dedup_command_BEEP_CALL_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_BEEP_CALL_SERVICE = BashOperator(
    task_id='PCFCheck_BEEP_CALL_SERVICE',
    bash_command=PCFCheck_command_BEEP_CALL_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_BEEP_CALL_SERVICE = EmailOperator(
    task_id='faile_BEEP_CALL_SERVICE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BEEP_CALL_SERVICE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BEEP_CALL_SERVICE: PythonOperator = PythonOperator(task_id="waitForFlush_BEEP_CALL_SERVICE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BEEP_CALL_SERVICE >> PCF_BEEP_CALL_SERVICE >> PCFCheck_BEEP_CALL_SERVICE >> delay_python_task_BEEP_CALL_SERVICE >> Dedup_BEEP_CALL_SERVICE >> Validation_End 
Branching_BEEP_CALL_SERVICE >> Dedup2_BEEP_CALL_SERVICE >> Validation_End
Branching_BEEP_CALL_SERVICE >> faile_BEEP_CALL_SERVICE
Branching_BEEP_CALL_SERVICE >> Validation_End


feedname_IMEI_UPLOAD_DETAILS = 'IMEI_UPLOAD_DETAILS'.upper()
feedname2_IMEI_UPLOAD_DETAILS = 'IMEI_UPLOAD_DETAILS'.lower()

dedup_command_IMEI_UPLOAD_DETAILS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_IMEI_UPLOAD_DETAILS)
logging.info(dedup_command_IMEI_UPLOAD_DETAILS) 

pcf_command_IMEI_UPLOAD_DETAILS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_IMEI_UPLOAD_DETAILS)
logging.info(pcf_command_IMEI_UPLOAD_DETAILS)

PCFCheck_command_IMEI_UPLOAD_DETAILS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_IMEI_UPLOAD_DETAILS,run_date,sleeptime)
logging.info(PCFCheck_command_IMEI_UPLOAD_DETAILS)

branchScript_IMEI_UPLOAD_DETAILS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_IMEI_UPLOAD_DETAILS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_IMEI_UPLOAD_DETAILS)

def branch_IMEI_UPLOAD_DETAILS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_IMEI_UPLOAD_DETAILS,feedname2_IMEI_UPLOAD_DETAILS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_IMEI_UPLOAD_DETAILS)
    x = readcsv(filepath,feedname2_IMEI_UPLOAD_DETAILS)
    logging.info('branch_IMEI_UPLOAD_DETAILS' + x)
    return x

Branching_IMEI_UPLOAD_DETAILS = BranchPythonOperator(
    task_id='branchid_IMEI_UPLOAD_DETAILS',
    python_callable=branch_IMEI_UPLOAD_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_IMEI_UPLOAD_DETAILS = BashOperator(
    task_id='PCF_IMEI_UPLOAD_DETAILS',
    bash_command= pcf_command_IMEI_UPLOAD_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_IMEI_UPLOAD_DETAILS = BashOperator(
    task_id='Dedup_IMEI_UPLOAD_DETAILS',
    bash_command= dedup_command_IMEI_UPLOAD_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_IMEI_UPLOAD_DETAILS = BashOperator(
    task_id='Dedup2_IMEI_UPLOAD_DETAILS',
    bash_command= dedup_command_IMEI_UPLOAD_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_IMEI_UPLOAD_DETAILS = BashOperator(
    task_id='PCFCheck_IMEI_UPLOAD_DETAILS',
    bash_command=PCFCheck_command_IMEI_UPLOAD_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_IMEI_UPLOAD_DETAILS = EmailOperator(
    task_id='faile_IMEI_UPLOAD_DETAILS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_IMEI_UPLOAD_DETAILS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_IMEI_UPLOAD_DETAILS: PythonOperator = PythonOperator(task_id="waitForFlush_IMEI_UPLOAD_DETAILS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_IMEI_UPLOAD_DETAILS >> PCF_IMEI_UPLOAD_DETAILS >> PCFCheck_IMEI_UPLOAD_DETAILS >> delay_python_task_IMEI_UPLOAD_DETAILS >> Dedup_IMEI_UPLOAD_DETAILS >> Validation_End 
Branching_IMEI_UPLOAD_DETAILS >> Dedup2_IMEI_UPLOAD_DETAILS >> Validation_End
Branching_IMEI_UPLOAD_DETAILS >> faile_IMEI_UPLOAD_DETAILS
Branching_IMEI_UPLOAD_DETAILS >> Validation_End


feedname_BIB_AGL_FXL_STS_REPORT = 'BIB_AGL_FXL_STS_REPORT'.upper()
feedname2_BIB_AGL_FXL_STS_REPORT = 'BIB_AGL_FXL_STS_REPORT'.lower()

dedup_command_BIB_AGL_FXL_STS_REPORT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_BIB_AGL_FXL_STS_REPORT)
logging.info(dedup_command_BIB_AGL_FXL_STS_REPORT) 

pcf_command_BIB_AGL_FXL_STS_REPORT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_BIB_AGL_FXL_STS_REPORT)
logging.info(pcf_command_BIB_AGL_FXL_STS_REPORT)

PCFCheck_command_BIB_AGL_FXL_STS_REPORT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BIB_AGL_FXL_STS_REPORT,run_date,sleeptime)
logging.info(PCFCheck_command_BIB_AGL_FXL_STS_REPORT)

branchScript_BIB_AGL_FXL_STS_REPORT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BIB_AGL_FXL_STS_REPORT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BIB_AGL_FXL_STS_REPORT)

def branch_BIB_AGL_FXL_STS_REPORT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BIB_AGL_FXL_STS_REPORT,feedname2_BIB_AGL_FXL_STS_REPORT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BIB_AGL_FXL_STS_REPORT)
    x = readcsv(filepath,feedname2_BIB_AGL_FXL_STS_REPORT)
    logging.info('branch_BIB_AGL_FXL_STS_REPORT' + x)
    return x

Branching_BIB_AGL_FXL_STS_REPORT = BranchPythonOperator(
    task_id='branchid_BIB_AGL_FXL_STS_REPORT',
    python_callable=branch_BIB_AGL_FXL_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BIB_AGL_FXL_STS_REPORT = BashOperator(
    task_id='PCF_BIB_AGL_FXL_STS_REPORT',
    bash_command= pcf_command_BIB_AGL_FXL_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_BIB_AGL_FXL_STS_REPORT = BashOperator(
    task_id='Dedup_BIB_AGL_FXL_STS_REPORT',
    bash_command= dedup_command_BIB_AGL_FXL_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_BIB_AGL_FXL_STS_REPORT = BashOperator(
    task_id='Dedup2_BIB_AGL_FXL_STS_REPORT',
    bash_command= dedup_command_BIB_AGL_FXL_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_BIB_AGL_FXL_STS_REPORT = BashOperator(
    task_id='PCFCheck_BIB_AGL_FXL_STS_REPORT',
    bash_command=PCFCheck_command_BIB_AGL_FXL_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_BIB_AGL_FXL_STS_REPORT = EmailOperator(
    task_id='faile_BIB_AGL_FXL_STS_REPORT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BIB_AGL_FXL_STS_REPORT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BIB_AGL_FXL_STS_REPORT: PythonOperator = PythonOperator(task_id="waitForFlush_BIB_AGL_FXL_STS_REPORT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BIB_AGL_FXL_STS_REPORT >> PCF_BIB_AGL_FXL_STS_REPORT >> PCFCheck_BIB_AGL_FXL_STS_REPORT >> delay_python_task_BIB_AGL_FXL_STS_REPORT >> Dedup_BIB_AGL_FXL_STS_REPORT >> Validation_End 
Branching_BIB_AGL_FXL_STS_REPORT >> Dedup2_BIB_AGL_FXL_STS_REPORT >> Validation_End
Branching_BIB_AGL_FXL_STS_REPORT >> faile_BIB_AGL_FXL_STS_REPORT
Branching_BIB_AGL_FXL_STS_REPORT >> Validation_End


feedname_BIB_AGL_GSM_STS_REPORT = 'BIB_AGL_GSM_STS_REPORT'.upper()
feedname2_BIB_AGL_GSM_STS_REPORT = 'BIB_AGL_GSM_STS_REPORT'.lower()

dedup_command_BIB_AGL_GSM_STS_REPORT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_BIB_AGL_GSM_STS_REPORT)
logging.info(dedup_command_BIB_AGL_GSM_STS_REPORT) 

pcf_command_BIB_AGL_GSM_STS_REPORT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_BIB_AGL_GSM_STS_REPORT)
logging.info(pcf_command_BIB_AGL_GSM_STS_REPORT)

PCFCheck_command_BIB_AGL_GSM_STS_REPORT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BIB_AGL_GSM_STS_REPORT,run_date,sleeptime)
logging.info(PCFCheck_command_BIB_AGL_GSM_STS_REPORT)

branchScript_BIB_AGL_GSM_STS_REPORT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BIB_AGL_GSM_STS_REPORT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BIB_AGL_GSM_STS_REPORT)

def branch_BIB_AGL_GSM_STS_REPORT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BIB_AGL_GSM_STS_REPORT,feedname2_BIB_AGL_GSM_STS_REPORT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BIB_AGL_GSM_STS_REPORT)
    x = readcsv(filepath,feedname2_BIB_AGL_GSM_STS_REPORT)
    logging.info('branch_BIB_AGL_GSM_STS_REPORT' + x)
    return x

Branching_BIB_AGL_GSM_STS_REPORT = BranchPythonOperator(
    task_id='branchid_BIB_AGL_GSM_STS_REPORT',
    python_callable=branch_BIB_AGL_GSM_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BIB_AGL_GSM_STS_REPORT = BashOperator(
    task_id='PCF_BIB_AGL_GSM_STS_REPORT',
    bash_command= pcf_command_BIB_AGL_GSM_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_BIB_AGL_GSM_STS_REPORT = BashOperator(
    task_id='Dedup_BIB_AGL_GSM_STS_REPORT',
    bash_command= dedup_command_BIB_AGL_GSM_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_BIB_AGL_GSM_STS_REPORT = BashOperator(
    task_id='Dedup2_BIB_AGL_GSM_STS_REPORT',
    bash_command= dedup_command_BIB_AGL_GSM_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_BIB_AGL_GSM_STS_REPORT = BashOperator(
    task_id='PCFCheck_BIB_AGL_GSM_STS_REPORT',
    bash_command=PCFCheck_command_BIB_AGL_GSM_STS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_BIB_AGL_GSM_STS_REPORT = EmailOperator(
    task_id='faile_BIB_AGL_GSM_STS_REPORT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BIB_AGL_GSM_STS_REPORT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BIB_AGL_GSM_STS_REPORT: PythonOperator = PythonOperator(task_id="waitForFlush_BIB_AGL_GSM_STS_REPORT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BIB_AGL_GSM_STS_REPORT >> PCF_BIB_AGL_GSM_STS_REPORT >> PCFCheck_BIB_AGL_GSM_STS_REPORT >> delay_python_task_BIB_AGL_GSM_STS_REPORT >> Dedup_BIB_AGL_GSM_STS_REPORT >> Validation_End 
Branching_BIB_AGL_GSM_STS_REPORT >> Dedup2_BIB_AGL_GSM_STS_REPORT >> Validation_End
Branching_BIB_AGL_GSM_STS_REPORT >> faile_BIB_AGL_GSM_STS_REPORT
Branching_BIB_AGL_GSM_STS_REPORT >> Validation_End


feedname_SERV_STATUS_SEAMFIX_REF = 'SERV_STATUS_SEAMFIX_REF'.upper()
feedname2_SERV_STATUS_SEAMFIX_REF = 'SERV_STATUS_SEAMFIX_REF'.lower()

dedup_command_SERV_STATUS_SEAMFIX_REF="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SERV_STATUS_SEAMFIX_REF)
logging.info(dedup_command_SERV_STATUS_SEAMFIX_REF) 

pcf_command_SERV_STATUS_SEAMFIX_REF = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SERV_STATUS_SEAMFIX_REF)
logging.info(pcf_command_SERV_STATUS_SEAMFIX_REF)

PCFCheck_command_SERV_STATUS_SEAMFIX_REF = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SERV_STATUS_SEAMFIX_REF,run_date,sleeptime)
logging.info(PCFCheck_command_SERV_STATUS_SEAMFIX_REF)

branchScript_SERV_STATUS_SEAMFIX_REF = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SERV_STATUS_SEAMFIX_REF,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SERV_STATUS_SEAMFIX_REF)

def branch_SERV_STATUS_SEAMFIX_REF():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SERV_STATUS_SEAMFIX_REF,feedname2_SERV_STATUS_SEAMFIX_REF)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SERV_STATUS_SEAMFIX_REF)
    x = readcsv(filepath,feedname2_SERV_STATUS_SEAMFIX_REF)
    logging.info('branch_SERV_STATUS_SEAMFIX_REF' + x)
    return x

Branching_SERV_STATUS_SEAMFIX_REF = BranchPythonOperator(
    task_id='branchid_SERV_STATUS_SEAMFIX_REF',
    python_callable=branch_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='PCF_SERV_STATUS_SEAMFIX_REF',
    bash_command= pcf_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='Dedup_SERV_STATUS_SEAMFIX_REF',
    bash_command= dedup_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='Dedup2_SERV_STATUS_SEAMFIX_REF',
    bash_command= dedup_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='PCFCheck_SERV_STATUS_SEAMFIX_REF',
    bash_command=PCFCheck_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SERV_STATUS_SEAMFIX_REF = EmailOperator(
    task_id='faile_SERV_STATUS_SEAMFIX_REF',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SERV_STATUS_SEAMFIX_REF),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SERV_STATUS_SEAMFIX_REF: PythonOperator = PythonOperator(task_id="waitForFlush_SERV_STATUS_SEAMFIX_REF",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SERV_STATUS_SEAMFIX_REF >> PCF_SERV_STATUS_SEAMFIX_REF >> PCFCheck_SERV_STATUS_SEAMFIX_REF >> delay_python_task_SERV_STATUS_SEAMFIX_REF >> Dedup_SERV_STATUS_SEAMFIX_REF >> Validation_End 
Branching_SERV_STATUS_SEAMFIX_REF >> Dedup2_SERV_STATUS_SEAMFIX_REF >> Validation_End
Branching_SERV_STATUS_SEAMFIX_REF >> faile_SERV_STATUS_SEAMFIX_REF
Branching_SERV_STATUS_SEAMFIX_REF >> Validation_End


feedname_CLAWBACK_RPT = 'CLAWBACK_RPT'.upper()
feedname2_CLAWBACK_RPT = 'CLAWBACK_RPT'.lower()

dedup_command_CLAWBACK_RPT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CLAWBACK_RPT)
logging.info(dedup_command_CLAWBACK_RPT) 

pcf_command_CLAWBACK_RPT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CLAWBACK_RPT)
logging.info(pcf_command_CLAWBACK_RPT)

PCFCheck_command_CLAWBACK_RPT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CLAWBACK_RPT,run_date,sleeptime)
logging.info(PCFCheck_command_CLAWBACK_RPT)

branchScript_CLAWBACK_RPT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CLAWBACK_RPT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CLAWBACK_RPT)

def branch_CLAWBACK_RPT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CLAWBACK_RPT,feedname2_CLAWBACK_RPT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CLAWBACK_RPT)
    x = readcsv(filepath,feedname2_CLAWBACK_RPT)
    logging.info('branch_CLAWBACK_RPT' + x)
    return x

Branching_CLAWBACK_RPT = BranchPythonOperator(
    task_id='branchid_CLAWBACK_RPT',
    python_callable=branch_CLAWBACK_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CLAWBACK_RPT = BashOperator(
    task_id='PCF_CLAWBACK_RPT',
    bash_command= pcf_command_CLAWBACK_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CLAWBACK_RPT = BashOperator(
    task_id='Dedup_CLAWBACK_RPT',
    bash_command= dedup_command_CLAWBACK_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CLAWBACK_RPT = BashOperator(
    task_id='Dedup2_CLAWBACK_RPT',
    bash_command= dedup_command_CLAWBACK_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CLAWBACK_RPT = BashOperator(
    task_id='PCFCheck_CLAWBACK_RPT',
    bash_command=PCFCheck_command_CLAWBACK_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CLAWBACK_RPT = EmailOperator(
    task_id='faile_CLAWBACK_RPT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CLAWBACK_RPT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CLAWBACK_RPT: PythonOperator = PythonOperator(task_id="waitForFlush_CLAWBACK_RPT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CLAWBACK_RPT >> PCF_CLAWBACK_RPT >> PCFCheck_CLAWBACK_RPT >> delay_python_task_CLAWBACK_RPT >> Dedup_CLAWBACK_RPT >> Validation_End 
Branching_CLAWBACK_RPT >> Dedup2_CLAWBACK_RPT >> Validation_End
Branching_CLAWBACK_RPT >> faile_CLAWBACK_RPT
Branching_CLAWBACK_RPT >> Validation_End


feedname_CUG_ACCESS_FEE_BASE_VW = 'CUG_ACCESS_FEE_BASE_VW'.upper()
feedname2_CUG_ACCESS_FEE_BASE_VW = 'CUG_ACCESS_FEE_BASE_VW'.lower()

dedup_command_CUG_ACCESS_FEE_BASE_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CUG_ACCESS_FEE_BASE_VW)
logging.info(dedup_command_CUG_ACCESS_FEE_BASE_VW) 

pcf_command_CUG_ACCESS_FEE_BASE_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CUG_ACCESS_FEE_BASE_VW)
logging.info(pcf_command_CUG_ACCESS_FEE_BASE_VW)

PCFCheck_command_CUG_ACCESS_FEE_BASE_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CUG_ACCESS_FEE_BASE_VW,run_date,sleeptime)
logging.info(PCFCheck_command_CUG_ACCESS_FEE_BASE_VW)

branchScript_CUG_ACCESS_FEE_BASE_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CUG_ACCESS_FEE_BASE_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CUG_ACCESS_FEE_BASE_VW)

def branch_CUG_ACCESS_FEE_BASE_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CUG_ACCESS_FEE_BASE_VW,feedname2_CUG_ACCESS_FEE_BASE_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CUG_ACCESS_FEE_BASE_VW)
    x = readcsv(filepath,feedname2_CUG_ACCESS_FEE_BASE_VW)
    logging.info('branch_CUG_ACCESS_FEE_BASE_VW' + x)
    return x

Branching_CUG_ACCESS_FEE_BASE_VW = BranchPythonOperator(
    task_id='branchid_CUG_ACCESS_FEE_BASE_VW',
    python_callable=branch_CUG_ACCESS_FEE_BASE_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CUG_ACCESS_FEE_BASE_VW = BashOperator(
    task_id='PCF_CUG_ACCESS_FEE_BASE_VW',
    bash_command= pcf_command_CUG_ACCESS_FEE_BASE_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CUG_ACCESS_FEE_BASE_VW = BashOperator(
    task_id='Dedup_CUG_ACCESS_FEE_BASE_VW',
    bash_command= dedup_command_CUG_ACCESS_FEE_BASE_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CUG_ACCESS_FEE_BASE_VW = BashOperator(
    task_id='Dedup2_CUG_ACCESS_FEE_BASE_VW',
    bash_command= dedup_command_CUG_ACCESS_FEE_BASE_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CUG_ACCESS_FEE_BASE_VW = BashOperator(
    task_id='PCFCheck_CUG_ACCESS_FEE_BASE_VW',
    bash_command=PCFCheck_command_CUG_ACCESS_FEE_BASE_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CUG_ACCESS_FEE_BASE_VW = EmailOperator(
    task_id='faile_CUG_ACCESS_FEE_BASE_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CUG_ACCESS_FEE_BASE_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CUG_ACCESS_FEE_BASE_VW: PythonOperator = PythonOperator(task_id="waitForFlush_CUG_ACCESS_FEE_BASE_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CUG_ACCESS_FEE_BASE_VW >> PCF_CUG_ACCESS_FEE_BASE_VW >> PCFCheck_CUG_ACCESS_FEE_BASE_VW >> delay_python_task_CUG_ACCESS_FEE_BASE_VW >> Dedup_CUG_ACCESS_FEE_BASE_VW >> Validation_End 
Branching_CUG_ACCESS_FEE_BASE_VW >> Dedup2_CUG_ACCESS_FEE_BASE_VW >> Validation_End
Branching_CUG_ACCESS_FEE_BASE_VW >> faile_CUG_ACCESS_FEE_BASE_VW
Branching_CUG_ACCESS_FEE_BASE_VW >> Validation_End


feedname_EXCESS_PAYMENT_RPT = 'EXCESS_PAYMENT_RPT'.upper()
feedname2_EXCESS_PAYMENT_RPT = 'EXCESS_PAYMENT_RPT'.lower()

dedup_command_EXCESS_PAYMENT_RPT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_EXCESS_PAYMENT_RPT)
logging.info(dedup_command_EXCESS_PAYMENT_RPT) 

pcf_command_EXCESS_PAYMENT_RPT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_EXCESS_PAYMENT_RPT)
logging.info(pcf_command_EXCESS_PAYMENT_RPT)

PCFCheck_command_EXCESS_PAYMENT_RPT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EXCESS_PAYMENT_RPT,run_date,sleeptime)
logging.info(PCFCheck_command_EXCESS_PAYMENT_RPT)

branchScript_EXCESS_PAYMENT_RPT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_EXCESS_PAYMENT_RPT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_EXCESS_PAYMENT_RPT)

def branch_EXCESS_PAYMENT_RPT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EXCESS_PAYMENT_RPT,feedname2_EXCESS_PAYMENT_RPT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EXCESS_PAYMENT_RPT)
    x = readcsv(filepath,feedname2_EXCESS_PAYMENT_RPT)
    logging.info('branch_EXCESS_PAYMENT_RPT' + x)
    return x

Branching_EXCESS_PAYMENT_RPT = BranchPythonOperator(
    task_id='branchid_EXCESS_PAYMENT_RPT',
    python_callable=branch_EXCESS_PAYMENT_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_EXCESS_PAYMENT_RPT = BashOperator(
    task_id='PCF_EXCESS_PAYMENT_RPT',
    bash_command= pcf_command_EXCESS_PAYMENT_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_EXCESS_PAYMENT_RPT = BashOperator(
    task_id='Dedup_EXCESS_PAYMENT_RPT',
    bash_command= dedup_command_EXCESS_PAYMENT_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_EXCESS_PAYMENT_RPT = BashOperator(
    task_id='Dedup2_EXCESS_PAYMENT_RPT',
    bash_command= dedup_command_EXCESS_PAYMENT_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_EXCESS_PAYMENT_RPT = BashOperator(
    task_id='PCFCheck_EXCESS_PAYMENT_RPT',
    bash_command=PCFCheck_command_EXCESS_PAYMENT_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_EXCESS_PAYMENT_RPT = EmailOperator(
    task_id='faile_EXCESS_PAYMENT_RPT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_EXCESS_PAYMENT_RPT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_EXCESS_PAYMENT_RPT: PythonOperator = PythonOperator(task_id="waitForFlush_EXCESS_PAYMENT_RPT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_EXCESS_PAYMENT_RPT >> PCF_EXCESS_PAYMENT_RPT >> PCFCheck_EXCESS_PAYMENT_RPT >> delay_python_task_EXCESS_PAYMENT_RPT >> Dedup_EXCESS_PAYMENT_RPT >> Validation_End 
Branching_EXCESS_PAYMENT_RPT >> Dedup2_EXCESS_PAYMENT_RPT >> Validation_End
Branching_EXCESS_PAYMENT_RPT >> faile_EXCESS_PAYMENT_RPT
Branching_EXCESS_PAYMENT_RPT >> Validation_End


feedname_MSO_BIB_PAYMENT_REVERSAL_VW = 'MSO_BIB_PAYMENT_REVERSAL_VW'.upper()
feedname2_MSO_BIB_PAYMENT_REVERSAL_VW = 'MSO_BIB_PAYMENT_REVERSAL_VW'.lower()

dedup_command_MSO_BIB_PAYMENT_REVERSAL_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MSO_BIB_PAYMENT_REVERSAL_VW)
logging.info(dedup_command_MSO_BIB_PAYMENT_REVERSAL_VW) 

pcf_command_MSO_BIB_PAYMENT_REVERSAL_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MSO_BIB_PAYMENT_REVERSAL_VW)
logging.info(pcf_command_MSO_BIB_PAYMENT_REVERSAL_VW)

PCFCheck_command_MSO_BIB_PAYMENT_REVERSAL_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSO_BIB_PAYMENT_REVERSAL_VW,run_date,sleeptime)
logging.info(PCFCheck_command_MSO_BIB_PAYMENT_REVERSAL_VW)

branchScript_MSO_BIB_PAYMENT_REVERSAL_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MSO_BIB_PAYMENT_REVERSAL_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MSO_BIB_PAYMENT_REVERSAL_VW)

def branch_MSO_BIB_PAYMENT_REVERSAL_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSO_BIB_PAYMENT_REVERSAL_VW,feedname2_MSO_BIB_PAYMENT_REVERSAL_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSO_BIB_PAYMENT_REVERSAL_VW)
    x = readcsv(filepath,feedname2_MSO_BIB_PAYMENT_REVERSAL_VW)
    logging.info('branch_MSO_BIB_PAYMENT_REVERSAL_VW' + x)
    return x

Branching_MSO_BIB_PAYMENT_REVERSAL_VW = BranchPythonOperator(
    task_id='branchid_MSO_BIB_PAYMENT_REVERSAL_VW',
    python_callable=branch_MSO_BIB_PAYMENT_REVERSAL_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MSO_BIB_PAYMENT_REVERSAL_VW = BashOperator(
    task_id='PCF_MSO_BIB_PAYMENT_REVERSAL_VW',
    bash_command= pcf_command_MSO_BIB_PAYMENT_REVERSAL_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MSO_BIB_PAYMENT_REVERSAL_VW = BashOperator(
    task_id='Dedup_MSO_BIB_PAYMENT_REVERSAL_VW',
    bash_command= dedup_command_MSO_BIB_PAYMENT_REVERSAL_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MSO_BIB_PAYMENT_REVERSAL_VW = BashOperator(
    task_id='Dedup2_MSO_BIB_PAYMENT_REVERSAL_VW',
    bash_command= dedup_command_MSO_BIB_PAYMENT_REVERSAL_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MSO_BIB_PAYMENT_REVERSAL_VW = BashOperator(
    task_id='PCFCheck_MSO_BIB_PAYMENT_REVERSAL_VW',
    bash_command=PCFCheck_command_MSO_BIB_PAYMENT_REVERSAL_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MSO_BIB_PAYMENT_REVERSAL_VW = EmailOperator(
    task_id='faile_MSO_BIB_PAYMENT_REVERSAL_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_BIB_PAYMENT_REVERSAL_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MSO_BIB_PAYMENT_REVERSAL_VW: PythonOperator = PythonOperator(task_id="waitForFlush_MSO_BIB_PAYMENT_REVERSAL_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MSO_BIB_PAYMENT_REVERSAL_VW >> PCF_MSO_BIB_PAYMENT_REVERSAL_VW >> PCFCheck_MSO_BIB_PAYMENT_REVERSAL_VW >> delay_python_task_MSO_BIB_PAYMENT_REVERSAL_VW >> Dedup_MSO_BIB_PAYMENT_REVERSAL_VW >> Validation_End 
Branching_MSO_BIB_PAYMENT_REVERSAL_VW >> Dedup2_MSO_BIB_PAYMENT_REVERSAL_VW >> Validation_End
Branching_MSO_BIB_PAYMENT_REVERSAL_VW >> faile_MSO_BIB_PAYMENT_REVERSAL_VW
Branching_MSO_BIB_PAYMENT_REVERSAL_VW >> Validation_End


feedname_MSO_POST_SWAP_EYEBALLING_VW = 'MSO_POST_SWAP_EYEBALLING_VW'.upper()
feedname2_MSO_POST_SWAP_EYEBALLING_VW = 'MSO_POST_SWAP_EYEBALLING_VW'.lower()

dedup_command_MSO_POST_SWAP_EYEBALLING_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MSO_POST_SWAP_EYEBALLING_VW)
logging.info(dedup_command_MSO_POST_SWAP_EYEBALLING_VW) 

pcf_command_MSO_POST_SWAP_EYEBALLING_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MSO_POST_SWAP_EYEBALLING_VW)
logging.info(pcf_command_MSO_POST_SWAP_EYEBALLING_VW)

PCFCheck_command_MSO_POST_SWAP_EYEBALLING_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSO_POST_SWAP_EYEBALLING_VW,run_date,sleeptime)
logging.info(PCFCheck_command_MSO_POST_SWAP_EYEBALLING_VW)

branchScript_MSO_POST_SWAP_EYEBALLING_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MSO_POST_SWAP_EYEBALLING_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MSO_POST_SWAP_EYEBALLING_VW)

def branch_MSO_POST_SWAP_EYEBALLING_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSO_POST_SWAP_EYEBALLING_VW,feedname2_MSO_POST_SWAP_EYEBALLING_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSO_POST_SWAP_EYEBALLING_VW)
    x = readcsv(filepath,feedname2_MSO_POST_SWAP_EYEBALLING_VW)
    logging.info('branch_MSO_POST_SWAP_EYEBALLING_VW' + x)
    return x

Branching_MSO_POST_SWAP_EYEBALLING_VW = BranchPythonOperator(
    task_id='branchid_MSO_POST_SWAP_EYEBALLING_VW',
    python_callable=branch_MSO_POST_SWAP_EYEBALLING_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MSO_POST_SWAP_EYEBALLING_VW = BashOperator(
    task_id='PCF_MSO_POST_SWAP_EYEBALLING_VW',
    bash_command= pcf_command_MSO_POST_SWAP_EYEBALLING_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MSO_POST_SWAP_EYEBALLING_VW = BashOperator(
    task_id='Dedup_MSO_POST_SWAP_EYEBALLING_VW',
    bash_command= dedup_command_MSO_POST_SWAP_EYEBALLING_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MSO_POST_SWAP_EYEBALLING_VW = BashOperator(
    task_id='Dedup2_MSO_POST_SWAP_EYEBALLING_VW',
    bash_command= dedup_command_MSO_POST_SWAP_EYEBALLING_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MSO_POST_SWAP_EYEBALLING_VW = BashOperator(
    task_id='PCFCheck_MSO_POST_SWAP_EYEBALLING_VW',
    bash_command=PCFCheck_command_MSO_POST_SWAP_EYEBALLING_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MSO_POST_SWAP_EYEBALLING_VW = EmailOperator(
    task_id='faile_MSO_POST_SWAP_EYEBALLING_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_POST_SWAP_EYEBALLING_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MSO_POST_SWAP_EYEBALLING_VW: PythonOperator = PythonOperator(task_id="waitForFlush_MSO_POST_SWAP_EYEBALLING_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MSO_POST_SWAP_EYEBALLING_VW >> PCF_MSO_POST_SWAP_EYEBALLING_VW >> PCFCheck_MSO_POST_SWAP_EYEBALLING_VW >> delay_python_task_MSO_POST_SWAP_EYEBALLING_VW >> Dedup_MSO_POST_SWAP_EYEBALLING_VW >> Validation_End 
Branching_MSO_POST_SWAP_EYEBALLING_VW >> Dedup2_MSO_POST_SWAP_EYEBALLING_VW >> Validation_End
Branching_MSO_POST_SWAP_EYEBALLING_VW >> faile_MSO_POST_SWAP_EYEBALLING_VW
Branching_MSO_POST_SWAP_EYEBALLING_VW >> Validation_End


feedname_CREDIT_LIMIT_ASSIGNED = 'CREDIT_LIMIT_ASSIGNED'.upper()
feedname2_CREDIT_LIMIT_ASSIGNED = 'CREDIT_LIMIT_ASSIGNED'.lower()

dedup_command_CREDIT_LIMIT_ASSIGNED="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CREDIT_LIMIT_ASSIGNED)
logging.info(dedup_command_CREDIT_LIMIT_ASSIGNED) 

pcf_command_CREDIT_LIMIT_ASSIGNED = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CREDIT_LIMIT_ASSIGNED)
logging.info(pcf_command_CREDIT_LIMIT_ASSIGNED)

PCFCheck_command_CREDIT_LIMIT_ASSIGNED = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CREDIT_LIMIT_ASSIGNED,run_date,sleeptime)
logging.info(PCFCheck_command_CREDIT_LIMIT_ASSIGNED)

branchScript_CREDIT_LIMIT_ASSIGNED = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CREDIT_LIMIT_ASSIGNED,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CREDIT_LIMIT_ASSIGNED)

def branch_CREDIT_LIMIT_ASSIGNED():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CREDIT_LIMIT_ASSIGNED,feedname2_CREDIT_LIMIT_ASSIGNED)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CREDIT_LIMIT_ASSIGNED)
    x = readcsv(filepath,feedname2_CREDIT_LIMIT_ASSIGNED)
    logging.info('branch_CREDIT_LIMIT_ASSIGNED' + x)
    return x

Branching_CREDIT_LIMIT_ASSIGNED = BranchPythonOperator(
    task_id='branchid_CREDIT_LIMIT_ASSIGNED',
    python_callable=branch_CREDIT_LIMIT_ASSIGNED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CREDIT_LIMIT_ASSIGNED = BashOperator(
    task_id='PCF_CREDIT_LIMIT_ASSIGNED',
    bash_command= pcf_command_CREDIT_LIMIT_ASSIGNED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CREDIT_LIMIT_ASSIGNED = BashOperator(
    task_id='Dedup_CREDIT_LIMIT_ASSIGNED',
    bash_command= dedup_command_CREDIT_LIMIT_ASSIGNED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CREDIT_LIMIT_ASSIGNED = BashOperator(
    task_id='Dedup2_CREDIT_LIMIT_ASSIGNED',
    bash_command= dedup_command_CREDIT_LIMIT_ASSIGNED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CREDIT_LIMIT_ASSIGNED = BashOperator(
    task_id='PCFCheck_CREDIT_LIMIT_ASSIGNED',
    bash_command=PCFCheck_command_CREDIT_LIMIT_ASSIGNED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CREDIT_LIMIT_ASSIGNED = EmailOperator(
    task_id='faile_CREDIT_LIMIT_ASSIGNED',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CREDIT_LIMIT_ASSIGNED),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CREDIT_LIMIT_ASSIGNED: PythonOperator = PythonOperator(task_id="waitForFlush_CREDIT_LIMIT_ASSIGNED",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CREDIT_LIMIT_ASSIGNED >> PCF_CREDIT_LIMIT_ASSIGNED >> PCFCheck_CREDIT_LIMIT_ASSIGNED >> delay_python_task_CREDIT_LIMIT_ASSIGNED >> Dedup_CREDIT_LIMIT_ASSIGNED >> Validation_End 
Branching_CREDIT_LIMIT_ASSIGNED >> Dedup2_CREDIT_LIMIT_ASSIGNED >> Validation_End
Branching_CREDIT_LIMIT_ASSIGNED >> faile_CREDIT_LIMIT_ASSIGNED
Branching_CREDIT_LIMIT_ASSIGNED >> Validation_End


feedname_CLM_WBO_LOCATION = 'CLM_WBO_LOCATION'.upper()
feedname2_CLM_WBO_LOCATION = 'CLM_WBO_LOCATION'.lower()

dedup_command_CLM_WBO_LOCATION="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CLM_WBO_LOCATION)
logging.info(dedup_command_CLM_WBO_LOCATION) 

pcf_command_CLM_WBO_LOCATION = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CLM_WBO_LOCATION)
logging.info(pcf_command_CLM_WBO_LOCATION)

PCFCheck_command_CLM_WBO_LOCATION = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CLM_WBO_LOCATION,run_date,sleeptime)
logging.info(PCFCheck_command_CLM_WBO_LOCATION)

branchScript_CLM_WBO_LOCATION = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CLM_WBO_LOCATION,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CLM_WBO_LOCATION)

def branch_CLM_WBO_LOCATION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CLM_WBO_LOCATION,feedname2_CLM_WBO_LOCATION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CLM_WBO_LOCATION)
    x = readcsv(filepath,feedname2_CLM_WBO_LOCATION)
    logging.info('branch_CLM_WBO_LOCATION' + x)
    return x

Branching_CLM_WBO_LOCATION = BranchPythonOperator(
    task_id='branchid_CLM_WBO_LOCATION',
    python_callable=branch_CLM_WBO_LOCATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CLM_WBO_LOCATION = BashOperator(
    task_id='PCF_CLM_WBO_LOCATION',
    bash_command= pcf_command_CLM_WBO_LOCATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CLM_WBO_LOCATION = BashOperator(
    task_id='Dedup_CLM_WBO_LOCATION',
    bash_command= dedup_command_CLM_WBO_LOCATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CLM_WBO_LOCATION = BashOperator(
    task_id='Dedup2_CLM_WBO_LOCATION',
    bash_command= dedup_command_CLM_WBO_LOCATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CLM_WBO_LOCATION = BashOperator(
    task_id='PCFCheck_CLM_WBO_LOCATION',
    bash_command=PCFCheck_command_CLM_WBO_LOCATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CLM_WBO_LOCATION = EmailOperator(
    task_id='faile_CLM_WBO_LOCATION',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CLM_WBO_LOCATION),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CLM_WBO_LOCATION: PythonOperator = PythonOperator(task_id="waitForFlush_CLM_WBO_LOCATION",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CLM_WBO_LOCATION >> PCF_CLM_WBO_LOCATION >> PCFCheck_CLM_WBO_LOCATION >> delay_python_task_CLM_WBO_LOCATION >> Dedup_CLM_WBO_LOCATION >> Validation_End 
Branching_CLM_WBO_LOCATION >> Dedup2_CLM_WBO_LOCATION >> Validation_End
Branching_CLM_WBO_LOCATION >> faile_CLM_WBO_LOCATION
Branching_CLM_WBO_LOCATION >> Validation_End
