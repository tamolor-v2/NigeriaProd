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
d_1 = Variable.get("rerunDate5", deserialize_json=True)
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

dag = DAG('New_Automation_Group_5', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group5 -c config.json' %(d_1)
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


feedname_CC_SURVEY = 'CC_SURVEY'.upper()
feedname2_CC_SURVEY = 'CC_SURVEY'.lower()

dedup_command_CC_SURVEY="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CC_SURVEY)
logging.info(dedup_command_CC_SURVEY) 

pcf_command_CC_SURVEY = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CC_SURVEY)
logging.info(pcf_command_CC_SURVEY)

PCFCheck_command_CC_SURVEY = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CC_SURVEY,run_date,sleeptime)
logging.info(PCFCheck_command_CC_SURVEY)

branchScript_CC_SURVEY = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CC_SURVEY,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CC_SURVEY)

def branch_CC_SURVEY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CC_SURVEY,feedname2_CC_SURVEY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CC_SURVEY)
    x = readcsv(filepath,feedname2_CC_SURVEY)
    logging.info('branch_CC_SURVEY' + x)
    return x

Branching_CC_SURVEY = BranchPythonOperator(
    task_id='branchid_CC_SURVEY',
    python_callable=branch_CC_SURVEY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CC_SURVEY = BashOperator(
    task_id='PCF_CC_SURVEY',
    bash_command= pcf_command_CC_SURVEY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CC_SURVEY = BashOperator(
    task_id='Dedup_CC_SURVEY',
    bash_command= dedup_command_CC_SURVEY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CC_SURVEY = BashOperator(
    task_id='Dedup2_CC_SURVEY',
    bash_command= dedup_command_CC_SURVEY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CC_SURVEY = BashOperator(
    task_id='PCFCheck_CC_SURVEY',
    bash_command=PCFCheck_command_CC_SURVEY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CC_SURVEY = EmailOperator(
    task_id='faile_CC_SURVEY',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CC_SURVEY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CC_SURVEY: PythonOperator = PythonOperator(task_id="waitForFlush_CC_SURVEY",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CC_SURVEY >> PCF_CC_SURVEY >> PCFCheck_CC_SURVEY >> delay_python_task_CC_SURVEY >> Dedup_CC_SURVEY >> Validation_End 
Branching_CC_SURVEY >> Dedup2_CC_SURVEY >> Validation_End
Branching_CC_SURVEY >> faile_CC_SURVEY
Branching_CC_SURVEY >> Validation_End


feedname_CB_SUBS_POS_SERVICES = 'CB_SUBS_POS_SERVICES'.upper()
feedname2_CB_SUBS_POS_SERVICES = 'CB_SUBS_POS_SERVICES'.lower()

dedup_command_CB_SUBS_POS_SERVICES="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CB_SUBS_POS_SERVICES)
logging.info(dedup_command_CB_SUBS_POS_SERVICES) 

pcf_command_CB_SUBS_POS_SERVICES = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CB_SUBS_POS_SERVICES)
logging.info(pcf_command_CB_SUBS_POS_SERVICES)

PCFCheck_command_CB_SUBS_POS_SERVICES = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_SUBS_POS_SERVICES,run_date,sleeptime)
logging.info(PCFCheck_command_CB_SUBS_POS_SERVICES)

branchScript_CB_SUBS_POS_SERVICES = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_SUBS_POS_SERVICES,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_SUBS_POS_SERVICES)

def branch_CB_SUBS_POS_SERVICES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_SUBS_POS_SERVICES,feedname2_CB_SUBS_POS_SERVICES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_SUBS_POS_SERVICES)
    x = readcsv(filepath,feedname2_CB_SUBS_POS_SERVICES)
    logging.info('branch_CB_SUBS_POS_SERVICES' + x)
    return x

Branching_CB_SUBS_POS_SERVICES = BranchPythonOperator(
    task_id='branchid_CB_SUBS_POS_SERVICES',
    python_callable=branch_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='PCF_CB_SUBS_POS_SERVICES',
    bash_command= pcf_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='Dedup_CB_SUBS_POS_SERVICES',
    bash_command= dedup_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='Dedup2_CB_SUBS_POS_SERVICES',
    bash_command= dedup_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='PCFCheck_CB_SUBS_POS_SERVICES',
    bash_command=PCFCheck_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CB_SUBS_POS_SERVICES = EmailOperator(
    task_id='faile_CB_SUBS_POS_SERVICES',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_SUBS_POS_SERVICES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CB_SUBS_POS_SERVICES: PythonOperator = PythonOperator(task_id="waitForFlush_CB_SUBS_POS_SERVICES",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_SUBS_POS_SERVICES >> PCF_CB_SUBS_POS_SERVICES >> PCFCheck_CB_SUBS_POS_SERVICES >> delay_python_task_CB_SUBS_POS_SERVICES >> Dedup_CB_SUBS_POS_SERVICES >> Validation_End 
Branching_CB_SUBS_POS_SERVICES >> Dedup2_CB_SUBS_POS_SERVICES >> Validation_End
Branching_CB_SUBS_POS_SERVICES >> faile_CB_SUBS_POS_SERVICES
Branching_CB_SUBS_POS_SERVICES >> Validation_End


feedname_MSO_PROCESS_AVG_TIME = 'MSO_PROCESS_AVG_TIME'.upper()
feedname2_MSO_PROCESS_AVG_TIME = 'MSO_PROCESS_AVG_TIME'.lower()

dedup_command_MSO_PROCESS_AVG_TIME="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MSO_PROCESS_AVG_TIME)
logging.info(dedup_command_MSO_PROCESS_AVG_TIME) 

pcf_command_MSO_PROCESS_AVG_TIME = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MSO_PROCESS_AVG_TIME)
logging.info(pcf_command_MSO_PROCESS_AVG_TIME)

PCFCheck_command_MSO_PROCESS_AVG_TIME = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSO_PROCESS_AVG_TIME,run_date,sleeptime)
logging.info(PCFCheck_command_MSO_PROCESS_AVG_TIME)

branchScript_MSO_PROCESS_AVG_TIME = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MSO_PROCESS_AVG_TIME,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MSO_PROCESS_AVG_TIME)

def branch_MSO_PROCESS_AVG_TIME():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSO_PROCESS_AVG_TIME,feedname2_MSO_PROCESS_AVG_TIME)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSO_PROCESS_AVG_TIME)
    x = readcsv(filepath,feedname2_MSO_PROCESS_AVG_TIME)
    logging.info('branch_MSO_PROCESS_AVG_TIME' + x)
    return x

Branching_MSO_PROCESS_AVG_TIME = BranchPythonOperator(
    task_id='branchid_MSO_PROCESS_AVG_TIME',
    python_callable=branch_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='PCF_MSO_PROCESS_AVG_TIME',
    bash_command= pcf_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='Dedup_MSO_PROCESS_AVG_TIME',
    bash_command= dedup_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='Dedup2_MSO_PROCESS_AVG_TIME',
    bash_command= dedup_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='PCFCheck_MSO_PROCESS_AVG_TIME',
    bash_command=PCFCheck_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MSO_PROCESS_AVG_TIME = EmailOperator(
    task_id='faile_MSO_PROCESS_AVG_TIME',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_PROCESS_AVG_TIME),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MSO_PROCESS_AVG_TIME: PythonOperator = PythonOperator(task_id="waitForFlush_MSO_PROCESS_AVG_TIME",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MSO_PROCESS_AVG_TIME >> PCF_MSO_PROCESS_AVG_TIME >> PCFCheck_MSO_PROCESS_AVG_TIME >> delay_python_task_MSO_PROCESS_AVG_TIME >> Dedup_MSO_PROCESS_AVG_TIME >> Validation_End 
Branching_MSO_PROCESS_AVG_TIME >> Dedup2_MSO_PROCESS_AVG_TIME >> Validation_End
Branching_MSO_PROCESS_AVG_TIME >> faile_MSO_PROCESS_AVG_TIME
Branching_MSO_PROCESS_AVG_TIME >> Validation_End


feedname_AGL_GDS_PROVIDER_CHILD_DTLS = 'AGL_GDS_PROVIDER_CHILD_DTLS'.upper()
feedname2_AGL_GDS_PROVIDER_CHILD_DTLS = 'AGL_GDS_PROVIDER_CHILD_DTLS'.lower()

dedup_command_AGL_GDS_PROVIDER_CHILD_DTLS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AGL_GDS_PROVIDER_CHILD_DTLS)
logging.info(dedup_command_AGL_GDS_PROVIDER_CHILD_DTLS) 

pcf_command_AGL_GDS_PROVIDER_CHILD_DTLS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AGL_GDS_PROVIDER_CHILD_DTLS)
logging.info(pcf_command_AGL_GDS_PROVIDER_CHILD_DTLS)

PCFCheck_command_AGL_GDS_PROVIDER_CHILD_DTLS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AGL_GDS_PROVIDER_CHILD_DTLS,run_date,sleeptime)
logging.info(PCFCheck_command_AGL_GDS_PROVIDER_CHILD_DTLS)

branchScript_AGL_GDS_PROVIDER_CHILD_DTLS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AGL_GDS_PROVIDER_CHILD_DTLS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AGL_GDS_PROVIDER_CHILD_DTLS)

def branch_AGL_GDS_PROVIDER_CHILD_DTLS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AGL_GDS_PROVIDER_CHILD_DTLS,feedname2_AGL_GDS_PROVIDER_CHILD_DTLS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AGL_GDS_PROVIDER_CHILD_DTLS)
    x = readcsv(filepath,feedname2_AGL_GDS_PROVIDER_CHILD_DTLS)
    logging.info('branch_AGL_GDS_PROVIDER_CHILD_DTLS' + x)
    return x

Branching_AGL_GDS_PROVIDER_CHILD_DTLS = BranchPythonOperator(
    task_id='branchid_AGL_GDS_PROVIDER_CHILD_DTLS',
    python_callable=branch_AGL_GDS_PROVIDER_CHILD_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AGL_GDS_PROVIDER_CHILD_DTLS = BashOperator(
    task_id='PCF_AGL_GDS_PROVIDER_CHILD_DTLS',
    bash_command= pcf_command_AGL_GDS_PROVIDER_CHILD_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AGL_GDS_PROVIDER_CHILD_DTLS = BashOperator(
    task_id='Dedup_AGL_GDS_PROVIDER_CHILD_DTLS',
    bash_command= dedup_command_AGL_GDS_PROVIDER_CHILD_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AGL_GDS_PROVIDER_CHILD_DTLS = BashOperator(
    task_id='Dedup2_AGL_GDS_PROVIDER_CHILD_DTLS',
    bash_command= dedup_command_AGL_GDS_PROVIDER_CHILD_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AGL_GDS_PROVIDER_CHILD_DTLS = BashOperator(
    task_id='PCFCheck_AGL_GDS_PROVIDER_CHILD_DTLS',
    bash_command=PCFCheck_command_AGL_GDS_PROVIDER_CHILD_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AGL_GDS_PROVIDER_CHILD_DTLS = EmailOperator(
    task_id='faile_AGL_GDS_PROVIDER_CHILD_DTLS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AGL_GDS_PROVIDER_CHILD_DTLS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AGL_GDS_PROVIDER_CHILD_DTLS: PythonOperator = PythonOperator(task_id="waitForFlush_AGL_GDS_PROVIDER_CHILD_DTLS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AGL_GDS_PROVIDER_CHILD_DTLS >> PCF_AGL_GDS_PROVIDER_CHILD_DTLS >> PCFCheck_AGL_GDS_PROVIDER_CHILD_DTLS >> delay_python_task_AGL_GDS_PROVIDER_CHILD_DTLS >> Dedup_AGL_GDS_PROVIDER_CHILD_DTLS >> Validation_End 
Branching_AGL_GDS_PROVIDER_CHILD_DTLS >> Dedup2_AGL_GDS_PROVIDER_CHILD_DTLS >> Validation_End
Branching_AGL_GDS_PROVIDER_CHILD_DTLS >> faile_AGL_GDS_PROVIDER_CHILD_DTLS
Branching_AGL_GDS_PROVIDER_CHILD_DTLS >> Validation_End


feedname_ALL_STMT_ACCOUNT_OPEN_INV_VW = 'ALL_STMT_ACCOUNT_OPEN_INV_VW'.upper()
feedname2_ALL_STMT_ACCOUNT_OPEN_INV_VW = 'ALL_STMT_ACCOUNT_OPEN_INV_VW'.lower()

dedup_command_ALL_STMT_ACCOUNT_OPEN_INV_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ALL_STMT_ACCOUNT_OPEN_INV_VW)
logging.info(dedup_command_ALL_STMT_ACCOUNT_OPEN_INV_VW) 

pcf_command_ALL_STMT_ACCOUNT_OPEN_INV_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ALL_STMT_ACCOUNT_OPEN_INV_VW)
logging.info(pcf_command_ALL_STMT_ACCOUNT_OPEN_INV_VW)

PCFCheck_command_ALL_STMT_ACCOUNT_OPEN_INV_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ALL_STMT_ACCOUNT_OPEN_INV_VW,run_date,sleeptime)
logging.info(PCFCheck_command_ALL_STMT_ACCOUNT_OPEN_INV_VW)

branchScript_ALL_STMT_ACCOUNT_OPEN_INV_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ALL_STMT_ACCOUNT_OPEN_INV_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ALL_STMT_ACCOUNT_OPEN_INV_VW)

def branch_ALL_STMT_ACCOUNT_OPEN_INV_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ALL_STMT_ACCOUNT_OPEN_INV_VW,feedname2_ALL_STMT_ACCOUNT_OPEN_INV_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ALL_STMT_ACCOUNT_OPEN_INV_VW)
    x = readcsv(filepath,feedname2_ALL_STMT_ACCOUNT_OPEN_INV_VW)
    logging.info('branch_ALL_STMT_ACCOUNT_OPEN_INV_VW' + x)
    return x

Branching_ALL_STMT_ACCOUNT_OPEN_INV_VW = BranchPythonOperator(
    task_id='branchid_ALL_STMT_ACCOUNT_OPEN_INV_VW',
    python_callable=branch_ALL_STMT_ACCOUNT_OPEN_INV_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ALL_STMT_ACCOUNT_OPEN_INV_VW = BashOperator(
    task_id='PCF_ALL_STMT_ACCOUNT_OPEN_INV_VW',
    bash_command= pcf_command_ALL_STMT_ACCOUNT_OPEN_INV_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ALL_STMT_ACCOUNT_OPEN_INV_VW = BashOperator(
    task_id='Dedup_ALL_STMT_ACCOUNT_OPEN_INV_VW',
    bash_command= dedup_command_ALL_STMT_ACCOUNT_OPEN_INV_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ALL_STMT_ACCOUNT_OPEN_INV_VW = BashOperator(
    task_id='Dedup2_ALL_STMT_ACCOUNT_OPEN_INV_VW',
    bash_command= dedup_command_ALL_STMT_ACCOUNT_OPEN_INV_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ALL_STMT_ACCOUNT_OPEN_INV_VW = BashOperator(
    task_id='PCFCheck_ALL_STMT_ACCOUNT_OPEN_INV_VW',
    bash_command=PCFCheck_command_ALL_STMT_ACCOUNT_OPEN_INV_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ALL_STMT_ACCOUNT_OPEN_INV_VW = EmailOperator(
    task_id='faile_ALL_STMT_ACCOUNT_OPEN_INV_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ALL_STMT_ACCOUNT_OPEN_INV_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ALL_STMT_ACCOUNT_OPEN_INV_VW: PythonOperator = PythonOperator(task_id="waitForFlush_ALL_STMT_ACCOUNT_OPEN_INV_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ALL_STMT_ACCOUNT_OPEN_INV_VW >> PCF_ALL_STMT_ACCOUNT_OPEN_INV_VW >> PCFCheck_ALL_STMT_ACCOUNT_OPEN_INV_VW >> delay_python_task_ALL_STMT_ACCOUNT_OPEN_INV_VW >> Dedup_ALL_STMT_ACCOUNT_OPEN_INV_VW >> Validation_End 
Branching_ALL_STMT_ACCOUNT_OPEN_INV_VW >> Dedup2_ALL_STMT_ACCOUNT_OPEN_INV_VW >> Validation_End
Branching_ALL_STMT_ACCOUNT_OPEN_INV_VW >> faile_ALL_STMT_ACCOUNT_OPEN_INV_VW
Branching_ALL_STMT_ACCOUNT_OPEN_INV_VW >> Validation_End


feedname_ALL_STMT_SERVICE_OPEN_CRED_VW = 'ALL_STMT_SERVICE_OPEN_CRED_VW'.upper()
feedname2_ALL_STMT_SERVICE_OPEN_CRED_VW = 'ALL_STMT_SERVICE_OPEN_CRED_VW'.lower()

dedup_command_ALL_STMT_SERVICE_OPEN_CRED_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ALL_STMT_SERVICE_OPEN_CRED_VW)
logging.info(dedup_command_ALL_STMT_SERVICE_OPEN_CRED_VW) 

pcf_command_ALL_STMT_SERVICE_OPEN_CRED_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ALL_STMT_SERVICE_OPEN_CRED_VW)
logging.info(pcf_command_ALL_STMT_SERVICE_OPEN_CRED_VW)

PCFCheck_command_ALL_STMT_SERVICE_OPEN_CRED_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ALL_STMT_SERVICE_OPEN_CRED_VW,run_date,sleeptime)
logging.info(PCFCheck_command_ALL_STMT_SERVICE_OPEN_CRED_VW)

branchScript_ALL_STMT_SERVICE_OPEN_CRED_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ALL_STMT_SERVICE_OPEN_CRED_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ALL_STMT_SERVICE_OPEN_CRED_VW)

def branch_ALL_STMT_SERVICE_OPEN_CRED_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ALL_STMT_SERVICE_OPEN_CRED_VW,feedname2_ALL_STMT_SERVICE_OPEN_CRED_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ALL_STMT_SERVICE_OPEN_CRED_VW)
    x = readcsv(filepath,feedname2_ALL_STMT_SERVICE_OPEN_CRED_VW)
    logging.info('branch_ALL_STMT_SERVICE_OPEN_CRED_VW' + x)
    return x

Branching_ALL_STMT_SERVICE_OPEN_CRED_VW = BranchPythonOperator(
    task_id='branchid_ALL_STMT_SERVICE_OPEN_CRED_VW',
    python_callable=branch_ALL_STMT_SERVICE_OPEN_CRED_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ALL_STMT_SERVICE_OPEN_CRED_VW = BashOperator(
    task_id='PCF_ALL_STMT_SERVICE_OPEN_CRED_VW',
    bash_command= pcf_command_ALL_STMT_SERVICE_OPEN_CRED_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ALL_STMT_SERVICE_OPEN_CRED_VW = BashOperator(
    task_id='Dedup_ALL_STMT_SERVICE_OPEN_CRED_VW',
    bash_command= dedup_command_ALL_STMT_SERVICE_OPEN_CRED_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ALL_STMT_SERVICE_OPEN_CRED_VW = BashOperator(
    task_id='Dedup2_ALL_STMT_SERVICE_OPEN_CRED_VW',
    bash_command= dedup_command_ALL_STMT_SERVICE_OPEN_CRED_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ALL_STMT_SERVICE_OPEN_CRED_VW = BashOperator(
    task_id='PCFCheck_ALL_STMT_SERVICE_OPEN_CRED_VW',
    bash_command=PCFCheck_command_ALL_STMT_SERVICE_OPEN_CRED_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ALL_STMT_SERVICE_OPEN_CRED_VW = EmailOperator(
    task_id='faile_ALL_STMT_SERVICE_OPEN_CRED_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ALL_STMT_SERVICE_OPEN_CRED_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ALL_STMT_SERVICE_OPEN_CRED_VW: PythonOperator = PythonOperator(task_id="waitForFlush_ALL_STMT_SERVICE_OPEN_CRED_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ALL_STMT_SERVICE_OPEN_CRED_VW >> PCF_ALL_STMT_SERVICE_OPEN_CRED_VW >> PCFCheck_ALL_STMT_SERVICE_OPEN_CRED_VW >> delay_python_task_ALL_STMT_SERVICE_OPEN_CRED_VW >> Dedup_ALL_STMT_SERVICE_OPEN_CRED_VW >> Validation_End 
Branching_ALL_STMT_SERVICE_OPEN_CRED_VW >> Dedup2_ALL_STMT_SERVICE_OPEN_CRED_VW >> Validation_End
Branching_ALL_STMT_SERVICE_OPEN_CRED_VW >> faile_ALL_STMT_SERVICE_OPEN_CRED_VW
Branching_ALL_STMT_SERVICE_OPEN_CRED_VW >> Validation_End


feedname_ALL_STMT_SERVICE_OPEN_DEBT_VW = 'ALL_STMT_SERVICE_OPEN_DEBT_VW'.upper()
feedname2_ALL_STMT_SERVICE_OPEN_DEBT_VW = 'ALL_STMT_SERVICE_OPEN_DEBT_VW'.lower()

dedup_command_ALL_STMT_SERVICE_OPEN_DEBT_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ALL_STMT_SERVICE_OPEN_DEBT_VW)
logging.info(dedup_command_ALL_STMT_SERVICE_OPEN_DEBT_VW) 

pcf_command_ALL_STMT_SERVICE_OPEN_DEBT_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ALL_STMT_SERVICE_OPEN_DEBT_VW)
logging.info(pcf_command_ALL_STMT_SERVICE_OPEN_DEBT_VW)

PCFCheck_command_ALL_STMT_SERVICE_OPEN_DEBT_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ALL_STMT_SERVICE_OPEN_DEBT_VW,run_date,sleeptime)
logging.info(PCFCheck_command_ALL_STMT_SERVICE_OPEN_DEBT_VW)

branchScript_ALL_STMT_SERVICE_OPEN_DEBT_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ALL_STMT_SERVICE_OPEN_DEBT_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ALL_STMT_SERVICE_OPEN_DEBT_VW)

def branch_ALL_STMT_SERVICE_OPEN_DEBT_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ALL_STMT_SERVICE_OPEN_DEBT_VW,feedname2_ALL_STMT_SERVICE_OPEN_DEBT_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ALL_STMT_SERVICE_OPEN_DEBT_VW)
    x = readcsv(filepath,feedname2_ALL_STMT_SERVICE_OPEN_DEBT_VW)
    logging.info('branch_ALL_STMT_SERVICE_OPEN_DEBT_VW' + x)
    return x

Branching_ALL_STMT_SERVICE_OPEN_DEBT_VW = BranchPythonOperator(
    task_id='branchid_ALL_STMT_SERVICE_OPEN_DEBT_VW',
    python_callable=branch_ALL_STMT_SERVICE_OPEN_DEBT_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ALL_STMT_SERVICE_OPEN_DEBT_VW = BashOperator(
    task_id='PCF_ALL_STMT_SERVICE_OPEN_DEBT_VW',
    bash_command= pcf_command_ALL_STMT_SERVICE_OPEN_DEBT_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ALL_STMT_SERVICE_OPEN_DEBT_VW = BashOperator(
    task_id='Dedup_ALL_STMT_SERVICE_OPEN_DEBT_VW',
    bash_command= dedup_command_ALL_STMT_SERVICE_OPEN_DEBT_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ALL_STMT_SERVICE_OPEN_DEBT_VW = BashOperator(
    task_id='Dedup2_ALL_STMT_SERVICE_OPEN_DEBT_VW',
    bash_command= dedup_command_ALL_STMT_SERVICE_OPEN_DEBT_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ALL_STMT_SERVICE_OPEN_DEBT_VW = BashOperator(
    task_id='PCFCheck_ALL_STMT_SERVICE_OPEN_DEBT_VW',
    bash_command=PCFCheck_command_ALL_STMT_SERVICE_OPEN_DEBT_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ALL_STMT_SERVICE_OPEN_DEBT_VW = EmailOperator(
    task_id='faile_ALL_STMT_SERVICE_OPEN_DEBT_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ALL_STMT_SERVICE_OPEN_DEBT_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ALL_STMT_SERVICE_OPEN_DEBT_VW: PythonOperator = PythonOperator(task_id="waitForFlush_ALL_STMT_SERVICE_OPEN_DEBT_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ALL_STMT_SERVICE_OPEN_DEBT_VW >> PCF_ALL_STMT_SERVICE_OPEN_DEBT_VW >> PCFCheck_ALL_STMT_SERVICE_OPEN_DEBT_VW >> delay_python_task_ALL_STMT_SERVICE_OPEN_DEBT_VW >> Dedup_ALL_STMT_SERVICE_OPEN_DEBT_VW >> Validation_End 
Branching_ALL_STMT_SERVICE_OPEN_DEBT_VW >> Dedup2_ALL_STMT_SERVICE_OPEN_DEBT_VW >> Validation_End
Branching_ALL_STMT_SERVICE_OPEN_DEBT_VW >> faile_ALL_STMT_SERVICE_OPEN_DEBT_VW
Branching_ALL_STMT_SERVICE_OPEN_DEBT_VW >> Validation_End


feedname_HYNETFLEX_CUSTOMER = 'HYNETFLEX_CUSTOMER'.upper()
feedname2_HYNETFLEX_CUSTOMER = 'HYNETFLEX_CUSTOMER'.lower()

dedup_command_HYNETFLEX_CUSTOMER="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_HYNETFLEX_CUSTOMER)
logging.info(dedup_command_HYNETFLEX_CUSTOMER) 

pcf_command_HYNETFLEX_CUSTOMER = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_HYNETFLEX_CUSTOMER)
logging.info(pcf_command_HYNETFLEX_CUSTOMER)

PCFCheck_command_HYNETFLEX_CUSTOMER = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_HYNETFLEX_CUSTOMER,run_date,sleeptime)
logging.info(PCFCheck_command_HYNETFLEX_CUSTOMER)

branchScript_HYNETFLEX_CUSTOMER = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_HYNETFLEX_CUSTOMER,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_HYNETFLEX_CUSTOMER)

def branch_HYNETFLEX_CUSTOMER():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_HYNETFLEX_CUSTOMER,feedname2_HYNETFLEX_CUSTOMER)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_HYNETFLEX_CUSTOMER)
    x = readcsv(filepath,feedname2_HYNETFLEX_CUSTOMER)
    logging.info('branch_HYNETFLEX_CUSTOMER' + x)
    return x

Branching_HYNETFLEX_CUSTOMER = BranchPythonOperator(
    task_id='branchid_HYNETFLEX_CUSTOMER',
    python_callable=branch_HYNETFLEX_CUSTOMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_HYNETFLEX_CUSTOMER = BashOperator(
    task_id='PCF_HYNETFLEX_CUSTOMER',
    bash_command= pcf_command_HYNETFLEX_CUSTOMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_HYNETFLEX_CUSTOMER = BashOperator(
    task_id='Dedup_HYNETFLEX_CUSTOMER',
    bash_command= dedup_command_HYNETFLEX_CUSTOMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_HYNETFLEX_CUSTOMER = BashOperator(
    task_id='Dedup2_HYNETFLEX_CUSTOMER',
    bash_command= dedup_command_HYNETFLEX_CUSTOMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_HYNETFLEX_CUSTOMER = BashOperator(
    task_id='PCFCheck_HYNETFLEX_CUSTOMER',
    bash_command=PCFCheck_command_HYNETFLEX_CUSTOMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_HYNETFLEX_CUSTOMER = EmailOperator(
    task_id='faile_HYNETFLEX_CUSTOMER',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_HYNETFLEX_CUSTOMER),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_HYNETFLEX_CUSTOMER: PythonOperator = PythonOperator(task_id="waitForFlush_HYNETFLEX_CUSTOMER",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_HYNETFLEX_CUSTOMER >> PCF_HYNETFLEX_CUSTOMER >> PCFCheck_HYNETFLEX_CUSTOMER >> delay_python_task_HYNETFLEX_CUSTOMER >> Dedup_HYNETFLEX_CUSTOMER >> Validation_End 
Branching_HYNETFLEX_CUSTOMER >> Dedup2_HYNETFLEX_CUSTOMER >> Validation_End
Branching_HYNETFLEX_CUSTOMER >> faile_HYNETFLEX_CUSTOMER
Branching_HYNETFLEX_CUSTOMER >> Validation_End


feedname_CB_MNP_INTERFACE_DTLS = 'CB_MNP_INTERFACE_DTLS'.upper()
feedname2_CB_MNP_INTERFACE_DTLS = 'CB_MNP_INTERFACE_DTLS'.lower()

dedup_command_CB_MNP_INTERFACE_DTLS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CB_MNP_INTERFACE_DTLS)
logging.info(dedup_command_CB_MNP_INTERFACE_DTLS) 

pcf_command_CB_MNP_INTERFACE_DTLS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CB_MNP_INTERFACE_DTLS)
logging.info(pcf_command_CB_MNP_INTERFACE_DTLS)

PCFCheck_command_CB_MNP_INTERFACE_DTLS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_MNP_INTERFACE_DTLS,run_date,sleeptime)
logging.info(PCFCheck_command_CB_MNP_INTERFACE_DTLS)

branchScript_CB_MNP_INTERFACE_DTLS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_MNP_INTERFACE_DTLS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_MNP_INTERFACE_DTLS)

def branch_CB_MNP_INTERFACE_DTLS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_MNP_INTERFACE_DTLS,feedname2_CB_MNP_INTERFACE_DTLS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_MNP_INTERFACE_DTLS)
    x = readcsv(filepath,feedname2_CB_MNP_INTERFACE_DTLS)
    logging.info('branch_CB_MNP_INTERFACE_DTLS' + x)
    return x

Branching_CB_MNP_INTERFACE_DTLS = BranchPythonOperator(
    task_id='branchid_CB_MNP_INTERFACE_DTLS',
    python_callable=branch_CB_MNP_INTERFACE_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CB_MNP_INTERFACE_DTLS = BashOperator(
    task_id='PCF_CB_MNP_INTERFACE_DTLS',
    bash_command= pcf_command_CB_MNP_INTERFACE_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CB_MNP_INTERFACE_DTLS = BashOperator(
    task_id='Dedup_CB_MNP_INTERFACE_DTLS',
    bash_command= dedup_command_CB_MNP_INTERFACE_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CB_MNP_INTERFACE_DTLS = BashOperator(
    task_id='Dedup2_CB_MNP_INTERFACE_DTLS',
    bash_command= dedup_command_CB_MNP_INTERFACE_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CB_MNP_INTERFACE_DTLS = BashOperator(
    task_id='PCFCheck_CB_MNP_INTERFACE_DTLS',
    bash_command=PCFCheck_command_CB_MNP_INTERFACE_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CB_MNP_INTERFACE_DTLS = EmailOperator(
    task_id='faile_CB_MNP_INTERFACE_DTLS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_MNP_INTERFACE_DTLS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CB_MNP_INTERFACE_DTLS: PythonOperator = PythonOperator(task_id="waitForFlush_CB_MNP_INTERFACE_DTLS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_MNP_INTERFACE_DTLS >> PCF_CB_MNP_INTERFACE_DTLS >> PCFCheck_CB_MNP_INTERFACE_DTLS >> delay_python_task_CB_MNP_INTERFACE_DTLS >> Dedup_CB_MNP_INTERFACE_DTLS >> Validation_End 
Branching_CB_MNP_INTERFACE_DTLS >> Dedup2_CB_MNP_INTERFACE_DTLS >> Validation_End
Branching_CB_MNP_INTERFACE_DTLS >> faile_CB_MNP_INTERFACE_DTLS
Branching_CB_MNP_INTERFACE_DTLS >> Validation_End


feedname_CC_ONLINE_ACTIVITY = 'CC_ONLINE_ACTIVITY'.upper()
feedname2_CC_ONLINE_ACTIVITY = 'CC_ONLINE_ACTIVITY'.lower()

dedup_command_CC_ONLINE_ACTIVITY="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CC_ONLINE_ACTIVITY)
logging.info(dedup_command_CC_ONLINE_ACTIVITY) 

pcf_command_CC_ONLINE_ACTIVITY = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CC_ONLINE_ACTIVITY)
logging.info(pcf_command_CC_ONLINE_ACTIVITY)

PCFCheck_command_CC_ONLINE_ACTIVITY = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CC_ONLINE_ACTIVITY,run_date,sleeptime)
logging.info(PCFCheck_command_CC_ONLINE_ACTIVITY)

branchScript_CC_ONLINE_ACTIVITY = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CC_ONLINE_ACTIVITY,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CC_ONLINE_ACTIVITY)

def branch_CC_ONLINE_ACTIVITY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CC_ONLINE_ACTIVITY,feedname2_CC_ONLINE_ACTIVITY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CC_ONLINE_ACTIVITY)
    x = readcsv(filepath,feedname2_CC_ONLINE_ACTIVITY)
    logging.info('branch_CC_ONLINE_ACTIVITY' + x)
    return x

Branching_CC_ONLINE_ACTIVITY = BranchPythonOperator(
    task_id='branchid_CC_ONLINE_ACTIVITY',
    python_callable=branch_CC_ONLINE_ACTIVITY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='PCF_CC_ONLINE_ACTIVITY',
    bash_command= pcf_command_CC_ONLINE_ACTIVITY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='Dedup_CC_ONLINE_ACTIVITY',
    bash_command= dedup_command_CC_ONLINE_ACTIVITY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='Dedup2_CC_ONLINE_ACTIVITY',
    bash_command= dedup_command_CC_ONLINE_ACTIVITY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='PCFCheck_CC_ONLINE_ACTIVITY',
    bash_command=PCFCheck_command_CC_ONLINE_ACTIVITY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CC_ONLINE_ACTIVITY = EmailOperator(
    task_id='faile_CC_ONLINE_ACTIVITY',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CC_ONLINE_ACTIVITY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CC_ONLINE_ACTIVITY: PythonOperator = PythonOperator(task_id="waitForFlush_CC_ONLINE_ACTIVITY",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CC_ONLINE_ACTIVITY >> PCF_CC_ONLINE_ACTIVITY >> PCFCheck_CC_ONLINE_ACTIVITY >> delay_python_task_CC_ONLINE_ACTIVITY >> Dedup_CC_ONLINE_ACTIVITY >> Validation_End 
Branching_CC_ONLINE_ACTIVITY >> Dedup2_CC_ONLINE_ACTIVITY >> Validation_End
Branching_CC_ONLINE_ACTIVITY >> faile_CC_ONLINE_ACTIVITY
Branching_CC_ONLINE_ACTIVITY >> Validation_End


feedname_SIMREG_CLIENT_ACTIVITY_LOG = 'SIMREG_CLIENT_ACTIVITY_LOG'.upper()
feedname2_SIMREG_CLIENT_ACTIVITY_LOG = 'SIMREG_CLIENT_ACTIVITY_LOG'.lower()

dedup_command_SIMREG_CLIENT_ACTIVITY_LOG="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SIMREG_CLIENT_ACTIVITY_LOG)
logging.info(dedup_command_SIMREG_CLIENT_ACTIVITY_LOG) 

pcf_command_SIMREG_CLIENT_ACTIVITY_LOG = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SIMREG_CLIENT_ACTIVITY_LOG)
logging.info(pcf_command_SIMREG_CLIENT_ACTIVITY_LOG)

PCFCheck_command_SIMREG_CLIENT_ACTIVITY_LOG = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SIMREG_CLIENT_ACTIVITY_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_SIMREG_CLIENT_ACTIVITY_LOG)

branchScript_SIMREG_CLIENT_ACTIVITY_LOG = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SIMREG_CLIENT_ACTIVITY_LOG,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SIMREG_CLIENT_ACTIVITY_LOG)

def branch_SIMREG_CLIENT_ACTIVITY_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SIMREG_CLIENT_ACTIVITY_LOG,feedname2_SIMREG_CLIENT_ACTIVITY_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SIMREG_CLIENT_ACTIVITY_LOG)
    x = readcsv(filepath,feedname2_SIMREG_CLIENT_ACTIVITY_LOG)
    logging.info('branch_SIMREG_CLIENT_ACTIVITY_LOG' + x)
    return x

Branching_SIMREG_CLIENT_ACTIVITY_LOG = BranchPythonOperator(
    task_id='branchid_SIMREG_CLIENT_ACTIVITY_LOG',
    python_callable=branch_SIMREG_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SIMREG_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='PCF_SIMREG_CLIENT_ACTIVITY_LOG',
    bash_command= pcf_command_SIMREG_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SIMREG_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='Dedup_SIMREG_CLIENT_ACTIVITY_LOG',
    bash_command= dedup_command_SIMREG_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SIMREG_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='Dedup2_SIMREG_CLIENT_ACTIVITY_LOG',
    bash_command= dedup_command_SIMREG_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SIMREG_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='PCFCheck_SIMREG_CLIENT_ACTIVITY_LOG',
    bash_command=PCFCheck_command_SIMREG_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SIMREG_CLIENT_ACTIVITY_LOG = EmailOperator(
    task_id='faile_SIMREG_CLIENT_ACTIVITY_LOG',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SIMREG_CLIENT_ACTIVITY_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SIMREG_CLIENT_ACTIVITY_LOG: PythonOperator = PythonOperator(task_id="waitForFlush_SIMREG_CLIENT_ACTIVITY_LOG",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SIMREG_CLIENT_ACTIVITY_LOG >> PCF_SIMREG_CLIENT_ACTIVITY_LOG >> PCFCheck_SIMREG_CLIENT_ACTIVITY_LOG >> delay_python_task_SIMREG_CLIENT_ACTIVITY_LOG >> Dedup_SIMREG_CLIENT_ACTIVITY_LOG >> Validation_End 
Branching_SIMREG_CLIENT_ACTIVITY_LOG >> Dedup2_SIMREG_CLIENT_ACTIVITY_LOG >> Validation_End
Branching_SIMREG_CLIENT_ACTIVITY_LOG >> faile_SIMREG_CLIENT_ACTIVITY_LOG
Branching_SIMREG_CLIENT_ACTIVITY_LOG >> Validation_End


feedname_SPON_PROVIDER_DTLS = 'SPON_PROVIDER_DTLS'.upper()
feedname2_SPON_PROVIDER_DTLS = 'SPON_PROVIDER_DTLS'.lower()

dedup_command_SPON_PROVIDER_DTLS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SPON_PROVIDER_DTLS)
logging.info(dedup_command_SPON_PROVIDER_DTLS) 

pcf_command_SPON_PROVIDER_DTLS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SPON_PROVIDER_DTLS)
logging.info(pcf_command_SPON_PROVIDER_DTLS)

PCFCheck_command_SPON_PROVIDER_DTLS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SPON_PROVIDER_DTLS,run_date,sleeptime)
logging.info(PCFCheck_command_SPON_PROVIDER_DTLS)

branchScript_SPON_PROVIDER_DTLS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SPON_PROVIDER_DTLS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SPON_PROVIDER_DTLS)

def branch_SPON_PROVIDER_DTLS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SPON_PROVIDER_DTLS,feedname2_SPON_PROVIDER_DTLS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SPON_PROVIDER_DTLS)
    x = readcsv(filepath,feedname2_SPON_PROVIDER_DTLS)
    logging.info('branch_SPON_PROVIDER_DTLS' + x)
    return x

Branching_SPON_PROVIDER_DTLS = BranchPythonOperator(
    task_id='branchid_SPON_PROVIDER_DTLS',
    python_callable=branch_SPON_PROVIDER_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SPON_PROVIDER_DTLS = BashOperator(
    task_id='PCF_SPON_PROVIDER_DTLS',
    bash_command= pcf_command_SPON_PROVIDER_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SPON_PROVIDER_DTLS = BashOperator(
    task_id='Dedup_SPON_PROVIDER_DTLS',
    bash_command= dedup_command_SPON_PROVIDER_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SPON_PROVIDER_DTLS = BashOperator(
    task_id='Dedup2_SPON_PROVIDER_DTLS',
    bash_command= dedup_command_SPON_PROVIDER_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SPON_PROVIDER_DTLS = BashOperator(
    task_id='PCFCheck_SPON_PROVIDER_DTLS',
    bash_command=PCFCheck_command_SPON_PROVIDER_DTLS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SPON_PROVIDER_DTLS = EmailOperator(
    task_id='faile_SPON_PROVIDER_DTLS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SPON_PROVIDER_DTLS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SPON_PROVIDER_DTLS: PythonOperator = PythonOperator(task_id="waitForFlush_SPON_PROVIDER_DTLS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SPON_PROVIDER_DTLS >> PCF_SPON_PROVIDER_DTLS >> PCFCheck_SPON_PROVIDER_DTLS >> delay_python_task_SPON_PROVIDER_DTLS >> Dedup_SPON_PROVIDER_DTLS >> Validation_End 
Branching_SPON_PROVIDER_DTLS >> Dedup2_SPON_PROVIDER_DTLS >> Validation_End
Branching_SPON_PROVIDER_DTLS >> faile_SPON_PROVIDER_DTLS
Branching_SPON_PROVIDER_DTLS >> Validation_End


feedname_BIB_AGL_SUBBASE_TAB_RPT = 'BIB_AGL_SUBBASE_TAB_RPT'.upper()
feedname2_BIB_AGL_SUBBASE_TAB_RPT = 'BIB_AGL_SUBBASE_TAB_RPT'.lower()

dedup_command_BIB_AGL_SUBBASE_TAB_RPT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_BIB_AGL_SUBBASE_TAB_RPT)
logging.info(dedup_command_BIB_AGL_SUBBASE_TAB_RPT) 

pcf_command_BIB_AGL_SUBBASE_TAB_RPT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_BIB_AGL_SUBBASE_TAB_RPT)
logging.info(pcf_command_BIB_AGL_SUBBASE_TAB_RPT)

PCFCheck_command_BIB_AGL_SUBBASE_TAB_RPT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BIB_AGL_SUBBASE_TAB_RPT,run_date,sleeptime)
logging.info(PCFCheck_command_BIB_AGL_SUBBASE_TAB_RPT)

branchScript_BIB_AGL_SUBBASE_TAB_RPT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BIB_AGL_SUBBASE_TAB_RPT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BIB_AGL_SUBBASE_TAB_RPT)

def branch_BIB_AGL_SUBBASE_TAB_RPT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BIB_AGL_SUBBASE_TAB_RPT,feedname2_BIB_AGL_SUBBASE_TAB_RPT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BIB_AGL_SUBBASE_TAB_RPT)
    x = readcsv(filepath,feedname2_BIB_AGL_SUBBASE_TAB_RPT)
    logging.info('branch_BIB_AGL_SUBBASE_TAB_RPT' + x)
    return x

Branching_BIB_AGL_SUBBASE_TAB_RPT = BranchPythonOperator(
    task_id='branchid_BIB_AGL_SUBBASE_TAB_RPT',
    python_callable=branch_BIB_AGL_SUBBASE_TAB_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BIB_AGL_SUBBASE_TAB_RPT = BashOperator(
    task_id='PCF_BIB_AGL_SUBBASE_TAB_RPT',
    bash_command= pcf_command_BIB_AGL_SUBBASE_TAB_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_BIB_AGL_SUBBASE_TAB_RPT = BashOperator(
    task_id='Dedup_BIB_AGL_SUBBASE_TAB_RPT',
    bash_command= dedup_command_BIB_AGL_SUBBASE_TAB_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_BIB_AGL_SUBBASE_TAB_RPT = BashOperator(
    task_id='Dedup2_BIB_AGL_SUBBASE_TAB_RPT',
    bash_command= dedup_command_BIB_AGL_SUBBASE_TAB_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_BIB_AGL_SUBBASE_TAB_RPT = BashOperator(
    task_id='PCFCheck_BIB_AGL_SUBBASE_TAB_RPT',
    bash_command=PCFCheck_command_BIB_AGL_SUBBASE_TAB_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_BIB_AGL_SUBBASE_TAB_RPT = EmailOperator(
    task_id='faile_BIB_AGL_SUBBASE_TAB_RPT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BIB_AGL_SUBBASE_TAB_RPT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BIB_AGL_SUBBASE_TAB_RPT: PythonOperator = PythonOperator(task_id="waitForFlush_BIB_AGL_SUBBASE_TAB_RPT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BIB_AGL_SUBBASE_TAB_RPT >> PCF_BIB_AGL_SUBBASE_TAB_RPT >> PCFCheck_BIB_AGL_SUBBASE_TAB_RPT >> delay_python_task_BIB_AGL_SUBBASE_TAB_RPT >> Dedup_BIB_AGL_SUBBASE_TAB_RPT >> Validation_End 
Branching_BIB_AGL_SUBBASE_TAB_RPT >> Dedup2_BIB_AGL_SUBBASE_TAB_RPT >> Validation_End
Branching_BIB_AGL_SUBBASE_TAB_RPT >> faile_BIB_AGL_SUBBASE_TAB_RPT
Branching_BIB_AGL_SUBBASE_TAB_RPT >> Validation_End
