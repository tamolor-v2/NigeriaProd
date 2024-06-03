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
d_1 = Variable.get("rerunDate9", deserialize_json=True)
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

dag = DAG('New_Automation_Group_9', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group9 -c config.json' %(d_1)
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


feedname_AVAYA_CLID = 'AVAYA_CLID'.upper()
feedname2_AVAYA_CLID = 'AVAYA_CLID'.lower()

dedup_command_AVAYA_CLID="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AVAYA_CLID)
logging.info(dedup_command_AVAYA_CLID) 

pcf_command_AVAYA_CLID = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AVAYA_CLID)
logging.info(pcf_command_AVAYA_CLID)

PCFCheck_command_AVAYA_CLID = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AVAYA_CLID,run_date,sleeptime)
logging.info(PCFCheck_command_AVAYA_CLID)

branchScript_AVAYA_CLID = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AVAYA_CLID,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AVAYA_CLID)

def branch_AVAYA_CLID():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AVAYA_CLID,feedname2_AVAYA_CLID)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AVAYA_CLID)
    x = readcsv(filepath,feedname2_AVAYA_CLID)
    logging.info('branch_AVAYA_CLID' + x)
    return x

Branching_AVAYA_CLID = BranchPythonOperator(
    task_id='branchid_AVAYA_CLID',
    python_callable=branch_AVAYA_CLID,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AVAYA_CLID = BashOperator(
    task_id='PCF_AVAYA_CLID',
    bash_command= pcf_command_AVAYA_CLID,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AVAYA_CLID = BashOperator(
    task_id='Dedup_AVAYA_CLID',
    bash_command= dedup_command_AVAYA_CLID,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AVAYA_CLID = BashOperator(
    task_id='Dedup2_AVAYA_CLID',
    bash_command= dedup_command_AVAYA_CLID,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AVAYA_CLID = BashOperator(
    task_id='PCFCheck_AVAYA_CLID',
    bash_command=PCFCheck_command_AVAYA_CLID,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AVAYA_CLID = EmailOperator(
    task_id='faile_AVAYA_CLID',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AVAYA_CLID),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AVAYA_CLID: PythonOperator = PythonOperator(task_id="waitForFlush_AVAYA_CLID",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AVAYA_CLID >> PCF_AVAYA_CLID >> PCFCheck_AVAYA_CLID >> delay_python_task_AVAYA_CLID >> Dedup_AVAYA_CLID >> Validation_End 
Branching_AVAYA_CLID >> Dedup2_AVAYA_CLID >> Validation_End
Branching_AVAYA_CLID >> faile_AVAYA_CLID
Branching_AVAYA_CLID >> Validation_End


feedname_TAPOUT_VOICE = 'TAPOUT_VOICE'.upper()
feedname2_TAPOUT_VOICE = 'TAPOUT_VOICE'.lower()

dedup_command_TAPOUT_VOICE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAPOUT_VOICE)
logging.info(dedup_command_TAPOUT_VOICE) 

pcf_command_TAPOUT_VOICE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAPOUT_VOICE)
logging.info(pcf_command_TAPOUT_VOICE)

PCFCheck_command_TAPOUT_VOICE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAPOUT_VOICE,run_date,sleeptime)
logging.info(PCFCheck_command_TAPOUT_VOICE)

branchScript_TAPOUT_VOICE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAPOUT_VOICE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAPOUT_VOICE)

def branch_TAPOUT_VOICE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAPOUT_VOICE,feedname2_TAPOUT_VOICE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAPOUT_VOICE)
    x = readcsv(filepath,feedname2_TAPOUT_VOICE)
    logging.info('branch_TAPOUT_VOICE' + x)
    return x

Branching_TAPOUT_VOICE = BranchPythonOperator(
    task_id='branchid_TAPOUT_VOICE',
    python_callable=branch_TAPOUT_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAPOUT_VOICE = BashOperator(
    task_id='PCF_TAPOUT_VOICE',
    bash_command= pcf_command_TAPOUT_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAPOUT_VOICE = BashOperator(
    task_id='Dedup_TAPOUT_VOICE',
    bash_command= dedup_command_TAPOUT_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAPOUT_VOICE = BashOperator(
    task_id='Dedup2_TAPOUT_VOICE',
    bash_command= dedup_command_TAPOUT_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAPOUT_VOICE = BashOperator(
    task_id='PCFCheck_TAPOUT_VOICE',
    bash_command=PCFCheck_command_TAPOUT_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAPOUT_VOICE = EmailOperator(
    task_id='faile_TAPOUT_VOICE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAPOUT_VOICE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAPOUT_VOICE: PythonOperator = PythonOperator(task_id="waitForFlush_TAPOUT_VOICE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAPOUT_VOICE >> PCF_TAPOUT_VOICE >> PCFCheck_TAPOUT_VOICE >> delay_python_task_TAPOUT_VOICE >> Dedup_TAPOUT_VOICE >> Validation_End 
Branching_TAPOUT_VOICE >> Dedup2_TAPOUT_VOICE >> Validation_End
Branching_TAPOUT_VOICE >> faile_TAPOUT_VOICE
Branching_TAPOUT_VOICE >> Validation_End


feedname_TAPOUT_GPRS = 'TAPOUT_GPRS'.upper()
feedname2_TAPOUT_GPRS = 'TAPOUT_GPRS'.lower()

dedup_command_TAPOUT_GPRS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAPOUT_GPRS)
logging.info(dedup_command_TAPOUT_GPRS) 

pcf_command_TAPOUT_GPRS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAPOUT_GPRS)
logging.info(pcf_command_TAPOUT_GPRS)

PCFCheck_command_TAPOUT_GPRS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAPOUT_GPRS,run_date,sleeptime)
logging.info(PCFCheck_command_TAPOUT_GPRS)

branchScript_TAPOUT_GPRS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAPOUT_GPRS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAPOUT_GPRS)

def branch_TAPOUT_GPRS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAPOUT_GPRS,feedname2_TAPOUT_GPRS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAPOUT_GPRS)
    x = readcsv(filepath,feedname2_TAPOUT_GPRS)
    logging.info('branch_TAPOUT_GPRS' + x)
    return x

Branching_TAPOUT_GPRS = BranchPythonOperator(
    task_id='branchid_TAPOUT_GPRS',
    python_callable=branch_TAPOUT_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAPOUT_GPRS = BashOperator(
    task_id='PCF_TAPOUT_GPRS',
    bash_command= pcf_command_TAPOUT_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAPOUT_GPRS = BashOperator(
    task_id='Dedup_TAPOUT_GPRS',
    bash_command= dedup_command_TAPOUT_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAPOUT_GPRS = BashOperator(
    task_id='Dedup2_TAPOUT_GPRS',
    bash_command= dedup_command_TAPOUT_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAPOUT_GPRS = BashOperator(
    task_id='PCFCheck_TAPOUT_GPRS',
    bash_command=PCFCheck_command_TAPOUT_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAPOUT_GPRS = EmailOperator(
    task_id='faile_TAPOUT_GPRS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAPOUT_GPRS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAPOUT_GPRS: PythonOperator = PythonOperator(task_id="waitForFlush_TAPOUT_GPRS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAPOUT_GPRS >> PCF_TAPOUT_GPRS >> PCFCheck_TAPOUT_GPRS >> delay_python_task_TAPOUT_GPRS >> Dedup_TAPOUT_GPRS >> Validation_End 
Branching_TAPOUT_GPRS >> Dedup2_TAPOUT_GPRS >> Validation_End
Branching_TAPOUT_GPRS >> faile_TAPOUT_GPRS
Branching_TAPOUT_GPRS >> Validation_End


feedname_CLM_SHOP_REQUESTS = 'CLM_SHOP_REQUESTS'.upper()
feedname2_CLM_SHOP_REQUESTS = 'CLM_SHOP_REQUESTS'.lower()

dedup_command_CLM_SHOP_REQUESTS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CLM_SHOP_REQUESTS)
logging.info(dedup_command_CLM_SHOP_REQUESTS) 

pcf_command_CLM_SHOP_REQUESTS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CLM_SHOP_REQUESTS)
logging.info(pcf_command_CLM_SHOP_REQUESTS)

PCFCheck_command_CLM_SHOP_REQUESTS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CLM_SHOP_REQUESTS,run_date,sleeptime)
logging.info(PCFCheck_command_CLM_SHOP_REQUESTS)

branchScript_CLM_SHOP_REQUESTS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CLM_SHOP_REQUESTS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CLM_SHOP_REQUESTS)

def branch_CLM_SHOP_REQUESTS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CLM_SHOP_REQUESTS,feedname2_CLM_SHOP_REQUESTS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CLM_SHOP_REQUESTS)
    x = readcsv(filepath,feedname2_CLM_SHOP_REQUESTS)
    logging.info('branch_CLM_SHOP_REQUESTS' + x)
    return x

Branching_CLM_SHOP_REQUESTS = BranchPythonOperator(
    task_id='branchid_CLM_SHOP_REQUESTS',
    python_callable=branch_CLM_SHOP_REQUESTS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CLM_SHOP_REQUESTS = BashOperator(
    task_id='PCF_CLM_SHOP_REQUESTS',
    bash_command= pcf_command_CLM_SHOP_REQUESTS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CLM_SHOP_REQUESTS = BashOperator(
    task_id='Dedup_CLM_SHOP_REQUESTS',
    bash_command= dedup_command_CLM_SHOP_REQUESTS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CLM_SHOP_REQUESTS = BashOperator(
    task_id='Dedup2_CLM_SHOP_REQUESTS',
    bash_command= dedup_command_CLM_SHOP_REQUESTS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CLM_SHOP_REQUESTS = BashOperator(
    task_id='PCFCheck_CLM_SHOP_REQUESTS',
    bash_command=PCFCheck_command_CLM_SHOP_REQUESTS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CLM_SHOP_REQUESTS = EmailOperator(
    task_id='faile_CLM_SHOP_REQUESTS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CLM_SHOP_REQUESTS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CLM_SHOP_REQUESTS: PythonOperator = PythonOperator(task_id="waitForFlush_CLM_SHOP_REQUESTS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CLM_SHOP_REQUESTS >> PCF_CLM_SHOP_REQUESTS >> PCFCheck_CLM_SHOP_REQUESTS >> delay_python_task_CLM_SHOP_REQUESTS >> Dedup_CLM_SHOP_REQUESTS >> Validation_End 
Branching_CLM_SHOP_REQUESTS >> Dedup2_CLM_SHOP_REQUESTS >> Validation_End
Branching_CLM_SHOP_REQUESTS >> faile_CLM_SHOP_REQUESTS
Branching_CLM_SHOP_REQUESTS >> Validation_End


feedname_SMART_APP_NEW_USERS = 'SMART_APP_NEW_USERS'.upper()
feedname2_SMART_APP_NEW_USERS = 'SMART_APP_NEW_USERS'.lower()

dedup_command_SMART_APP_NEW_USERS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SMART_APP_NEW_USERS)
logging.info(dedup_command_SMART_APP_NEW_USERS) 

pcf_command_SMART_APP_NEW_USERS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SMART_APP_NEW_USERS)
logging.info(pcf_command_SMART_APP_NEW_USERS)

PCFCheck_command_SMART_APP_NEW_USERS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMART_APP_NEW_USERS,run_date,sleeptime)
logging.info(PCFCheck_command_SMART_APP_NEW_USERS)

branchScript_SMART_APP_NEW_USERS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SMART_APP_NEW_USERS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SMART_APP_NEW_USERS)

def branch_SMART_APP_NEW_USERS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMART_APP_NEW_USERS,feedname2_SMART_APP_NEW_USERS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMART_APP_NEW_USERS)
    x = readcsv(filepath,feedname2_SMART_APP_NEW_USERS)
    logging.info('branch_SMART_APP_NEW_USERS' + x)
    return x

Branching_SMART_APP_NEW_USERS = BranchPythonOperator(
    task_id='branchid_SMART_APP_NEW_USERS',
    python_callable=branch_SMART_APP_NEW_USERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMART_APP_NEW_USERS = BashOperator(
    task_id='PCF_SMART_APP_NEW_USERS',
    bash_command= pcf_command_SMART_APP_NEW_USERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SMART_APP_NEW_USERS = BashOperator(
    task_id='Dedup_SMART_APP_NEW_USERS',
    bash_command= dedup_command_SMART_APP_NEW_USERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SMART_APP_NEW_USERS = BashOperator(
    task_id='Dedup2_SMART_APP_NEW_USERS',
    bash_command= dedup_command_SMART_APP_NEW_USERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SMART_APP_NEW_USERS = BashOperator(
    task_id='PCFCheck_SMART_APP_NEW_USERS',
    bash_command=PCFCheck_command_SMART_APP_NEW_USERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SMART_APP_NEW_USERS = EmailOperator(
    task_id='faile_SMART_APP_NEW_USERS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMART_APP_NEW_USERS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SMART_APP_NEW_USERS: PythonOperator = PythonOperator(task_id="waitForFlush_SMART_APP_NEW_USERS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SMART_APP_NEW_USERS >> PCF_SMART_APP_NEW_USERS >> PCFCheck_SMART_APP_NEW_USERS >> delay_python_task_SMART_APP_NEW_USERS >> Dedup_SMART_APP_NEW_USERS >> Validation_End 
Branching_SMART_APP_NEW_USERS >> Dedup2_SMART_APP_NEW_USERS >> Validation_End
Branching_SMART_APP_NEW_USERS >> faile_SMART_APP_NEW_USERS
Branching_SMART_APP_NEW_USERS >> Validation_End


feedname_AUDIT_LOGS = 'AUDIT_LOGS'.upper()
feedname2_AUDIT_LOGS = 'AUDIT_LOGS'.lower()

dedup_command_AUDIT_LOGS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AUDIT_LOGS)
logging.info(dedup_command_AUDIT_LOGS) 

pcf_command_AUDIT_LOGS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AUDIT_LOGS)
logging.info(pcf_command_AUDIT_LOGS)

PCFCheck_command_AUDIT_LOGS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AUDIT_LOGS,run_date,sleeptime)
logging.info(PCFCheck_command_AUDIT_LOGS)

branchScript_AUDIT_LOGS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AUDIT_LOGS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AUDIT_LOGS)

def branch_AUDIT_LOGS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AUDIT_LOGS,feedname2_AUDIT_LOGS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AUDIT_LOGS)
    x = readcsv(filepath,feedname2_AUDIT_LOGS)
    logging.info('branch_AUDIT_LOGS' + x)
    return x

Branching_AUDIT_LOGS = BranchPythonOperator(
    task_id='branchid_AUDIT_LOGS',
    python_callable=branch_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AUDIT_LOGS = BashOperator(
    task_id='PCF_AUDIT_LOGS',
    bash_command= pcf_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AUDIT_LOGS = BashOperator(
    task_id='Dedup_AUDIT_LOGS',
    bash_command= dedup_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AUDIT_LOGS = BashOperator(
    task_id='Dedup2_AUDIT_LOGS',
    bash_command= dedup_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AUDIT_LOGS = BashOperator(
    task_id='PCFCheck_AUDIT_LOGS',
    bash_command=PCFCheck_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AUDIT_LOGS = EmailOperator(
    task_id='faile_AUDIT_LOGS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AUDIT_LOGS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AUDIT_LOGS: PythonOperator = PythonOperator(task_id="waitForFlush_AUDIT_LOGS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AUDIT_LOGS >> PCF_AUDIT_LOGS >> PCFCheck_AUDIT_LOGS >> delay_python_task_AUDIT_LOGS >> Dedup_AUDIT_LOGS >> Validation_End 
Branching_AUDIT_LOGS >> Dedup2_AUDIT_LOGS >> Validation_End
Branching_AUDIT_LOGS >> faile_AUDIT_LOGS
Branching_AUDIT_LOGS >> Validation_End


feedname_FINANCIAL_LOG = 'FINANCIAL_LOG'.upper()
feedname2_FINANCIAL_LOG = 'FINANCIAL_LOG'.lower()

dedup_command_FINANCIAL_LOG="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_FINANCIAL_LOG)
logging.info(dedup_command_FINANCIAL_LOG) 

pcf_command_FINANCIAL_LOG = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_FINANCIAL_LOG)
logging.info(pcf_command_FINANCIAL_LOG)

PCFCheck_command_FINANCIAL_LOG = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_FINANCIAL_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_FINANCIAL_LOG)

branchScript_FINANCIAL_LOG = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_FINANCIAL_LOG,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_FINANCIAL_LOG)

def branch_FINANCIAL_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_FINANCIAL_LOG,feedname2_FINANCIAL_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_FINANCIAL_LOG)
    x = readcsv(filepath,feedname2_FINANCIAL_LOG)
    logging.info('branch_FINANCIAL_LOG' + x)
    return x

Branching_FINANCIAL_LOG = BranchPythonOperator(
    task_id='branchid_FINANCIAL_LOG',
    python_callable=branch_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_FINANCIAL_LOG = BashOperator(
    task_id='PCF_FINANCIAL_LOG',
    bash_command= pcf_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_FINANCIAL_LOG = BashOperator(
    task_id='Dedup_FINANCIAL_LOG',
    bash_command= dedup_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_FINANCIAL_LOG = BashOperator(
    task_id='Dedup2_FINANCIAL_LOG',
    bash_command= dedup_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_FINANCIAL_LOG = BashOperator(
    task_id='PCFCheck_FINANCIAL_LOG',
    bash_command=PCFCheck_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_FINANCIAL_LOG = EmailOperator(
    task_id='faile_FINANCIAL_LOG',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_FINANCIAL_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_FINANCIAL_LOG: PythonOperator = PythonOperator(task_id="waitForFlush_FINANCIAL_LOG",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_FINANCIAL_LOG >> PCF_FINANCIAL_LOG >> PCFCheck_FINANCIAL_LOG >> delay_python_task_FINANCIAL_LOG >> Dedup_FINANCIAL_LOG >> Validation_End 
Branching_FINANCIAL_LOG >> Dedup2_FINANCIAL_LOG >> Validation_End
Branching_FINANCIAL_LOG >> faile_FINANCIAL_LOG
Branching_FINANCIAL_LOG >> Validation_End


feedname_WBS_REPORT = 'WBS_REPORT'.upper()
feedname2_WBS_REPORT = 'WBS_REPORT'.lower()

dedup_command_WBS_REPORT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_WBS_REPORT)
logging.info(dedup_command_WBS_REPORT) 

pcf_command_WBS_REPORT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_WBS_REPORT)
logging.info(pcf_command_WBS_REPORT)

PCFCheck_command_WBS_REPORT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_WBS_REPORT,run_date,sleeptime)
logging.info(PCFCheck_command_WBS_REPORT)

branchScript_WBS_REPORT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_WBS_REPORT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_WBS_REPORT)

def branch_WBS_REPORT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_WBS_REPORT,feedname2_WBS_REPORT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_WBS_REPORT)
    x = readcsv(filepath,feedname2_WBS_REPORT)
    logging.info('branch_WBS_REPORT' + x)
    return x

Branching_WBS_REPORT = BranchPythonOperator(
    task_id='branchid_WBS_REPORT',
    python_callable=branch_WBS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_WBS_REPORT = BashOperator(
    task_id='PCF_WBS_REPORT',
    bash_command= pcf_command_WBS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_WBS_REPORT = BashOperator(
    task_id='Dedup_WBS_REPORT',
    bash_command= dedup_command_WBS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_WBS_REPORT = BashOperator(
    task_id='Dedup2_WBS_REPORT',
    bash_command= dedup_command_WBS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_WBS_REPORT = BashOperator(
    task_id='PCFCheck_WBS_REPORT',
    bash_command=PCFCheck_command_WBS_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_WBS_REPORT = EmailOperator(
    task_id='faile_WBS_REPORT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_WBS_REPORT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_WBS_REPORT: PythonOperator = PythonOperator(task_id="waitForFlush_WBS_REPORT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_WBS_REPORT >> PCF_WBS_REPORT >> PCFCheck_WBS_REPORT >> delay_python_task_WBS_REPORT >> Dedup_WBS_REPORT >> Validation_End 
Branching_WBS_REPORT >> Dedup2_WBS_REPORT >> Validation_End
Branching_WBS_REPORT >> faile_WBS_REPORT
Branching_WBS_REPORT >> Validation_End


feedname_PAYMENT_GATEWAY_AIRTIME = 'PAYMENT_GATEWAY_AIRTIME'.upper()
feedname2_PAYMENT_GATEWAY_AIRTIME = 'PAYMENT_GATEWAY_AIRTIME'.lower()

dedup_command_PAYMENT_GATEWAY_AIRTIME="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_PAYMENT_GATEWAY_AIRTIME)
logging.info(dedup_command_PAYMENT_GATEWAY_AIRTIME) 

pcf_command_PAYMENT_GATEWAY_AIRTIME = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_PAYMENT_GATEWAY_AIRTIME)
logging.info(pcf_command_PAYMENT_GATEWAY_AIRTIME)

PCFCheck_command_PAYMENT_GATEWAY_AIRTIME = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_PAYMENT_GATEWAY_AIRTIME,run_date,sleeptime)
logging.info(PCFCheck_command_PAYMENT_GATEWAY_AIRTIME)

branchScript_PAYMENT_GATEWAY_AIRTIME = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_PAYMENT_GATEWAY_AIRTIME,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_PAYMENT_GATEWAY_AIRTIME)

def branch_PAYMENT_GATEWAY_AIRTIME():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_PAYMENT_GATEWAY_AIRTIME,feedname2_PAYMENT_GATEWAY_AIRTIME)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_PAYMENT_GATEWAY_AIRTIME)
    x = readcsv(filepath,feedname2_PAYMENT_GATEWAY_AIRTIME)
    logging.info('branch_PAYMENT_GATEWAY_AIRTIME' + x)
    return x

Branching_PAYMENT_GATEWAY_AIRTIME = BranchPythonOperator(
    task_id='branchid_PAYMENT_GATEWAY_AIRTIME',
    python_callable=branch_PAYMENT_GATEWAY_AIRTIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='PCF_PAYMENT_GATEWAY_AIRTIME',
    bash_command= pcf_command_PAYMENT_GATEWAY_AIRTIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='Dedup_PAYMENT_GATEWAY_AIRTIME',
    bash_command= dedup_command_PAYMENT_GATEWAY_AIRTIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='Dedup2_PAYMENT_GATEWAY_AIRTIME',
    bash_command= dedup_command_PAYMENT_GATEWAY_AIRTIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='PCFCheck_PAYMENT_GATEWAY_AIRTIME',
    bash_command=PCFCheck_command_PAYMENT_GATEWAY_AIRTIME,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_PAYMENT_GATEWAY_AIRTIME = EmailOperator(
    task_id='faile_PAYMENT_GATEWAY_AIRTIME',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_PAYMENT_GATEWAY_AIRTIME),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_PAYMENT_GATEWAY_AIRTIME: PythonOperator = PythonOperator(task_id="waitForFlush_PAYMENT_GATEWAY_AIRTIME",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_PAYMENT_GATEWAY_AIRTIME >> PCF_PAYMENT_GATEWAY_AIRTIME >> PCFCheck_PAYMENT_GATEWAY_AIRTIME >> delay_python_task_PAYMENT_GATEWAY_AIRTIME >> Dedup_PAYMENT_GATEWAY_AIRTIME >> Validation_End 
Branching_PAYMENT_GATEWAY_AIRTIME >> Dedup2_PAYMENT_GATEWAY_AIRTIME >> Validation_End
Branching_PAYMENT_GATEWAY_AIRTIME >> faile_PAYMENT_GATEWAY_AIRTIME
Branching_PAYMENT_GATEWAY_AIRTIME >> Validation_End


feedname_CB_POS_TRANSACTIONS = 'CB_POS_TRANSACTIONS'.upper()
feedname2_CB_POS_TRANSACTIONS = 'CB_POS_TRANSACTIONS'.lower()

dedup_command_CB_POS_TRANSACTIONS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CB_POS_TRANSACTIONS)
logging.info(dedup_command_CB_POS_TRANSACTIONS) 

pcf_command_CB_POS_TRANSACTIONS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CB_POS_TRANSACTIONS)
logging.info(pcf_command_CB_POS_TRANSACTIONS)

PCFCheck_command_CB_POS_TRANSACTIONS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_POS_TRANSACTIONS,run_date,sleeptime)
logging.info(PCFCheck_command_CB_POS_TRANSACTIONS)

branchScript_CB_POS_TRANSACTIONS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_POS_TRANSACTIONS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_POS_TRANSACTIONS)

def branch_CB_POS_TRANSACTIONS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_POS_TRANSACTIONS,feedname2_CB_POS_TRANSACTIONS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_POS_TRANSACTIONS)
    x = readcsv(filepath,feedname2_CB_POS_TRANSACTIONS)
    logging.info('branch_CB_POS_TRANSACTIONS' + x)
    return x

Branching_CB_POS_TRANSACTIONS = BranchPythonOperator(
    task_id='branchid_CB_POS_TRANSACTIONS',
    python_callable=branch_CB_POS_TRANSACTIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CB_POS_TRANSACTIONS = BashOperator(
    task_id='PCF_CB_POS_TRANSACTIONS',
    bash_command= pcf_command_CB_POS_TRANSACTIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CB_POS_TRANSACTIONS = BashOperator(
    task_id='Dedup_CB_POS_TRANSACTIONS',
    bash_command= dedup_command_CB_POS_TRANSACTIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CB_POS_TRANSACTIONS = BashOperator(
    task_id='Dedup2_CB_POS_TRANSACTIONS',
    bash_command= dedup_command_CB_POS_TRANSACTIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CB_POS_TRANSACTIONS = BashOperator(
    task_id='PCFCheck_CB_POS_TRANSACTIONS',
    bash_command=PCFCheck_command_CB_POS_TRANSACTIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CB_POS_TRANSACTIONS = EmailOperator(
    task_id='faile_CB_POS_TRANSACTIONS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_POS_TRANSACTIONS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CB_POS_TRANSACTIONS: PythonOperator = PythonOperator(task_id="waitForFlush_CB_POS_TRANSACTIONS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_POS_TRANSACTIONS >> PCF_CB_POS_TRANSACTIONS >> PCFCheck_CB_POS_TRANSACTIONS >> delay_python_task_CB_POS_TRANSACTIONS >> Dedup_CB_POS_TRANSACTIONS >> Validation_End 
Branching_CB_POS_TRANSACTIONS >> Dedup2_CB_POS_TRANSACTIONS >> Validation_End
Branching_CB_POS_TRANSACTIONS >> faile_CB_POS_TRANSACTIONS
Branching_CB_POS_TRANSACTIONS >> Validation_End


feedname_CB_RETAIL_OUTLETS = 'CB_RETAIL_OUTLETS'.upper()
feedname2_CB_RETAIL_OUTLETS = 'CB_RETAIL_OUTLETS'.lower()

dedup_command_CB_RETAIL_OUTLETS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CB_RETAIL_OUTLETS)
logging.info(dedup_command_CB_RETAIL_OUTLETS) 

pcf_command_CB_RETAIL_OUTLETS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CB_RETAIL_OUTLETS)
logging.info(pcf_command_CB_RETAIL_OUTLETS)

PCFCheck_command_CB_RETAIL_OUTLETS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_RETAIL_OUTLETS,run_date,sleeptime)
logging.info(PCFCheck_command_CB_RETAIL_OUTLETS)

branchScript_CB_RETAIL_OUTLETS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_RETAIL_OUTLETS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_RETAIL_OUTLETS)

def branch_CB_RETAIL_OUTLETS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_RETAIL_OUTLETS,feedname2_CB_RETAIL_OUTLETS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_RETAIL_OUTLETS)
    x = readcsv(filepath,feedname2_CB_RETAIL_OUTLETS)
    logging.info('branch_CB_RETAIL_OUTLETS' + x)
    return x

Branching_CB_RETAIL_OUTLETS = BranchPythonOperator(
    task_id='branchid_CB_RETAIL_OUTLETS',
    python_callable=branch_CB_RETAIL_OUTLETS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CB_RETAIL_OUTLETS = BashOperator(
    task_id='PCF_CB_RETAIL_OUTLETS',
    bash_command= pcf_command_CB_RETAIL_OUTLETS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CB_RETAIL_OUTLETS = BashOperator(
    task_id='Dedup_CB_RETAIL_OUTLETS',
    bash_command= dedup_command_CB_RETAIL_OUTLETS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CB_RETAIL_OUTLETS = BashOperator(
    task_id='Dedup2_CB_RETAIL_OUTLETS',
    bash_command= dedup_command_CB_RETAIL_OUTLETS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CB_RETAIL_OUTLETS = BashOperator(
    task_id='PCFCheck_CB_RETAIL_OUTLETS',
    bash_command=PCFCheck_command_CB_RETAIL_OUTLETS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CB_RETAIL_OUTLETS = EmailOperator(
    task_id='faile_CB_RETAIL_OUTLETS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_RETAIL_OUTLETS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CB_RETAIL_OUTLETS: PythonOperator = PythonOperator(task_id="waitForFlush_CB_RETAIL_OUTLETS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_RETAIL_OUTLETS >> PCF_CB_RETAIL_OUTLETS >> PCFCheck_CB_RETAIL_OUTLETS >> delay_python_task_CB_RETAIL_OUTLETS >> Dedup_CB_RETAIL_OUTLETS >> Validation_End 
Branching_CB_RETAIL_OUTLETS >> Dedup2_CB_RETAIL_OUTLETS >> Validation_End
Branching_CB_RETAIL_OUTLETS >> faile_CB_RETAIL_OUTLETS
Branching_CB_RETAIL_OUTLETS >> Validation_End


feedname_AVAYA_IVR = 'AVAYA_IVR'.upper()
feedname2_AVAYA_IVR = 'AVAYA_IVR'.lower()

dedup_command_AVAYA_IVR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AVAYA_IVR)
logging.info(dedup_command_AVAYA_IVR) 

pcf_command_AVAYA_IVR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_AVAYA_IVR)
logging.info(pcf_command_AVAYA_IVR)

PCFCheck_command_AVAYA_IVR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AVAYA_IVR,run_date,sleeptime)
logging.info(PCFCheck_command_AVAYA_IVR)

branchScript_AVAYA_IVR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AVAYA_IVR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AVAYA_IVR)

def branch_AVAYA_IVR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AVAYA_IVR,feedname2_AVAYA_IVR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AVAYA_IVR)
    x = readcsv(filepath,feedname2_AVAYA_IVR)
    logging.info('branch_AVAYA_IVR' + x)
    return x

Branching_AVAYA_IVR = BranchPythonOperator(
    task_id='branchid_AVAYA_IVR',
    python_callable=branch_AVAYA_IVR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AVAYA_IVR = BashOperator(
    task_id='PCF_AVAYA_IVR',
    bash_command= pcf_command_AVAYA_IVR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AVAYA_IVR = BashOperator(
    task_id='Dedup_AVAYA_IVR',
    bash_command= dedup_command_AVAYA_IVR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AVAYA_IVR = BashOperator(
    task_id='Dedup2_AVAYA_IVR',
    bash_command= dedup_command_AVAYA_IVR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AVAYA_IVR = BashOperator(
    task_id='PCFCheck_AVAYA_IVR',
    bash_command=PCFCheck_command_AVAYA_IVR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_AVAYA_IVR = EmailOperator(
    task_id='faile_AVAYA_IVR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AVAYA_IVR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AVAYA_IVR: PythonOperator = PythonOperator(task_id="waitForFlush_AVAYA_IVR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AVAYA_IVR >> PCF_AVAYA_IVR >> PCFCheck_AVAYA_IVR >> delay_python_task_AVAYA_IVR >> Dedup_AVAYA_IVR >> Validation_End 
Branching_AVAYA_IVR >> Dedup2_AVAYA_IVR >> Validation_End
Branching_AVAYA_IVR >> faile_AVAYA_IVR
Branching_AVAYA_IVR >> Validation_End
