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
d_1 = Variable.get("rerunDate4", deserialize_json=True)
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

dag = DAG('New_Automation_Group_4', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group4 -c config.json' %(d_1)
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


feedname_MNP = 'MNP'.upper()
feedname2_MNP = 'MNP'.lower()

dedup_command_MNP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MNP)
logging.info(dedup_command_MNP) 

pcf_command_MNP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MNP)
logging.info(pcf_command_MNP)

PCFCheck_command_MNP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MNP,run_date,sleeptime)
logging.info(PCFCheck_command_MNP)

branchScript_MNP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MNP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MNP)

def branch_MNP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MNP,feedname2_MNP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MNP)
    x = readcsv(filepath,feedname2_MNP)
    logging.info('branch_MNP' + x)
    return x

Branching_MNP = BranchPythonOperator(
    task_id='branchid_MNP',
    python_callable=branch_MNP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MNP = BashOperator(
    task_id='PCF_MNP',
    bash_command= pcf_command_MNP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MNP = BashOperator(
    task_id='Dedup_MNP',
    bash_command= dedup_command_MNP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MNP = BashOperator(
    task_id='Dedup2_MNP',
    bash_command= dedup_command_MNP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MNP = BashOperator(
    task_id='PCFCheck_MNP',
    bash_command=PCFCheck_command_MNP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MNP = EmailOperator(
    task_id='faile_MNP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MNP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MNP: PythonOperator = PythonOperator(task_id="waitForFlush_MNP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MNP >> PCF_MNP >> PCFCheck_MNP >> delay_python_task_MNP >> Dedup_MNP >> Validation_End 
Branching_MNP >> Dedup2_MNP >> Validation_End
Branching_MNP >> faile_MNP
Branching_MNP >> Validation_End


feedname_AUTO_LOGGING = 'AUTO_LOGGING'.upper()
feedname2_AUTO_LOGGING = 'AUTO_LOGGING'.lower()

dedup_command_AUTO_LOGGING="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AUTO_LOGGING)
logging.info(dedup_command_AUTO_LOGGING) 

pcf_command_AUTO_LOGGING = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AUTO_LOGGING)
logging.info(pcf_command_AUTO_LOGGING)

PCFCheck_command_AUTO_LOGGING = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AUTO_LOGGING,run_date,sleeptime)
logging.info(PCFCheck_command_AUTO_LOGGING)

branchScript_AUTO_LOGGING = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AUTO_LOGGING,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AUTO_LOGGING)

def branch_AUTO_LOGGING():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AUTO_LOGGING,feedname2_AUTO_LOGGING)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AUTO_LOGGING)
    x = readcsv(filepath,feedname2_AUTO_LOGGING)
    logging.info('branch_AUTO_LOGGING' + x)
    return x

Branching_AUTO_LOGGING = BranchPythonOperator(
    task_id='branchid_AUTO_LOGGING',
    python_callable=branch_AUTO_LOGGING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AUTO_LOGGING = BashOperator(
    task_id='PCF_AUTO_LOGGING',
    bash_command= pcf_command_AUTO_LOGGING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AUTO_LOGGING = BashOperator(
    task_id='Dedup_AUTO_LOGGING',
    bash_command= dedup_command_AUTO_LOGGING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AUTO_LOGGING = BashOperator(
    task_id='Dedup2_AUTO_LOGGING',
    bash_command= dedup_command_AUTO_LOGGING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AUTO_LOGGING = BashOperator(
    task_id='PCFCheck_AUTO_LOGGING',
    bash_command=PCFCheck_command_AUTO_LOGGING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AUTO_LOGGING = EmailOperator(
    task_id='faile_AUTO_LOGGING',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AUTO_LOGGING),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AUTO_LOGGING: PythonOperator = PythonOperator(task_id="waitForFlush_AUTO_LOGGING",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AUTO_LOGGING >> PCF_AUTO_LOGGING >> PCFCheck_AUTO_LOGGING >> delay_python_task_AUTO_LOGGING >> Dedup_AUTO_LOGGING >> Validation_End 
Branching_AUTO_LOGGING >> Dedup2_AUTO_LOGGING >> Validation_End
Branching_AUTO_LOGGING >> faile_AUTO_LOGGING
Branching_AUTO_LOGGING >> Validation_End


feedname_BIOSMART_NIN_REPORT = 'BIOSMART_NIN_REPORT'.upper()
feedname2_BIOSMART_NIN_REPORT = 'BIOSMART_NIN_REPORT'.lower()

dedup_command_BIOSMART_NIN_REPORT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_BIOSMART_NIN_REPORT)
logging.info(dedup_command_BIOSMART_NIN_REPORT) 

pcf_command_BIOSMART_NIN_REPORT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_BIOSMART_NIN_REPORT)
logging.info(pcf_command_BIOSMART_NIN_REPORT)

PCFCheck_command_BIOSMART_NIN_REPORT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BIOSMART_NIN_REPORT,run_date,sleeptime)
logging.info(PCFCheck_command_BIOSMART_NIN_REPORT)

branchScript_BIOSMART_NIN_REPORT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BIOSMART_NIN_REPORT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BIOSMART_NIN_REPORT)

def branch_BIOSMART_NIN_REPORT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BIOSMART_NIN_REPORT,feedname2_BIOSMART_NIN_REPORT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BIOSMART_NIN_REPORT)
    x = readcsv(filepath,feedname2_BIOSMART_NIN_REPORT)
    logging.info('branch_BIOSMART_NIN_REPORT' + x)
    return x

Branching_BIOSMART_NIN_REPORT = BranchPythonOperator(
    task_id='branchid_BIOSMART_NIN_REPORT',
    python_callable=branch_BIOSMART_NIN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BIOSMART_NIN_REPORT = BashOperator(
    task_id='PCF_BIOSMART_NIN_REPORT',
    bash_command= pcf_command_BIOSMART_NIN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_BIOSMART_NIN_REPORT = BashOperator(
    task_id='Dedup_BIOSMART_NIN_REPORT',
    bash_command= dedup_command_BIOSMART_NIN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_BIOSMART_NIN_REPORT = BashOperator(
    task_id='Dedup2_BIOSMART_NIN_REPORT',
    bash_command= dedup_command_BIOSMART_NIN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_BIOSMART_NIN_REPORT = BashOperator(
    task_id='PCFCheck_BIOSMART_NIN_REPORT',
    bash_command=PCFCheck_command_BIOSMART_NIN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_BIOSMART_NIN_REPORT = EmailOperator(
    task_id='faile_BIOSMART_NIN_REPORT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BIOSMART_NIN_REPORT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BIOSMART_NIN_REPORT: PythonOperator = PythonOperator(task_id="waitForFlush_BIOSMART_NIN_REPORT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BIOSMART_NIN_REPORT >> PCF_BIOSMART_NIN_REPORT >> PCFCheck_BIOSMART_NIN_REPORT >> delay_python_task_BIOSMART_NIN_REPORT >> Dedup_BIOSMART_NIN_REPORT >> Validation_End 
Branching_BIOSMART_NIN_REPORT >> Dedup2_BIOSMART_NIN_REPORT >> Validation_End
Branching_BIOSMART_NIN_REPORT >> faile_BIOSMART_NIN_REPORT
Branching_BIOSMART_NIN_REPORT >> Validation_End


feedname_COMPENSATION = 'COMPENSATION'.upper()
feedname2_COMPENSATION = 'COMPENSATION'.lower()

dedup_command_COMPENSATION="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_COMPENSATION)
logging.info(dedup_command_COMPENSATION) 

pcf_command_COMPENSATION = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_COMPENSATION)
logging.info(pcf_command_COMPENSATION)

PCFCheck_command_COMPENSATION = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_COMPENSATION,run_date,sleeptime)
logging.info(PCFCheck_command_COMPENSATION)

branchScript_COMPENSATION = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_COMPENSATION,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_COMPENSATION)

def branch_COMPENSATION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_COMPENSATION,feedname2_COMPENSATION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_COMPENSATION)
    x = readcsv(filepath,feedname2_COMPENSATION)
    logging.info('branch_COMPENSATION' + x)
    return x

Branching_COMPENSATION = BranchPythonOperator(
    task_id='branchid_COMPENSATION',
    python_callable=branch_COMPENSATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_COMPENSATION = BashOperator(
    task_id='PCF_COMPENSATION',
    bash_command= pcf_command_COMPENSATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_COMPENSATION = BashOperator(
    task_id='Dedup_COMPENSATION',
    bash_command= dedup_command_COMPENSATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_COMPENSATION = BashOperator(
    task_id='Dedup2_COMPENSATION',
    bash_command= dedup_command_COMPENSATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_COMPENSATION = BashOperator(
    task_id='PCFCheck_COMPENSATION',
    bash_command=PCFCheck_command_COMPENSATION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_COMPENSATION = EmailOperator(
    task_id='faile_COMPENSATION',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_COMPENSATION),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_COMPENSATION: PythonOperator = PythonOperator(task_id="waitForFlush_COMPENSATION",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_COMPENSATION >> PCF_COMPENSATION >> PCFCheck_COMPENSATION >> delay_python_task_COMPENSATION >> Dedup_COMPENSATION >> Validation_End 
Branching_COMPENSATION >> Dedup2_COMPENSATION >> Validation_End
Branching_COMPENSATION >> faile_COMPENSATION
Branching_COMPENSATION >> Validation_End


feedname_SUBSCRIBER_TRANSACTIONS_CDR = 'SUBSCRIBER_TRANSACTIONS_CDR'.upper()
feedname2_SUBSCRIBER_TRANSACTIONS_CDR = 'SUBSCRIBER_TRANSACTIONS_CDR'.lower()

dedup_command_SUBSCRIBER_TRANSACTIONS_CDR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SUBSCRIBER_TRANSACTIONS_CDR)
logging.info(dedup_command_SUBSCRIBER_TRANSACTIONS_CDR) 

pcf_command_SUBSCRIBER_TRANSACTIONS_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SUBSCRIBER_TRANSACTIONS_CDR)
logging.info(pcf_command_SUBSCRIBER_TRANSACTIONS_CDR)

PCFCheck_command_SUBSCRIBER_TRANSACTIONS_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SUBSCRIBER_TRANSACTIONS_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_SUBSCRIBER_TRANSACTIONS_CDR)

branchScript_SUBSCRIBER_TRANSACTIONS_CDR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SUBSCRIBER_TRANSACTIONS_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SUBSCRIBER_TRANSACTIONS_CDR)

def branch_SUBSCRIBER_TRANSACTIONS_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SUBSCRIBER_TRANSACTIONS_CDR,feedname2_SUBSCRIBER_TRANSACTIONS_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SUBSCRIBER_TRANSACTIONS_CDR)
    x = readcsv(filepath,feedname2_SUBSCRIBER_TRANSACTIONS_CDR)
    logging.info('branch_SUBSCRIBER_TRANSACTIONS_CDR' + x)
    return x

Branching_SUBSCRIBER_TRANSACTIONS_CDR = BranchPythonOperator(
    task_id='branchid_SUBSCRIBER_TRANSACTIONS_CDR',
    python_callable=branch_SUBSCRIBER_TRANSACTIONS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='PCF_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command= pcf_command_SUBSCRIBER_TRANSACTIONS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='Dedup_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command= dedup_command_SUBSCRIBER_TRANSACTIONS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='Dedup2_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command= dedup_command_SUBSCRIBER_TRANSACTIONS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='PCFCheck_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command=PCFCheck_command_SUBSCRIBER_TRANSACTIONS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SUBSCRIBER_TRANSACTIONS_CDR = EmailOperator(
    task_id='faile_SUBSCRIBER_TRANSACTIONS_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SUBSCRIBER_TRANSACTIONS_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SUBSCRIBER_TRANSACTIONS_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_SUBSCRIBER_TRANSACTIONS_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SUBSCRIBER_TRANSACTIONS_CDR >> PCF_SUBSCRIBER_TRANSACTIONS_CDR >> PCFCheck_SUBSCRIBER_TRANSACTIONS_CDR >> delay_python_task_SUBSCRIBER_TRANSACTIONS_CDR >> Dedup_SUBSCRIBER_TRANSACTIONS_CDR >> Validation_End 
Branching_SUBSCRIBER_TRANSACTIONS_CDR >> Dedup2_SUBSCRIBER_TRANSACTIONS_CDR >> Validation_End
Branching_SUBSCRIBER_TRANSACTIONS_CDR >> faile_SUBSCRIBER_TRANSACTIONS_CDR
Branching_SUBSCRIBER_TRANSACTIONS_CDR >> Validation_End


feedname_CREDIT_LIMIT_STATUS = 'CREDIT_LIMIT_STATUS'.upper()
feedname2_CREDIT_LIMIT_STATUS = 'CREDIT_LIMIT_STATUS'.lower()

dedup_command_CREDIT_LIMIT_STATUS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CREDIT_LIMIT_STATUS)
logging.info(dedup_command_CREDIT_LIMIT_STATUS) 

pcf_command_CREDIT_LIMIT_STATUS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CREDIT_LIMIT_STATUS)
logging.info(pcf_command_CREDIT_LIMIT_STATUS)

PCFCheck_command_CREDIT_LIMIT_STATUS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CREDIT_LIMIT_STATUS,run_date,sleeptime)
logging.info(PCFCheck_command_CREDIT_LIMIT_STATUS)

branchScript_CREDIT_LIMIT_STATUS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CREDIT_LIMIT_STATUS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CREDIT_LIMIT_STATUS)

def branch_CREDIT_LIMIT_STATUS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CREDIT_LIMIT_STATUS,feedname2_CREDIT_LIMIT_STATUS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CREDIT_LIMIT_STATUS)
    x = readcsv(filepath,feedname2_CREDIT_LIMIT_STATUS)
    logging.info('branch_CREDIT_LIMIT_STATUS' + x)
    return x

Branching_CREDIT_LIMIT_STATUS = BranchPythonOperator(
    task_id='branchid_CREDIT_LIMIT_STATUS',
    python_callable=branch_CREDIT_LIMIT_STATUS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CREDIT_LIMIT_STATUS = BashOperator(
    task_id='PCF_CREDIT_LIMIT_STATUS',
    bash_command= pcf_command_CREDIT_LIMIT_STATUS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CREDIT_LIMIT_STATUS = BashOperator(
    task_id='Dedup_CREDIT_LIMIT_STATUS',
    bash_command= dedup_command_CREDIT_LIMIT_STATUS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CREDIT_LIMIT_STATUS = BashOperator(
    task_id='Dedup2_CREDIT_LIMIT_STATUS',
    bash_command= dedup_command_CREDIT_LIMIT_STATUS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CREDIT_LIMIT_STATUS = BashOperator(
    task_id='PCFCheck_CREDIT_LIMIT_STATUS',
    bash_command=PCFCheck_command_CREDIT_LIMIT_STATUS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CREDIT_LIMIT_STATUS = EmailOperator(
    task_id='faile_CREDIT_LIMIT_STATUS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CREDIT_LIMIT_STATUS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CREDIT_LIMIT_STATUS: PythonOperator = PythonOperator(task_id="waitForFlush_CREDIT_LIMIT_STATUS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CREDIT_LIMIT_STATUS >> PCF_CREDIT_LIMIT_STATUS >> PCFCheck_CREDIT_LIMIT_STATUS >> delay_python_task_CREDIT_LIMIT_STATUS >> Dedup_CREDIT_LIMIT_STATUS >> Validation_End 
Branching_CREDIT_LIMIT_STATUS >> Dedup2_CREDIT_LIMIT_STATUS >> Validation_End
Branching_CREDIT_LIMIT_STATUS >> faile_CREDIT_LIMIT_STATUS
Branching_CREDIT_LIMIT_STATUS >> Validation_End


feedname_NIMC_DETAILS = 'NIMC_DETAILS'.upper()
feedname2_NIMC_DETAILS = 'NIMC_DETAILS'.lower()

dedup_command_NIMC_DETAILS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_NIMC_DETAILS)
logging.info(dedup_command_NIMC_DETAILS) 

pcf_command_NIMC_DETAILS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_NIMC_DETAILS)
logging.info(pcf_command_NIMC_DETAILS)

PCFCheck_command_NIMC_DETAILS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NIMC_DETAILS,run_date,sleeptime)
logging.info(PCFCheck_command_NIMC_DETAILS)

branchScript_NIMC_DETAILS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_NIMC_DETAILS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_NIMC_DETAILS)

def branch_NIMC_DETAILS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NIMC_DETAILS,feedname2_NIMC_DETAILS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NIMC_DETAILS)
    x = readcsv(filepath,feedname2_NIMC_DETAILS)
    logging.info('branch_NIMC_DETAILS' + x)
    return x

Branching_NIMC_DETAILS = BranchPythonOperator(
    task_id='branchid_NIMC_DETAILS',
    python_callable=branch_NIMC_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_NIMC_DETAILS = BashOperator(
    task_id='PCF_NIMC_DETAILS',
    bash_command= pcf_command_NIMC_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_NIMC_DETAILS = BashOperator(
    task_id='Dedup_NIMC_DETAILS',
    bash_command= dedup_command_NIMC_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_NIMC_DETAILS = BashOperator(
    task_id='Dedup2_NIMC_DETAILS',
    bash_command= dedup_command_NIMC_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_NIMC_DETAILS = BashOperator(
    task_id='PCFCheck_NIMC_DETAILS',
    bash_command=PCFCheck_command_NIMC_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_NIMC_DETAILS = EmailOperator(
    task_id='faile_NIMC_DETAILS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_NIMC_DETAILS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_NIMC_DETAILS: PythonOperator = PythonOperator(task_id="waitForFlush_NIMC_DETAILS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_NIMC_DETAILS >> PCF_NIMC_DETAILS >> PCFCheck_NIMC_DETAILS >> delay_python_task_NIMC_DETAILS >> Dedup_NIMC_DETAILS >> Validation_End 
Branching_NIMC_DETAILS >> Dedup2_NIMC_DETAILS >> Validation_End
Branching_NIMC_DETAILS >> faile_NIMC_DETAILS
Branching_NIMC_DETAILS >> Validation_End


feedname_NGVS_DUMP = 'NGVS_DUMP'.upper()
feedname2_NGVS_DUMP = 'NGVS_DUMP'.lower()

dedup_command_NGVS_DUMP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_NGVS_DUMP)
logging.info(dedup_command_NGVS_DUMP) 

pcf_command_NGVS_DUMP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_NGVS_DUMP)
logging.info(pcf_command_NGVS_DUMP)

PCFCheck_command_NGVS_DUMP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NGVS_DUMP,run_date,sleeptime)
logging.info(PCFCheck_command_NGVS_DUMP)

branchScript_NGVS_DUMP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_NGVS_DUMP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_NGVS_DUMP)

def branch_NGVS_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NGVS_DUMP,feedname2_NGVS_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NGVS_DUMP)
    x = readcsv(filepath,feedname2_NGVS_DUMP)
    logging.info('branch_NGVS_DUMP' + x)
    return x

Branching_NGVS_DUMP = BranchPythonOperator(
    task_id='branchid_NGVS_DUMP',
    python_callable=branch_NGVS_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_NGVS_DUMP = BashOperator(
    task_id='PCF_NGVS_DUMP',
    bash_command= pcf_command_NGVS_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_NGVS_DUMP = BashOperator(
    task_id='Dedup_NGVS_DUMP',
    bash_command= dedup_command_NGVS_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_NGVS_DUMP = BashOperator(
    task_id='Dedup2_NGVS_DUMP',
    bash_command= dedup_command_NGVS_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_NGVS_DUMP = BashOperator(
    task_id='PCFCheck_NGVS_DUMP',
    bash_command=PCFCheck_command_NGVS_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_NGVS_DUMP = EmailOperator(
    task_id='faile_NGVS_DUMP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_NGVS_DUMP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_NGVS_DUMP: PythonOperator = PythonOperator(task_id="waitForFlush_NGVS_DUMP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_NGVS_DUMP >> PCF_NGVS_DUMP >> PCFCheck_NGVS_DUMP >> delay_python_task_NGVS_DUMP >> Dedup_NGVS_DUMP >> Validation_End 
Branching_NGVS_DUMP >> Dedup2_NGVS_DUMP >> Validation_End
Branching_NGVS_DUMP >> faile_NGVS_DUMP
Branching_NGVS_DUMP >> Validation_End


feedname_SDP_DMP_AC = 'SDP_DMP_AC'.upper()
feedname2_SDP_DMP_AC = 'SDP_DMP_AC'.lower()

dedup_command_SDP_DMP_AC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SDP_DMP_AC)
logging.info(dedup_command_SDP_DMP_AC) 

pcf_command_SDP_DMP_AC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SDP_DMP_AC)
logging.info(pcf_command_SDP_DMP_AC)

PCFCheck_command_SDP_DMP_AC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SDP_DMP_AC,run_date,sleeptime)
logging.info(PCFCheck_command_SDP_DMP_AC)

branchScript_SDP_DMP_AC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SDP_DMP_AC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SDP_DMP_AC)

def branch_SDP_DMP_AC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SDP_DMP_AC,feedname2_SDP_DMP_AC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SDP_DMP_AC)
    x = readcsv(filepath,feedname2_SDP_DMP_AC)
    logging.info('branch_SDP_DMP_AC' + x)
    return x

Branching_SDP_DMP_AC = BranchPythonOperator(
    task_id='branchid_SDP_DMP_AC',
    python_callable=branch_SDP_DMP_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SDP_DMP_AC = BashOperator(
    task_id='PCF_SDP_DMP_AC',
    bash_command= pcf_command_SDP_DMP_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SDP_DMP_AC = BashOperator(
    task_id='Dedup_SDP_DMP_AC',
    bash_command= dedup_command_SDP_DMP_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SDP_DMP_AC = BashOperator(
    task_id='Dedup2_SDP_DMP_AC',
    bash_command= dedup_command_SDP_DMP_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SDP_DMP_AC = BashOperator(
    task_id='PCFCheck_SDP_DMP_AC',
    bash_command=PCFCheck_command_SDP_DMP_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_SDP_DMP_AC = EmailOperator(
    task_id='faile_SDP_DMP_AC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SDP_DMP_AC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SDP_DMP_AC: PythonOperator = PythonOperator(task_id="waitForFlush_SDP_DMP_AC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SDP_DMP_AC >> PCF_SDP_DMP_AC >> PCFCheck_SDP_DMP_AC >> delay_python_task_SDP_DMP_AC >> Dedup_SDP_DMP_AC >> Validation_End 
Branching_SDP_DMP_AC >> Dedup2_SDP_DMP_AC >> Validation_End
Branching_SDP_DMP_AC >> faile_SDP_DMP_AC
Branching_SDP_DMP_AC >> Validation_End
