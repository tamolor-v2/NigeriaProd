import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator,PythonOperator
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
sleeptime= 2*1000*60
##sleeptime= 1000*10






pathcsv = '/nas/share05/tools/DQ/status2/'
phases = ['Success','Dedup2_','PCF_','faile_']
email = ['a.jawawdeh@ligadata.com','support@ligadata.com']

default_args = {
    'owner': 'VR',
    'depends_on_past': False,
    'start_date': datetime(2020,3,31),
    'email': ['a.jawawdeh@ligadata.com','support@ligadata.com'],
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

dag = DAG('VR_G_KPI', default_args=default_args, catchup=False,schedule_interval='30 9 * * *')

ValidationCode = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto_P2.sh  %s all kpi_1.conf  true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_airflow_KPI_G_%s.txt' %(d_1,dayStr)
logging.info(ValidationCode)


Validation = BashOperator(
task_id='Validation',
bash_command= ValidationCode,
trigger_rule='all_success',
run_as_user='daasuser',
dag=dag,
run_as_owner=True,
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

feedname_AGL_BILL_ANALYZER_MONTHLY = 'AGL_BILL_ANALYZER_MONTHLY'.upper()
feedname2_AGL_BILL_ANALYZER_MONTHLY = 'AGL_BILL_ANALYZER_MONTHLY'.lower()

dedup_command_AGL_BILL_ANALYZER_MONTHLY="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_AGL_BILL_ANALYZER_MONTHLY)
logging.info(dedup_command_AGL_BILL_ANALYZER_MONTHLY)


pcf_command_AGL_BILL_ANALYZER_MONTHLY = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_AGL_BILL_ANALYZER_MONTHLY)
logging.info(pcf_command_AGL_BILL_ANALYZER_MONTHLY)

PCFCheck_command_AGL_BILL_ANALYZER_MONTHLY = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AGL_BILL_ANALYZER_MONTHLY,run_date,sleeptime)
logging.info(PCFCheck_command_AGL_BILL_ANALYZER_MONTHLY)


branchScript_AGL_BILL_ANALYZER_MONTHLY = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_AGL_BILL_ANALYZER_MONTHLY,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_AGL_BILL_ANALYZER_MONTHLY)

def branch_AGL_BILL_ANALYZER_MONTHLY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AGL_BILL_ANALYZER_MONTHLY,feedname2_AGL_BILL_ANALYZER_MONTHLY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AGL_BILL_ANALYZER_MONTHLY)
    x = readcsv(filepath,feedname_AGL_BILL_ANALYZER_MONTHLY)
    logging.info('branch_AGL_BILL_ANALYZER_MONTHLY' + x)
    return x

Branching_AGL_BILL_ANALYZER_MONTHLY = BranchPythonOperator(
    task_id='branchid_AGL_BILL_ANALYZER_MONTHLY',
    python_callable=branch_AGL_BILL_ANALYZER_MONTHLY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='PCF_AGL_BILL_ANALYZER_MONTHLY',
    bash_command= pcf_command_AGL_BILL_ANALYZER_MONTHLY,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='Dedup_AGL_BILL_ANALYZER_MONTHLY',
    bash_command= dedup_command_AGL_BILL_ANALYZER_MONTHLY,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='Dedup2_AGL_BILL_ANALYZER_MONTHLY',
    bash_command= dedup_command_AGL_BILL_ANALYZER_MONTHLY,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='PCFCheck_AGL_BILL_ANALYZER_MONTHLY',
    bash_command=PCFCheck_command_AGL_BILL_ANALYZER_MONTHLY,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_AGL_BILL_ANALYZER_MONTHLY = EmailOperator(
    task_id='faile_AGL_BILL_ANALYZER_MONTHLY',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AGL_BILL_ANALYZER_MONTHLY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_AGL_BILL_ANALYZER_MONTHLY >> PCF_AGL_BILL_ANALYZER_MONTHLY >> PCFCheck_AGL_BILL_ANALYZER_MONTHLY  >> Dedup_AGL_BILL_ANALYZER_MONTHLY >> Success
Branching_AGL_BILL_ANALYZER_MONTHLY >> Dedup2_AGL_BILL_ANALYZER_MONTHLY >> Success
Branching_AGL_BILL_ANALYZER_MONTHLY >> faile_AGL_BILL_ANALYZER_MONTHLY
Branching_AGL_BILL_ANALYZER_MONTHLY >> Success
feedname_BILL_RUN_STATISTICS_TAB = 'BILL_RUN_STATISTICS_TAB'.upper()
feedname2_BILL_RUN_STATISTICS_TAB = 'BILL_RUN_STATISTICS_TAB'.lower()

dedup_command_BILL_RUN_STATISTICS_TAB="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_BILL_RUN_STATISTICS_TAB)
logging.info(dedup_command_BILL_RUN_STATISTICS_TAB)


pcf_command_BILL_RUN_STATISTICS_TAB = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_BILL_RUN_STATISTICS_TAB)
logging.info(pcf_command_BILL_RUN_STATISTICS_TAB)

PCFCheck_command_BILL_RUN_STATISTICS_TAB = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BILL_RUN_STATISTICS_TAB,run_date,sleeptime)
logging.info(PCFCheck_command_BILL_RUN_STATISTICS_TAB)


branchScript_BILL_RUN_STATISTICS_TAB = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_BILL_RUN_STATISTICS_TAB,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_BILL_RUN_STATISTICS_TAB)

def branch_BILL_RUN_STATISTICS_TAB():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BILL_RUN_STATISTICS_TAB,feedname2_BILL_RUN_STATISTICS_TAB)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BILL_RUN_STATISTICS_TAB)
    x = readcsv(filepath,feedname_BILL_RUN_STATISTICS_TAB)
    logging.info('branch_BILL_RUN_STATISTICS_TAB' + x)
    return x

Branching_BILL_RUN_STATISTICS_TAB = BranchPythonOperator(
    task_id='branchid_BILL_RUN_STATISTICS_TAB',
    python_callable=branch_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='PCF_BILL_RUN_STATISTICS_TAB',
    bash_command= pcf_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='Dedup_BILL_RUN_STATISTICS_TAB',
    bash_command= dedup_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='Dedup2_BILL_RUN_STATISTICS_TAB',
    bash_command= dedup_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='PCFCheck_BILL_RUN_STATISTICS_TAB',
    bash_command=PCFCheck_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_BILL_RUN_STATISTICS_TAB = EmailOperator(
    task_id='faile_BILL_RUN_STATISTICS_TAB',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BILL_RUN_STATISTICS_TAB),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_BILL_RUN_STATISTICS_TAB >> PCF_BILL_RUN_STATISTICS_TAB >> PCFCheck_BILL_RUN_STATISTICS_TAB  >> Dedup_BILL_RUN_STATISTICS_TAB >> Success
Branching_BILL_RUN_STATISTICS_TAB >> Dedup2_BILL_RUN_STATISTICS_TAB >> Success
Branching_BILL_RUN_STATISTICS_TAB >> faile_BILL_RUN_STATISTICS_TAB
Branching_BILL_RUN_STATISTICS_TAB >> Success
feedname_BUDGET_YEAR_AMOUNT = 'BUDGET_YEAR_AMOUNT'.upper()
feedname2_BUDGET_YEAR_AMOUNT = 'BUDGET_YEAR_AMOUNT'.lower()

dedup_command_BUDGET_YEAR_AMOUNT="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_BUDGET_YEAR_AMOUNT)
logging.info(dedup_command_BUDGET_YEAR_AMOUNT)


pcf_command_BUDGET_YEAR_AMOUNT = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_BUDGET_YEAR_AMOUNT)
logging.info(pcf_command_BUDGET_YEAR_AMOUNT)

PCFCheck_command_BUDGET_YEAR_AMOUNT = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BUDGET_YEAR_AMOUNT,run_date,sleeptime)
logging.info(PCFCheck_command_BUDGET_YEAR_AMOUNT)


branchScript_BUDGET_YEAR_AMOUNT = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_BUDGET_YEAR_AMOUNT,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_BUDGET_YEAR_AMOUNT)

def branch_BUDGET_YEAR_AMOUNT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BUDGET_YEAR_AMOUNT,feedname2_BUDGET_YEAR_AMOUNT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BUDGET_YEAR_AMOUNT)
    x = readcsv(filepath,feedname_BUDGET_YEAR_AMOUNT)
    logging.info('branch_BUDGET_YEAR_AMOUNT' + x)
    return x

Branching_BUDGET_YEAR_AMOUNT = BranchPythonOperator(
    task_id='branchid_BUDGET_YEAR_AMOUNT',
    python_callable=branch_BUDGET_YEAR_AMOUNT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='PCF_BUDGET_YEAR_AMOUNT',
    bash_command= pcf_command_BUDGET_YEAR_AMOUNT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='Dedup_BUDGET_YEAR_AMOUNT',
    bash_command= dedup_command_BUDGET_YEAR_AMOUNT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='Dedup2_BUDGET_YEAR_AMOUNT',
    bash_command= dedup_command_BUDGET_YEAR_AMOUNT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='PCFCheck_BUDGET_YEAR_AMOUNT',
    bash_command=PCFCheck_command_BUDGET_YEAR_AMOUNT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_BUDGET_YEAR_AMOUNT = EmailOperator(
    task_id='faile_BUDGET_YEAR_AMOUNT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BUDGET_YEAR_AMOUNT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_BUDGET_YEAR_AMOUNT >> PCF_BUDGET_YEAR_AMOUNT >> PCFCheck_BUDGET_YEAR_AMOUNT  >> Dedup_BUDGET_YEAR_AMOUNT >> Success
Branching_BUDGET_YEAR_AMOUNT >> Dedup2_BUDGET_YEAR_AMOUNT >> Success
Branching_BUDGET_YEAR_AMOUNT >> faile_BUDGET_YEAR_AMOUNT
Branching_BUDGET_YEAR_AMOUNT >> Success
feedname_CB_SCHEDULES = 'CB_SCHEDULES'.upper()
feedname2_CB_SCHEDULES = 'CB_SCHEDULES'.lower()

dedup_command_CB_SCHEDULES="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_CB_SCHEDULES)
logging.info(dedup_command_CB_SCHEDULES)


pcf_command_CB_SCHEDULES = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_CB_SCHEDULES)
logging.info(pcf_command_CB_SCHEDULES)

PCFCheck_command_CB_SCHEDULES = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_SCHEDULES,run_date,sleeptime)
logging.info(PCFCheck_command_CB_SCHEDULES)


branchScript_CB_SCHEDULES = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_CB_SCHEDULES,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_CB_SCHEDULES)

def branch_CB_SCHEDULES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_SCHEDULES,feedname2_CB_SCHEDULES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_SCHEDULES)
    x = readcsv(filepath,feedname_CB_SCHEDULES)
    logging.info('branch_CB_SCHEDULES' + x)
    return x

Branching_CB_SCHEDULES = BranchPythonOperator(
    task_id='branchid_CB_SCHEDULES',
    python_callable=branch_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CB_SCHEDULES = BashOperator(
    task_id='PCF_CB_SCHEDULES',
    bash_command= pcf_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_CB_SCHEDULES = BashOperator(
    task_id='Dedup_CB_SCHEDULES',
    bash_command= dedup_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_CB_SCHEDULES = BashOperator(
    task_id='Dedup2_CB_SCHEDULES',
    bash_command= dedup_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_CB_SCHEDULES = BashOperator(
    task_id='PCFCheck_CB_SCHEDULES',
    bash_command=PCFCheck_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_CB_SCHEDULES = EmailOperator(
    task_id='faile_CB_SCHEDULES',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_SCHEDULES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_CB_SCHEDULES >> PCF_CB_SCHEDULES >> PCFCheck_CB_SCHEDULES  >> Dedup_CB_SCHEDULES >> Success
Branching_CB_SCHEDULES >> Dedup2_CB_SCHEDULES >> Success
Branching_CB_SCHEDULES >> faile_CB_SCHEDULES
Branching_CB_SCHEDULES >> Success
feedname_CB_SUBS_POS_SERVICES = 'CB_SUBS_POS_SERVICES'.upper()
feedname2_CB_SUBS_POS_SERVICES = 'CB_SUBS_POS_SERVICES'.lower()

dedup_command_CB_SUBS_POS_SERVICES="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_CB_SUBS_POS_SERVICES)
logging.info(dedup_command_CB_SUBS_POS_SERVICES)


pcf_command_CB_SUBS_POS_SERVICES = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_CB_SUBS_POS_SERVICES)
logging.info(pcf_command_CB_SUBS_POS_SERVICES)

PCFCheck_command_CB_SUBS_POS_SERVICES = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_SUBS_POS_SERVICES,run_date,sleeptime)
logging.info(PCFCheck_command_CB_SUBS_POS_SERVICES)


branchScript_CB_SUBS_POS_SERVICES = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_CB_SUBS_POS_SERVICES,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_CB_SUBS_POS_SERVICES)

def branch_CB_SUBS_POS_SERVICES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_SUBS_POS_SERVICES,feedname2_CB_SUBS_POS_SERVICES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_SUBS_POS_SERVICES)
    x = readcsv(filepath,feedname_CB_SUBS_POS_SERVICES)
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
    run_as_owner=True,
    priority_weight=1
)

Dedup_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='Dedup_CB_SUBS_POS_SERVICES',
    bash_command= dedup_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='Dedup2_CB_SUBS_POS_SERVICES',
    bash_command= dedup_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='PCFCheck_CB_SUBS_POS_SERVICES',
    bash_command=PCFCheck_command_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
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

Branching_CB_SUBS_POS_SERVICES >> PCF_CB_SUBS_POS_SERVICES >> PCFCheck_CB_SUBS_POS_SERVICES  >> Dedup_CB_SUBS_POS_SERVICES >> Success
Branching_CB_SUBS_POS_SERVICES >> Dedup2_CB_SUBS_POS_SERVICES >> Success
Branching_CB_SUBS_POS_SERVICES >> faile_CB_SUBS_POS_SERVICES
Branching_CB_SUBS_POS_SERVICES >> Success
feedname_CHANGE_REPORT_MTN = 'CHANGE_REPORT_MTN'.upper()
feedname2_CHANGE_REPORT_MTN = 'CHANGE_REPORT_MTN'.lower()

dedup_command_CHANGE_REPORT_MTN="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_CHANGE_REPORT_MTN)
logging.info(dedup_command_CHANGE_REPORT_MTN)


pcf_command_CHANGE_REPORT_MTN = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_CHANGE_REPORT_MTN)
logging.info(pcf_command_CHANGE_REPORT_MTN)

PCFCheck_command_CHANGE_REPORT_MTN = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CHANGE_REPORT_MTN,run_date,sleeptime)
logging.info(PCFCheck_command_CHANGE_REPORT_MTN)


branchScript_CHANGE_REPORT_MTN = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_CHANGE_REPORT_MTN,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_CHANGE_REPORT_MTN)

def branch_CHANGE_REPORT_MTN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CHANGE_REPORT_MTN,feedname2_CHANGE_REPORT_MTN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CHANGE_REPORT_MTN)
    x = readcsv(filepath,feedname_CHANGE_REPORT_MTN)
    logging.info('branch_CHANGE_REPORT_MTN' + x)
    return x

Branching_CHANGE_REPORT_MTN = BranchPythonOperator(
    task_id='branchid_CHANGE_REPORT_MTN',
    python_callable=branch_CHANGE_REPORT_MTN,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CHANGE_REPORT_MTN = BashOperator(
    task_id='PCF_CHANGE_REPORT_MTN',
    bash_command= pcf_command_CHANGE_REPORT_MTN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_CHANGE_REPORT_MTN = BashOperator(
    task_id='Dedup_CHANGE_REPORT_MTN',
    bash_command= dedup_command_CHANGE_REPORT_MTN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_CHANGE_REPORT_MTN = BashOperator(
    task_id='Dedup2_CHANGE_REPORT_MTN',
    bash_command= dedup_command_CHANGE_REPORT_MTN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_CHANGE_REPORT_MTN = BashOperator(
    task_id='PCFCheck_CHANGE_REPORT_MTN',
    bash_command=PCFCheck_command_CHANGE_REPORT_MTN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_CHANGE_REPORT_MTN = EmailOperator(
    task_id='faile_CHANGE_REPORT_MTN',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CHANGE_REPORT_MTN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_CHANGE_REPORT_MTN >> PCF_CHANGE_REPORT_MTN >> PCFCheck_CHANGE_REPORT_MTN  >> Dedup_CHANGE_REPORT_MTN >> Success
Branching_CHANGE_REPORT_MTN >> Dedup2_CHANGE_REPORT_MTN >> Success
Branching_CHANGE_REPORT_MTN >> faile_CHANGE_REPORT_MTN
Branching_CHANGE_REPORT_MTN >> Success
feedname_CLIENT_ACTIVITY_LOG = 'CLIENT_ACTIVITY_LOG'.upper()
feedname2_CLIENT_ACTIVITY_LOG = 'CLIENT_ACTIVITY_LOG'.lower()

dedup_command_CLIENT_ACTIVITY_LOG="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_CLIENT_ACTIVITY_LOG)
logging.info(dedup_command_CLIENT_ACTIVITY_LOG)


pcf_command_CLIENT_ACTIVITY_LOG = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_CLIENT_ACTIVITY_LOG)
logging.info(pcf_command_CLIENT_ACTIVITY_LOG)

PCFCheck_command_CLIENT_ACTIVITY_LOG = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CLIENT_ACTIVITY_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_CLIENT_ACTIVITY_LOG)


branchScript_CLIENT_ACTIVITY_LOG = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_CLIENT_ACTIVITY_LOG,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_CLIENT_ACTIVITY_LOG)

def branch_CLIENT_ACTIVITY_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CLIENT_ACTIVITY_LOG,feedname2_CLIENT_ACTIVITY_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CLIENT_ACTIVITY_LOG)
    x = readcsv(filepath,feedname_CLIENT_ACTIVITY_LOG)
    logging.info('branch_CLIENT_ACTIVITY_LOG' + x)
    return x

Branching_CLIENT_ACTIVITY_LOG = BranchPythonOperator(
    task_id='branchid_CLIENT_ACTIVITY_LOG',
    python_callable=branch_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='PCF_CLIENT_ACTIVITY_LOG',
    bash_command= pcf_command_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='Dedup_CLIENT_ACTIVITY_LOG',
    bash_command= dedup_command_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='Dedup2_CLIENT_ACTIVITY_LOG',
    bash_command= dedup_command_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='PCFCheck_CLIENT_ACTIVITY_LOG',
    bash_command=PCFCheck_command_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_CLIENT_ACTIVITY_LOG = EmailOperator(
    task_id='faile_CLIENT_ACTIVITY_LOG',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CLIENT_ACTIVITY_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_CLIENT_ACTIVITY_LOG >> PCF_CLIENT_ACTIVITY_LOG >> PCFCheck_CLIENT_ACTIVITY_LOG  >> Dedup_CLIENT_ACTIVITY_LOG >> Success
Branching_CLIENT_ACTIVITY_LOG >> Dedup2_CLIENT_ACTIVITY_LOG >> Success
Branching_CLIENT_ACTIVITY_LOG >> faile_CLIENT_ACTIVITY_LOG
Branching_CLIENT_ACTIVITY_LOG >> Success
feedname_HPD_HELP_DESK = 'HPD_HELP_DESK'.upper()
feedname2_HPD_HELP_DESK = 'HPD_HELP_DESK'.lower()

dedup_command_HPD_HELP_DESK="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_HPD_HELP_DESK)
logging.info(dedup_command_HPD_HELP_DESK)


pcf_command_HPD_HELP_DESK = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_HPD_HELP_DESK)
logging.info(pcf_command_HPD_HELP_DESK)

PCFCheck_command_HPD_HELP_DESK = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_HPD_HELP_DESK,run_date,sleeptime)
logging.info(PCFCheck_command_HPD_HELP_DESK)


branchScript_HPD_HELP_DESK = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_HPD_HELP_DESK,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_HPD_HELP_DESK)

def branch_HPD_HELP_DESK():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_HPD_HELP_DESK,feedname2_HPD_HELP_DESK)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_HPD_HELP_DESK)
    x = readcsv(filepath,feedname_HPD_HELP_DESK)
    logging.info('branch_HPD_HELP_DESK' + x)
    return x

Branching_HPD_HELP_DESK = BranchPythonOperator(
    task_id='branchid_HPD_HELP_DESK',
    python_callable=branch_HPD_HELP_DESK,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_HPD_HELP_DESK = BashOperator(
    task_id='PCF_HPD_HELP_DESK',
    bash_command= pcf_command_HPD_HELP_DESK,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_HPD_HELP_DESK = BashOperator(
    task_id='Dedup_HPD_HELP_DESK',
    bash_command= dedup_command_HPD_HELP_DESK,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_HPD_HELP_DESK = BashOperator(
    task_id='Dedup2_HPD_HELP_DESK',
    bash_command= dedup_command_HPD_HELP_DESK,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_HPD_HELP_DESK = BashOperator(
    task_id='PCFCheck_HPD_HELP_DESK',
    bash_command=PCFCheck_command_HPD_HELP_DESK,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_HPD_HELP_DESK = EmailOperator(
    task_id='faile_HPD_HELP_DESK',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_HPD_HELP_DESK),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_HPD_HELP_DESK >> PCF_HPD_HELP_DESK >> PCFCheck_HPD_HELP_DESK  >> Dedup_HPD_HELP_DESK >> Success
Branching_HPD_HELP_DESK >> Dedup2_HPD_HELP_DESK >> Success
Branching_HPD_HELP_DESK >> faile_HPD_HELP_DESK
Branching_HPD_HELP_DESK >> Success
feedname_MSO_ACCURACY_STATISTICS = 'MSO_ACCURACY_STATISTICS'.upper()
feedname2_MSO_ACCURACY_STATISTICS = 'MSO_ACCURACY_STATISTICS'.lower()

dedup_command_MSO_ACCURACY_STATISTICS="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_MSO_ACCURACY_STATISTICS)
logging.info(dedup_command_MSO_ACCURACY_STATISTICS)


pcf_command_MSO_ACCURACY_STATISTICS = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_MSO_ACCURACY_STATISTICS)
logging.info(pcf_command_MSO_ACCURACY_STATISTICS)

PCFCheck_command_MSO_ACCURACY_STATISTICS = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSO_ACCURACY_STATISTICS,run_date,sleeptime)
logging.info(PCFCheck_command_MSO_ACCURACY_STATISTICS)


branchScript_MSO_ACCURACY_STATISTICS = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_MSO_ACCURACY_STATISTICS,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_MSO_ACCURACY_STATISTICS)

def branch_MSO_ACCURACY_STATISTICS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSO_ACCURACY_STATISTICS,feedname2_MSO_ACCURACY_STATISTICS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSO_ACCURACY_STATISTICS)
    x = readcsv(filepath,feedname_MSO_ACCURACY_STATISTICS)
    logging.info('branch_MSO_ACCURACY_STATISTICS' + x)
    return x

Branching_MSO_ACCURACY_STATISTICS = BranchPythonOperator(
    task_id='branchid_MSO_ACCURACY_STATISTICS',
    python_callable=branch_MSO_ACCURACY_STATISTICS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='PCF_MSO_ACCURACY_STATISTICS',
    bash_command= pcf_command_MSO_ACCURACY_STATISTICS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='Dedup_MSO_ACCURACY_STATISTICS',
    bash_command= dedup_command_MSO_ACCURACY_STATISTICS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='Dedup2_MSO_ACCURACY_STATISTICS',
    bash_command= dedup_command_MSO_ACCURACY_STATISTICS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='PCFCheck_MSO_ACCURACY_STATISTICS',
    bash_command=PCFCheck_command_MSO_ACCURACY_STATISTICS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_MSO_ACCURACY_STATISTICS = EmailOperator(
    task_id='faile_MSO_ACCURACY_STATISTICS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_ACCURACY_STATISTICS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_MSO_ACCURACY_STATISTICS >> PCF_MSO_ACCURACY_STATISTICS >> PCFCheck_MSO_ACCURACY_STATISTICS  >> Dedup_MSO_ACCURACY_STATISTICS >> Success
Branching_MSO_ACCURACY_STATISTICS >> Dedup2_MSO_ACCURACY_STATISTICS >> Success
Branching_MSO_ACCURACY_STATISTICS >> faile_MSO_ACCURACY_STATISTICS
Branching_MSO_ACCURACY_STATISTICS >> Success
feedname_MSO_PROCESS_AVG_TIME = 'MSO_PROCESS_AVG_TIME'.upper()
feedname2_MSO_PROCESS_AVG_TIME = 'MSO_PROCESS_AVG_TIME'.lower()

dedup_command_MSO_PROCESS_AVG_TIME="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_MSO_PROCESS_AVG_TIME)
logging.info(dedup_command_MSO_PROCESS_AVG_TIME)


pcf_command_MSO_PROCESS_AVG_TIME = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_MSO_PROCESS_AVG_TIME)
logging.info(pcf_command_MSO_PROCESS_AVG_TIME)

PCFCheck_command_MSO_PROCESS_AVG_TIME = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSO_PROCESS_AVG_TIME,run_date,sleeptime)
logging.info(PCFCheck_command_MSO_PROCESS_AVG_TIME)


branchScript_MSO_PROCESS_AVG_TIME = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_MSO_PROCESS_AVG_TIME,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_MSO_PROCESS_AVG_TIME)

def branch_MSO_PROCESS_AVG_TIME():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSO_PROCESS_AVG_TIME,feedname2_MSO_PROCESS_AVG_TIME)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSO_PROCESS_AVG_TIME)
    x = readcsv(filepath,feedname_MSO_PROCESS_AVG_TIME)
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
    run_as_owner=True,
    priority_weight=1
)

Dedup_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='Dedup_MSO_PROCESS_AVG_TIME',
    bash_command= dedup_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='Dedup2_MSO_PROCESS_AVG_TIME',
    bash_command= dedup_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='PCFCheck_MSO_PROCESS_AVG_TIME',
    bash_command=PCFCheck_command_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
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

Branching_MSO_PROCESS_AVG_TIME >> PCF_MSO_PROCESS_AVG_TIME >> PCFCheck_MSO_PROCESS_AVG_TIME  >> Dedup_MSO_PROCESS_AVG_TIME >> Success
Branching_MSO_PROCESS_AVG_TIME >> Dedup2_MSO_PROCESS_AVG_TIME >> Success
Branching_MSO_PROCESS_AVG_TIME >> faile_MSO_PROCESS_AVG_TIME
Branching_MSO_PROCESS_AVG_TIME >> Success
feedname_MTN_OVERVIEW_COMM_RPT = 'MTN_OVERVIEW_COMM_RPT'.upper()
feedname2_MTN_OVERVIEW_COMM_RPT = 'MTN_OVERVIEW_COMM_RPT'.lower()

dedup_command_MTN_OVERVIEW_COMM_RPT="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_MTN_OVERVIEW_COMM_RPT)
logging.info(dedup_command_MTN_OVERVIEW_COMM_RPT)


pcf_command_MTN_OVERVIEW_COMM_RPT = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_MTN_OVERVIEW_COMM_RPT)
logging.info(pcf_command_MTN_OVERVIEW_COMM_RPT)

PCFCheck_command_MTN_OVERVIEW_COMM_RPT = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MTN_OVERVIEW_COMM_RPT,run_date,sleeptime)
logging.info(PCFCheck_command_MTN_OVERVIEW_COMM_RPT)


branchScript_MTN_OVERVIEW_COMM_RPT = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_MTN_OVERVIEW_COMM_RPT,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_MTN_OVERVIEW_COMM_RPT)

def branch_MTN_OVERVIEW_COMM_RPT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MTN_OVERVIEW_COMM_RPT,feedname2_MTN_OVERVIEW_COMM_RPT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MTN_OVERVIEW_COMM_RPT)
    x = readcsv(filepath,feedname_MTN_OVERVIEW_COMM_RPT)
    logging.info('branch_MTN_OVERVIEW_COMM_RPT' + x)
    return x

Branching_MTN_OVERVIEW_COMM_RPT = BranchPythonOperator(
    task_id='branchid_MTN_OVERVIEW_COMM_RPT',
    python_callable=branch_MTN_OVERVIEW_COMM_RPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='PCF_MTN_OVERVIEW_COMM_RPT',
    bash_command= pcf_command_MTN_OVERVIEW_COMM_RPT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='Dedup_MTN_OVERVIEW_COMM_RPT',
    bash_command= dedup_command_MTN_OVERVIEW_COMM_RPT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='Dedup2_MTN_OVERVIEW_COMM_RPT',
    bash_command= dedup_command_MTN_OVERVIEW_COMM_RPT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='PCFCheck_MTN_OVERVIEW_COMM_RPT',
    bash_command=PCFCheck_command_MTN_OVERVIEW_COMM_RPT,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_MTN_OVERVIEW_COMM_RPT = EmailOperator(
    task_id='faile_MTN_OVERVIEW_COMM_RPT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MTN_OVERVIEW_COMM_RPT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_MTN_OVERVIEW_COMM_RPT >> PCF_MTN_OVERVIEW_COMM_RPT >> PCFCheck_MTN_OVERVIEW_COMM_RPT  >> Dedup_MTN_OVERVIEW_COMM_RPT >> Success
Branching_MTN_OVERVIEW_COMM_RPT >> Dedup2_MTN_OVERVIEW_COMM_RPT >> Success
Branching_MTN_OVERVIEW_COMM_RPT >> faile_MTN_OVERVIEW_COMM_RPT
Branching_MTN_OVERVIEW_COMM_RPT >> Success
feedname_MTN_PR_DETAILS = 'MTN_PR_DETAILS'.upper()
feedname2_MTN_PR_DETAILS = 'MTN_PR_DETAILS'.lower()

dedup_command_MTN_PR_DETAILS="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_MTN_PR_DETAILS)
logging.info(dedup_command_MTN_PR_DETAILS)


pcf_command_MTN_PR_DETAILS = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_MTN_PR_DETAILS)
logging.info(pcf_command_MTN_PR_DETAILS)

PCFCheck_command_MTN_PR_DETAILS = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MTN_PR_DETAILS,run_date,sleeptime)
logging.info(PCFCheck_command_MTN_PR_DETAILS)


branchScript_MTN_PR_DETAILS = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_MTN_PR_DETAILS,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_MTN_PR_DETAILS)

def branch_MTN_PR_DETAILS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MTN_PR_DETAILS,feedname2_MTN_PR_DETAILS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MTN_PR_DETAILS)
    x = readcsv(filepath,feedname_MTN_PR_DETAILS)
    logging.info('branch_MTN_PR_DETAILS' + x)
    return x

Branching_MTN_PR_DETAILS = BranchPythonOperator(
    task_id='branchid_MTN_PR_DETAILS',
    python_callable=branch_MTN_PR_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MTN_PR_DETAILS = BashOperator(
    task_id='PCF_MTN_PR_DETAILS',
    bash_command= pcf_command_MTN_PR_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_MTN_PR_DETAILS = BashOperator(
    task_id='Dedup_MTN_PR_DETAILS',
    bash_command= dedup_command_MTN_PR_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_MTN_PR_DETAILS = BashOperator(
    task_id='Dedup2_MTN_PR_DETAILS',
    bash_command= dedup_command_MTN_PR_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_MTN_PR_DETAILS = BashOperator(
    task_id='PCFCheck_MTN_PR_DETAILS',
    bash_command=PCFCheck_command_MTN_PR_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_MTN_PR_DETAILS = EmailOperator(
    task_id='faile_MTN_PR_DETAILS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MTN_PR_DETAILS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_MTN_PR_DETAILS >> PCF_MTN_PR_DETAILS >> PCFCheck_MTN_PR_DETAILS  >> Dedup_MTN_PR_DETAILS >> Success
Branching_MTN_PR_DETAILS >> Dedup2_MTN_PR_DETAILS >> Success
Branching_MTN_PR_DETAILS >> faile_MTN_PR_DETAILS
Branching_MTN_PR_DETAILS >> Success
feedname_OTP_STATUS = 'OTP_STATUS'.upper()
feedname2_OTP_STATUS = 'OTP_STATUS'.lower()

dedup_command_OTP_STATUS="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_OTP_STATUS)
logging.info(dedup_command_OTP_STATUS)


pcf_command_OTP_STATUS = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_OTP_STATUS)
logging.info(pcf_command_OTP_STATUS)

PCFCheck_command_OTP_STATUS = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_OTP_STATUS,run_date,sleeptime)
logging.info(PCFCheck_command_OTP_STATUS)


branchScript_OTP_STATUS = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_OTP_STATUS,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_OTP_STATUS)

def branch_OTP_STATUS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_OTP_STATUS,feedname2_OTP_STATUS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_OTP_STATUS)
    x = readcsv(filepath,feedname_OTP_STATUS)
    logging.info('branch_OTP_STATUS' + x)
    return x

Branching_OTP_STATUS = BranchPythonOperator(
    task_id='branchid_OTP_STATUS',
    python_callable=branch_OTP_STATUS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_OTP_STATUS = BashOperator(
    task_id='PCF_OTP_STATUS',
    bash_command= pcf_command_OTP_STATUS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_OTP_STATUS = BashOperator(
    task_id='Dedup_OTP_STATUS',
    bash_command= dedup_command_OTP_STATUS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_OTP_STATUS = BashOperator(
    task_id='Dedup2_OTP_STATUS',
    bash_command= dedup_command_OTP_STATUS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_OTP_STATUS = BashOperator(
    task_id='PCFCheck_OTP_STATUS',
    bash_command=PCFCheck_command_OTP_STATUS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_OTP_STATUS = EmailOperator(
    task_id='faile_OTP_STATUS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_OTP_STATUS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_OTP_STATUS >> PCF_OTP_STATUS >> PCFCheck_OTP_STATUS  >> Dedup_OTP_STATUS >> Success
Branching_OTP_STATUS >> Dedup2_OTP_STATUS >> Success
Branching_OTP_STATUS >> faile_OTP_STATUS
Branching_OTP_STATUS >> Success
feedname_SERV_STATUS_SEAMFIX_REF = 'SERV_STATUS_SEAMFIX_REF'.upper()
feedname2_SERV_STATUS_SEAMFIX_REF = 'SERV_STATUS_SEAMFIX_REF'.lower()

dedup_command_SERV_STATUS_SEAMFIX_REF="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_SERV_STATUS_SEAMFIX_REF)
logging.info(dedup_command_SERV_STATUS_SEAMFIX_REF)


pcf_command_SERV_STATUS_SEAMFIX_REF = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_SERV_STATUS_SEAMFIX_REF)
logging.info(pcf_command_SERV_STATUS_SEAMFIX_REF)

PCFCheck_command_SERV_STATUS_SEAMFIX_REF = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SERV_STATUS_SEAMFIX_REF,run_date,sleeptime)
logging.info(PCFCheck_command_SERV_STATUS_SEAMFIX_REF)


branchScript_SERV_STATUS_SEAMFIX_REF = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_SERV_STATUS_SEAMFIX_REF,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_SERV_STATUS_SEAMFIX_REF)

def branch_SERV_STATUS_SEAMFIX_REF():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SERV_STATUS_SEAMFIX_REF,feedname2_SERV_STATUS_SEAMFIX_REF)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SERV_STATUS_SEAMFIX_REF)
    x = readcsv(filepath,feedname_SERV_STATUS_SEAMFIX_REF)
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
    run_as_owner=True,
    priority_weight=1
)

Dedup_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='Dedup_SERV_STATUS_SEAMFIX_REF',
    bash_command= dedup_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='Dedup2_SERV_STATUS_SEAMFIX_REF',
    bash_command= dedup_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='PCFCheck_SERV_STATUS_SEAMFIX_REF',
    bash_command=PCFCheck_command_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
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

Branching_SERV_STATUS_SEAMFIX_REF >> PCF_SERV_STATUS_SEAMFIX_REF >> PCFCheck_SERV_STATUS_SEAMFIX_REF  >> Dedup_SERV_STATUS_SEAMFIX_REF >> Success
Branching_SERV_STATUS_SEAMFIX_REF >> Dedup2_SERV_STATUS_SEAMFIX_REF >> Success
Branching_SERV_STATUS_SEAMFIX_REF >> faile_SERV_STATUS_SEAMFIX_REF
Branching_SERV_STATUS_SEAMFIX_REF >> Success
feedname_SMS_ACTIVATION_REQUEST_JOIN = 'SMS_ACTIVATION_REQUEST_JOIN'.upper()
feedname2_SMS_ACTIVATION_REQUEST_JOIN = 'SMS_ACTIVATION_REQUEST_JOIN'.lower()

dedup_command_SMS_ACTIVATION_REQUEST_JOIN="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_SMS_ACTIVATION_REQUEST_JOIN)
logging.info(dedup_command_SMS_ACTIVATION_REQUEST_JOIN)


pcf_command_SMS_ACTIVATION_REQUEST_JOIN = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_SMS_ACTIVATION_REQUEST_JOIN)
logging.info(pcf_command_SMS_ACTIVATION_REQUEST_JOIN)

PCFCheck_command_SMS_ACTIVATION_REQUEST_JOIN = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMS_ACTIVATION_REQUEST_JOIN,run_date,sleeptime)
logging.info(PCFCheck_command_SMS_ACTIVATION_REQUEST_JOIN)


branchScript_SMS_ACTIVATION_REQUEST_JOIN = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_SMS_ACTIVATION_REQUEST_JOIN,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_SMS_ACTIVATION_REQUEST_JOIN)

def branch_SMS_ACTIVATION_REQUEST_JOIN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMS_ACTIVATION_REQUEST_JOIN,feedname2_SMS_ACTIVATION_REQUEST_JOIN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMS_ACTIVATION_REQUEST_JOIN)
    x = readcsv(filepath,feedname_SMS_ACTIVATION_REQUEST_JOIN)
    logging.info('branch_SMS_ACTIVATION_REQUEST_JOIN' + x)
    return x

Branching_SMS_ACTIVATION_REQUEST_JOIN = BranchPythonOperator(
    task_id='branchid_SMS_ACTIVATION_REQUEST_JOIN',
    python_callable=branch_SMS_ACTIVATION_REQUEST_JOIN,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='PCF_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command= pcf_command_SMS_ACTIVATION_REQUEST_JOIN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='Dedup_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command= dedup_command_SMS_ACTIVATION_REQUEST_JOIN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='Dedup2_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command= dedup_command_SMS_ACTIVATION_REQUEST_JOIN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='PCFCheck_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command=PCFCheck_command_SMS_ACTIVATION_REQUEST_JOIN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_SMS_ACTIVATION_REQUEST_JOIN = EmailOperator(
    task_id='faile_SMS_ACTIVATION_REQUEST_JOIN',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMS_ACTIVATION_REQUEST_JOIN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_SMS_ACTIVATION_REQUEST_JOIN >> PCF_SMS_ACTIVATION_REQUEST_JOIN >> PCFCheck_SMS_ACTIVATION_REQUEST_JOIN  >> Dedup_SMS_ACTIVATION_REQUEST_JOIN >> Success
Branching_SMS_ACTIVATION_REQUEST_JOIN >> Dedup2_SMS_ACTIVATION_REQUEST_JOIN >> Success
Branching_SMS_ACTIVATION_REQUEST_JOIN >> faile_SMS_ACTIVATION_REQUEST_JOIN
Branching_SMS_ACTIVATION_REQUEST_JOIN >> Success
feedname_SMS_ACTIVATION_VW = 'SMS_ACTIVATION_VW'.upper()
feedname2_SMS_ACTIVATION_VW = 'SMS_ACTIVATION_VW'.lower()

dedup_command_SMS_ACTIVATION_VW="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_SMS_ACTIVATION_VW)
logging.info(dedup_command_SMS_ACTIVATION_VW)


pcf_command_SMS_ACTIVATION_VW = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_SMS_ACTIVATION_VW)
logging.info(pcf_command_SMS_ACTIVATION_VW)

PCFCheck_command_SMS_ACTIVATION_VW = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMS_ACTIVATION_VW,run_date,sleeptime)
logging.info(PCFCheck_command_SMS_ACTIVATION_VW)


branchScript_SMS_ACTIVATION_VW = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_SMS_ACTIVATION_VW,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_SMS_ACTIVATION_VW)

def branch_SMS_ACTIVATION_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMS_ACTIVATION_VW,feedname2_SMS_ACTIVATION_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMS_ACTIVATION_VW)
    x = readcsv(filepath,feedname_SMS_ACTIVATION_VW)
    logging.info('branch_SMS_ACTIVATION_VW' + x)
    return x

Branching_SMS_ACTIVATION_VW = BranchPythonOperator(
    task_id='branchid_SMS_ACTIVATION_VW',
    python_callable=branch_SMS_ACTIVATION_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMS_ACTIVATION_VW = BashOperator(
    task_id='PCF_SMS_ACTIVATION_VW',
    bash_command= pcf_command_SMS_ACTIVATION_VW,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_SMS_ACTIVATION_VW = BashOperator(
    task_id='Dedup_SMS_ACTIVATION_VW',
    bash_command= dedup_command_SMS_ACTIVATION_VW,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_SMS_ACTIVATION_VW = BashOperator(
    task_id='Dedup2_SMS_ACTIVATION_VW',
    bash_command= dedup_command_SMS_ACTIVATION_VW,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_SMS_ACTIVATION_VW = BashOperator(
    task_id='PCFCheck_SMS_ACTIVATION_VW',
    bash_command=PCFCheck_command_SMS_ACTIVATION_VW,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_SMS_ACTIVATION_VW = EmailOperator(
    task_id='faile_SMS_ACTIVATION_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMS_ACTIVATION_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_SMS_ACTIVATION_VW >> PCF_SMS_ACTIVATION_VW >> PCFCheck_SMS_ACTIVATION_VW  >> Dedup_SMS_ACTIVATION_VW >> Success
Branching_SMS_ACTIVATION_VW >> Dedup2_SMS_ACTIVATION_VW >> Success
Branching_SMS_ACTIVATION_VW >> faile_SMS_ACTIVATION_VW
Branching_SMS_ACTIVATION_VW >> Success
feedname_VALIDATION_PASSED = 'VALIDATION_PASSED'.upper()
feedname2_VALIDATION_PASSED = 'VALIDATION_PASSED'.lower()

dedup_command_VALIDATION_PASSED="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_VALIDATION_PASSED)
logging.info(dedup_command_VALIDATION_PASSED)


pcf_command_VALIDATION_PASSED = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_VALIDATION_PASSED)
logging.info(pcf_command_VALIDATION_PASSED)

PCFCheck_command_VALIDATION_PASSED = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_VALIDATION_PASSED,run_date,sleeptime)
logging.info(PCFCheck_command_VALIDATION_PASSED)


branchScript_VALIDATION_PASSED = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_VALIDATION_PASSED,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_VALIDATION_PASSED)

def branch_VALIDATION_PASSED():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_VALIDATION_PASSED,feedname2_VALIDATION_PASSED)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_VALIDATION_PASSED)
    x = readcsv(filepath,feedname_VALIDATION_PASSED)
    logging.info('branch_VALIDATION_PASSED' + x)
    return x

Branching_VALIDATION_PASSED = BranchPythonOperator(
    task_id='branchid_VALIDATION_PASSED',
    python_callable=branch_VALIDATION_PASSED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_VALIDATION_PASSED = BashOperator(
    task_id='PCF_VALIDATION_PASSED',
    bash_command= pcf_command_VALIDATION_PASSED,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_VALIDATION_PASSED = BashOperator(
    task_id='Dedup_VALIDATION_PASSED',
    bash_command= dedup_command_VALIDATION_PASSED,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_VALIDATION_PASSED = BashOperator(
    task_id='Dedup2_VALIDATION_PASSED',
    bash_command= dedup_command_VALIDATION_PASSED,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_VALIDATION_PASSED = BashOperator(
    task_id='PCFCheck_VALIDATION_PASSED',
    bash_command=PCFCheck_command_VALIDATION_PASSED,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_VALIDATION_PASSED = EmailOperator(
    task_id='faile_VALIDATION_PASSED',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_VALIDATION_PASSED),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_VALIDATION_PASSED >> PCF_VALIDATION_PASSED >> PCFCheck_VALIDATION_PASSED  >> Dedup_VALIDATION_PASSED >> Success
Branching_VALIDATION_PASSED >> Dedup2_VALIDATION_PASSED >> Success
Branching_VALIDATION_PASSED >> faile_VALIDATION_PASSED
Branching_VALIDATION_PASSED >> Success
feedname_SMS_ACTIVATION_REQUEST_KPI = 'SMS_ACTIVATION_REQUEST_KPI'.upper()
feedname2_SMS_ACTIVATION_REQUEST_KPI = 'SMS_ACTIVATION_REQUEST_KPI'.lower()

dedup_command_SMS_ACTIVATION_REQUEST_KPI="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_SMS_ACTIVATION_REQUEST_KPI)
logging.info(dedup_command_SMS_ACTIVATION_REQUEST_KPI)


pcf_command_SMS_ACTIVATION_REQUEST_KPI = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_SMS_ACTIVATION_REQUEST_KPI)
logging.info(pcf_command_SMS_ACTIVATION_REQUEST_KPI)

PCFCheck_command_SMS_ACTIVATION_REQUEST_KPI = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMS_ACTIVATION_REQUEST_KPI,run_date,sleeptime)
logging.info(PCFCheck_command_SMS_ACTIVATION_REQUEST_KPI)


branchScript_SMS_ACTIVATION_REQUEST_KPI = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_SMS_ACTIVATION_REQUEST_KPI,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_SMS_ACTIVATION_REQUEST_KPI)

def branch_SMS_ACTIVATION_REQUEST_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMS_ACTIVATION_REQUEST_KPI,feedname2_SMS_ACTIVATION_REQUEST_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMS_ACTIVATION_REQUEST_KPI)
    x = readcsv(filepath,feedname_SMS_ACTIVATION_REQUEST_KPI)
    logging.info('branch_SMS_ACTIVATION_REQUEST_KPI' + x)
    return x

Branching_SMS_ACTIVATION_REQUEST_KPI = BranchPythonOperator(
    task_id='branchid_SMS_ACTIVATION_REQUEST_KPI',
    python_callable=branch_SMS_ACTIVATION_REQUEST_KPI,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='PCF_SMS_ACTIVATION_REQUEST_KPI',
    bash_command= pcf_command_SMS_ACTIVATION_REQUEST_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='Dedup_SMS_ACTIVATION_REQUEST_KPI',
    bash_command= dedup_command_SMS_ACTIVATION_REQUEST_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='Dedup2_SMS_ACTIVATION_REQUEST_KPI',
    bash_command= dedup_command_SMS_ACTIVATION_REQUEST_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='PCFCheck_SMS_ACTIVATION_REQUEST_KPI',
    bash_command=PCFCheck_command_SMS_ACTIVATION_REQUEST_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_SMS_ACTIVATION_REQUEST_KPI = EmailOperator(
    task_id='faile_SMS_ACTIVATION_REQUEST_KPI',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMS_ACTIVATION_REQUEST_KPI),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_SMS_ACTIVATION_REQUEST_KPI >> PCF_SMS_ACTIVATION_REQUEST_KPI >> PCFCheck_SMS_ACTIVATION_REQUEST_KPI  >> Dedup_SMS_ACTIVATION_REQUEST_KPI >> Success
Branching_SMS_ACTIVATION_REQUEST_KPI >> Dedup2_SMS_ACTIVATION_REQUEST_KPI >> Success
Branching_SMS_ACTIVATION_REQUEST_KPI >> faile_SMS_ACTIVATION_REQUEST_KPI
Branching_SMS_ACTIVATION_REQUEST_KPI >> Success
feedname_RECHARGE_SUCCESS_RATE = 'RECHARGE_SUCCESS_RATE'.upper()
feedname2_RECHARGE_SUCCESS_RATE = 'RECHARGE_SUCCESS_RATE'.lower()

dedup_command_RECHARGE_SUCCESS_RATE="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_RECHARGE_SUCCESS_RATE)
logging.info(dedup_command_RECHARGE_SUCCESS_RATE)


pcf_command_RECHARGE_SUCCESS_RATE = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_RECHARGE_SUCCESS_RATE)
logging.info(pcf_command_RECHARGE_SUCCESS_RATE)

PCFCheck_command_RECHARGE_SUCCESS_RATE = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RECHARGE_SUCCESS_RATE,run_date,sleeptime)
logging.info(PCFCheck_command_RECHARGE_SUCCESS_RATE)


branchScript_RECHARGE_SUCCESS_RATE = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_RECHARGE_SUCCESS_RATE,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_RECHARGE_SUCCESS_RATE)

def branch_RECHARGE_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RECHARGE_SUCCESS_RATE,feedname2_RECHARGE_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RECHARGE_SUCCESS_RATE)
    x = readcsv(filepath,feedname_RECHARGE_SUCCESS_RATE)
    logging.info('branch_RECHARGE_SUCCESS_RATE' + x)
    return x

Branching_RECHARGE_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_RECHARGE_SUCCESS_RATE',
    python_callable=branch_RECHARGE_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='PCF_RECHARGE_SUCCESS_RATE',
    bash_command= pcf_command_RECHARGE_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='Dedup_RECHARGE_SUCCESS_RATE',
    bash_command= dedup_command_RECHARGE_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_RECHARGE_SUCCESS_RATE',
    bash_command= dedup_command_RECHARGE_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_RECHARGE_SUCCESS_RATE',
    bash_command=PCFCheck_command_RECHARGE_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_RECHARGE_SUCCESS_RATE = EmailOperator(
    task_id='faile_RECHARGE_SUCCESS_RATE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RECHARGE_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_RECHARGE_SUCCESS_RATE >> PCF_RECHARGE_SUCCESS_RATE >> PCFCheck_RECHARGE_SUCCESS_RATE  >> Dedup_RECHARGE_SUCCESS_RATE >> Success
Branching_RECHARGE_SUCCESS_RATE >> Dedup2_RECHARGE_SUCCESS_RATE >> Success
Branching_RECHARGE_SUCCESS_RATE >> faile_RECHARGE_SUCCESS_RATE
Branching_RECHARGE_SUCCESS_RATE >> Success
feedname_USSD_ERROR_CODE_BREAKDOWN = 'USSD_ERROR_CODE_BREAKDOWN'.upper()
feedname2_USSD_ERROR_CODE_BREAKDOWN = 'USSD_ERROR_CODE_BREAKDOWN'.lower()

dedup_command_USSD_ERROR_CODE_BREAKDOWN="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_USSD_ERROR_CODE_BREAKDOWN)
logging.info(dedup_command_USSD_ERROR_CODE_BREAKDOWN)


pcf_command_USSD_ERROR_CODE_BREAKDOWN = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_USSD_ERROR_CODE_BREAKDOWN)
logging.info(pcf_command_USSD_ERROR_CODE_BREAKDOWN)

PCFCheck_command_USSD_ERROR_CODE_BREAKDOWN = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_USSD_ERROR_CODE_BREAKDOWN,run_date,sleeptime)
logging.info(PCFCheck_command_USSD_ERROR_CODE_BREAKDOWN)


branchScript_USSD_ERROR_CODE_BREAKDOWN = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_USSD_ERROR_CODE_BREAKDOWN,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_USSD_ERROR_CODE_BREAKDOWN)

def branch_USSD_ERROR_CODE_BREAKDOWN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_USSD_ERROR_CODE_BREAKDOWN,feedname2_USSD_ERROR_CODE_BREAKDOWN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_USSD_ERROR_CODE_BREAKDOWN)
    x = readcsv(filepath,feedname_USSD_ERROR_CODE_BREAKDOWN)
    logging.info('branch_USSD_ERROR_CODE_BREAKDOWN' + x)
    return x

Branching_USSD_ERROR_CODE_BREAKDOWN = BranchPythonOperator(
    task_id='branchid_USSD_ERROR_CODE_BREAKDOWN',
    python_callable=branch_USSD_ERROR_CODE_BREAKDOWN,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='PCF_USSD_ERROR_CODE_BREAKDOWN',
    bash_command= pcf_command_USSD_ERROR_CODE_BREAKDOWN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='Dedup_USSD_ERROR_CODE_BREAKDOWN',
    bash_command= dedup_command_USSD_ERROR_CODE_BREAKDOWN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='Dedup2_USSD_ERROR_CODE_BREAKDOWN',
    bash_command= dedup_command_USSD_ERROR_CODE_BREAKDOWN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='PCFCheck_USSD_ERROR_CODE_BREAKDOWN',
    bash_command=PCFCheck_command_USSD_ERROR_CODE_BREAKDOWN,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_USSD_ERROR_CODE_BREAKDOWN = EmailOperator(
    task_id='faile_USSD_ERROR_CODE_BREAKDOWN',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_USSD_ERROR_CODE_BREAKDOWN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_USSD_ERROR_CODE_BREAKDOWN >> PCF_USSD_ERROR_CODE_BREAKDOWN >> PCFCheck_USSD_ERROR_CODE_BREAKDOWN  >> Dedup_USSD_ERROR_CODE_BREAKDOWN >> Success
Branching_USSD_ERROR_CODE_BREAKDOWN >> Dedup2_USSD_ERROR_CODE_BREAKDOWN >> Success
Branching_USSD_ERROR_CODE_BREAKDOWN >> faile_USSD_ERROR_CODE_BREAKDOWN
Branching_USSD_ERROR_CODE_BREAKDOWN >> Success
feedname_AUDIT_LOGS = 'AUDIT_LOGS'.upper()
feedname2_AUDIT_LOGS = 'AUDIT_LOGS'.lower()

dedup_command_AUDIT_LOGS="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_AUDIT_LOGS)
logging.info(dedup_command_AUDIT_LOGS)


pcf_command_AUDIT_LOGS = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_AUDIT_LOGS)
logging.info(pcf_command_AUDIT_LOGS)

PCFCheck_command_AUDIT_LOGS = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AUDIT_LOGS,run_date,sleeptime)
logging.info(PCFCheck_command_AUDIT_LOGS)


branchScript_AUDIT_LOGS = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_AUDIT_LOGS,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_AUDIT_LOGS)

def branch_AUDIT_LOGS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AUDIT_LOGS,feedname2_AUDIT_LOGS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AUDIT_LOGS)
    x = readcsv(filepath,feedname_AUDIT_LOGS)
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
    run_as_owner=True,
    priority_weight=1
)

Dedup_AUDIT_LOGS = BashOperator(
    task_id='Dedup_AUDIT_LOGS',
    bash_command= dedup_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_AUDIT_LOGS = BashOperator(
    task_id='Dedup2_AUDIT_LOGS',
    bash_command= dedup_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_AUDIT_LOGS = BashOperator(
    task_id='PCFCheck_AUDIT_LOGS',
    bash_command=PCFCheck_command_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
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

Branching_AUDIT_LOGS >> PCF_AUDIT_LOGS >> PCFCheck_AUDIT_LOGS  >> Dedup_AUDIT_LOGS >> Success
Branching_AUDIT_LOGS >> Dedup2_AUDIT_LOGS >> Success
Branching_AUDIT_LOGS >> faile_AUDIT_LOGS
Branching_AUDIT_LOGS >> Success
feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = 'DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE'.upper()
feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = 'DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE'.lower()

dedup_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
logging.info(dedup_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)


pcf_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
logging.info(pcf_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)

PCFCheck_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,run_date,sleeptime)
logging.info(PCFCheck_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)


branchScript_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)

def branch_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
    x = readcsv(filepath,feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
    logging.info('branch_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE' + x)
    return x

Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BranchPythonOperator(
    task_id='branchid_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    python_callable=branch_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='PCF_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command= pcf_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='Dedup_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command= dedup_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='Dedup2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command= dedup_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='PCFCheck_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command=PCFCheck_command_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = EmailOperator(
    task_id='faile_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> PCF_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> PCFCheck_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE  >> Dedup_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Success
Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Dedup2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Success
Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> faile_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE
Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Success
feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = 'SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE'.upper()
feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = 'SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE'.lower()

dedup_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
logging.info(dedup_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)


pcf_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
logging.info(pcf_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)

PCFCheck_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,run_date,sleeptime)
logging.info(PCFCheck_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)


branchScript_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)

def branch_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
    x = readcsv(filepath,feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
    logging.info('branch_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE' + x)
    return x

Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    python_callable=branch_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='PCF_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command= pcf_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='Dedup_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command= dedup_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command= dedup_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command=PCFCheck_command_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = EmailOperator(
    task_id='faile_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> PCF_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> PCFCheck_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE  >> Dedup_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Success
Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Dedup2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Success
Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> faile_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE
Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Success
feedname_EMM_DELIVERY_KPI = 'EMM_DELIVERY_KPI'.upper()
feedname2_EMM_DELIVERY_KPI = 'EMM_DELIVERY_KPI'.lower()

dedup_command_EMM_DELIVERY_KPI="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_EMM_DELIVERY_KPI)
logging.info(dedup_command_EMM_DELIVERY_KPI)


pcf_command_EMM_DELIVERY_KPI = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_EMM_DELIVERY_KPI)
logging.info(pcf_command_EMM_DELIVERY_KPI)

PCFCheck_command_EMM_DELIVERY_KPI = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EMM_DELIVERY_KPI,run_date,sleeptime)
logging.info(PCFCheck_command_EMM_DELIVERY_KPI)


branchScript_EMM_DELIVERY_KPI = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_EMM_DELIVERY_KPI,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_EMM_DELIVERY_KPI)

def branch_EMM_DELIVERY_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EMM_DELIVERY_KPI,feedname2_EMM_DELIVERY_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EMM_DELIVERY_KPI)
    x = readcsv(filepath,feedname_EMM_DELIVERY_KPI)
    logging.info('branch_EMM_DELIVERY_KPI' + x)
    return x

Branching_EMM_DELIVERY_KPI = BranchPythonOperator(
    task_id='branchid_EMM_DELIVERY_KPI',
    python_callable=branch_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_EMM_DELIVERY_KPI = BashOperator(
    task_id='PCF_EMM_DELIVERY_KPI',
    bash_command= pcf_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_EMM_DELIVERY_KPI = BashOperator(
    task_id='Dedup_EMM_DELIVERY_KPI',
    bash_command= dedup_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_EMM_DELIVERY_KPI = BashOperator(
    task_id='Dedup2_EMM_DELIVERY_KPI',
    bash_command= dedup_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_EMM_DELIVERY_KPI = BashOperator(
    task_id='PCFCheck_EMM_DELIVERY_KPI',
    bash_command=PCFCheck_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_EMM_DELIVERY_KPI = EmailOperator(
    task_id='faile_EMM_DELIVERY_KPI',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_EMM_DELIVERY_KPI),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_EMM_DELIVERY_KPI >> PCF_EMM_DELIVERY_KPI >> PCFCheck_EMM_DELIVERY_KPI  >> Dedup_EMM_DELIVERY_KPI >> Success
Branching_EMM_DELIVERY_KPI >> Dedup2_EMM_DELIVERY_KPI >> Success
Branching_EMM_DELIVERY_KPI >> faile_EMM_DELIVERY_KPI
Branching_EMM_DELIVERY_KPI >> Success
feedname_FINANCIAL_LOG = 'FINANCIAL_LOG'.upper()
feedname2_FINANCIAL_LOG = 'FINANCIAL_LOG'.lower()

dedup_command_FINANCIAL_LOG="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_FINANCIAL_LOG)
logging.info(dedup_command_FINANCIAL_LOG)


pcf_command_FINANCIAL_LOG = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_FINANCIAL_LOG)
logging.info(pcf_command_FINANCIAL_LOG)

PCFCheck_command_FINANCIAL_LOG = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_FINANCIAL_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_FINANCIAL_LOG)


branchScript_FINANCIAL_LOG = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_FINANCIAL_LOG,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_FINANCIAL_LOG)

def branch_FINANCIAL_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_FINANCIAL_LOG,feedname2_FINANCIAL_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_FINANCIAL_LOG)
    x = readcsv(filepath,feedname_FINANCIAL_LOG)
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
    run_as_owner=True,
    priority_weight=1
)

Dedup_FINANCIAL_LOG = BashOperator(
    task_id='Dedup_FINANCIAL_LOG',
    bash_command= dedup_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_FINANCIAL_LOG = BashOperator(
    task_id='Dedup2_FINANCIAL_LOG',
    bash_command= dedup_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_FINANCIAL_LOG = BashOperator(
    task_id='PCFCheck_FINANCIAL_LOG',
    bash_command=PCFCheck_command_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
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

Branching_FINANCIAL_LOG >> PCF_FINANCIAL_LOG >> PCFCheck_FINANCIAL_LOG  >> Dedup_FINANCIAL_LOG >> Success
Branching_FINANCIAL_LOG >> Dedup2_FINANCIAL_LOG >> Success
Branching_FINANCIAL_LOG >> faile_FINANCIAL_LOG
Branching_FINANCIAL_LOG >> Success
feedname_MANUAL_KPI = 'MANUAL_KPI'.upper()
feedname2_MANUAL_KPI = 'MANUAL_KPI'.lower()

dedup_command_MANUAL_KPI="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_MANUAL_KPI)
logging.info(dedup_command_MANUAL_KPI)


pcf_command_MANUAL_KPI = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_MANUAL_KPI)
logging.info(pcf_command_MANUAL_KPI)

PCFCheck_command_MANUAL_KPI = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MANUAL_KPI,run_date,sleeptime)
logging.info(PCFCheck_command_MANUAL_KPI)


branchScript_MANUAL_KPI = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_MANUAL_KPI,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_MANUAL_KPI)

def branch_MANUAL_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MANUAL_KPI,feedname2_MANUAL_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MANUAL_KPI)
    x = readcsv(filepath,feedname_MANUAL_KPI)
    logging.info('branch_MANUAL_KPI' + x)
    return x

Branching_MANUAL_KPI = BranchPythonOperator(
    task_id='branchid_MANUAL_KPI',
    python_callable=branch_MANUAL_KPI,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MANUAL_KPI = BashOperator(
    task_id='PCF_MANUAL_KPI',
    bash_command= pcf_command_MANUAL_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_MANUAL_KPI = BashOperator(
    task_id='Dedup_MANUAL_KPI',
    bash_command= dedup_command_MANUAL_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_MANUAL_KPI = BashOperator(
    task_id='Dedup2_MANUAL_KPI',
    bash_command= dedup_command_MANUAL_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_MANUAL_KPI = BashOperator(
    task_id='PCFCheck_MANUAL_KPI',
    bash_command=PCFCheck_command_MANUAL_KPI,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_MANUAL_KPI = EmailOperator(
    task_id='faile_MANUAL_KPI',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MANUAL_KPI),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_MANUAL_KPI >> PCF_MANUAL_KPI >> PCFCheck_MANUAL_KPI  >> Dedup_MANUAL_KPI >> Success
Branching_MANUAL_KPI >> Dedup2_MANUAL_KPI >> Success
Branching_MANUAL_KPI >> faile_MANUAL_KPI
Branching_MANUAL_KPI >> Success
feedname_ECW_TRANSACTION = 'ECW_TRANSACTION'.upper()
feedname2_ECW_TRANSACTION = 'ECW_TRANSACTION'.lower()

dedup_command_ECW_TRANSACTION="python2 /nas/share05/opsScripts/dedupPartitions.py %s  " % (feedname_ECW_TRANSACTION)
logging.info(dedup_command_ECW_TRANSACTION)


pcf_command_ECW_TRANSACTION = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_ECW_TRANSACTION)
logging.info(pcf_command_ECW_TRANSACTION)

PCFCheck_command_ECW_TRANSACTION = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ECW_TRANSACTION,run_date,sleeptime)
logging.info(PCFCheck_command_ECW_TRANSACTION)


branchScript_ECW_TRANSACTION = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation_2.11-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}".format(feedname2_ECW_TRANSACTION,d_1,sleeptime,pathcsv,run_date,hour_run)
logging.info(branchScript_ECW_TRANSACTION)

def branch_ECW_TRANSACTION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ECW_TRANSACTION,feedname2_ECW_TRANSACTION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ECW_TRANSACTION)
    x = readcsv(filepath,feedname_ECW_TRANSACTION)
    logging.info('branch_ECW_TRANSACTION' + x)
    return x

Branching_ECW_TRANSACTION = BranchPythonOperator(
    task_id='branchid_ECW_TRANSACTION',
    python_callable=branch_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ECW_TRANSACTION = BashOperator(
    task_id='PCF_ECW_TRANSACTION',
    bash_command= pcf_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

Dedup_ECW_TRANSACTION = BashOperator(
    task_id='Dedup_ECW_TRANSACTION',
    bash_command= dedup_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)
Dedup2_ECW_TRANSACTION = BashOperator(
    task_id='Dedup2_ECW_TRANSACTION',
    bash_command= dedup_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

PCFCheck_ECW_TRANSACTION = BashOperator(
    task_id='PCFCheck_ECW_TRANSACTION',
    bash_command=PCFCheck_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    run_as_owner=True,
    priority_weight=1
)

faile_ECW_TRANSACTION = EmailOperator(
    task_id='faile_ECW_TRANSACTION',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ECW_TRANSACTION),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_ECW_TRANSACTION >> PCF_ECW_TRANSACTION >> PCFCheck_ECW_TRANSACTION  >> Dedup_ECW_TRANSACTION >> Success
Branching_ECW_TRANSACTION >> Dedup2_ECW_TRANSACTION >> Success
Branching_ECW_TRANSACTION >> faile_ECW_TRANSACTION
Branching_ECW_TRANSACTION >> Success

