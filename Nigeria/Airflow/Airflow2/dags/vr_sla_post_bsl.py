import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
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
sleeptime= 2*1000*60
url='jdbc:presto://10.1.197.145:8999/hive5/flare_8'
##sleeptime= 1000*10






pathcsv = '/nas/share05/tools/DQ/status2/'
phases = ['Success','Dedup2_','PCF_','faile_']
email = ['o.olanipekun']

default_args = {
    'owner': 'VR',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun'],
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
        result = phases[1]+ feedname.upper()
    elif i == '2' :
        result = phases[2]+ feedname.upper()
    elif i == '3' :
        result = phases[3]+ feedname.upper()
    return result


def readcsv (path,feedname):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = task (line[0],feedname)
    return result

dag = DAG('VR_GROUP7', default_args=default_args, catchup=False,schedule_interval='30 11 * * *')

ValidationCode = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh %s all SLA_POST_BSL.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_airflow_%s.txt' %(d_1,dayStr)
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

feedname_MNP_PORTING_BROADCAST = 'MNP_PORTING_BROADCAST'.upper()
feedname2_MNP_PORTING_BROADCAST = 'MNP_PORTING_BROADCAST'.lower()

dedup_command_MNP_PORTING_BROADCAST="bash /nas/share05/tools/BSLAutomation/Dedup_part.sh %s %s %s " % (d_1,feedname_MNP_PORTING_BROADCAST,'false')
logging.info(dedup_command_MNP_PORTING_BROADCAST)


pcf_command_MNP_PORTING_BROADCAST = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_MNP_PORTING_BROADCAST)
logging.info(pcf_command_MNP_PORTING_BROADCAST)

PCFCheck_command_MNP_PORTING_BROADCAST = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MNP_PORTING_BROADCAST,run_date,sleeptime)
logging.info(PCFCheck_command_MNP_PORTING_BROADCAST)

branchScript_MNP_PORTING_BROADCAST = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5} -url {6}".format(feedname2_MNP_PORTING_BROADCAST,d_1,sleeptime,pathcsv,run_date,hour_run, url)
logging.info(branchScript_MNP_PORTING_BROADCAST)

def branch_MNP_PORTING_BROADCAST():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MNP_PORTING_BROADCAST,feedname2_MNP_PORTING_BROADCAST)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MNP_PORTING_BROADCAST)
    x = readcsv(filepath,feedname_MNP_PORTING_BROADCAST)
    logging.info('branch_MNP_PORTING_BROADCAST' + x)
    return x

Branching_MNP_PORTING_BROADCAST = BranchPythonOperator(
    task_id='branchid_MNP_PORTING_BROADCAST',
    python_callable=branch_MNP_PORTING_BROADCAST,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MNP_PORTING_BROADCAST = BashOperator(
    task_id='PCF_MNP_PORTING_BROADCAST',
    bash_command= pcf_command_MNP_PORTING_BROADCAST,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MNP_PORTING_BROADCAST = BashOperator(
    task_id='Dedup_MNP_PORTING_BROADCAST',
    bash_command= dedup_command_MNP_PORTING_BROADCAST,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MNP_PORTING_BROADCAST = BashOperator(
    task_id='Dedup2_MNP_PORTING_BROADCAST',
    bash_command= dedup_command_MNP_PORTING_BROADCAST,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MNP_PORTING_BROADCAST = BashOperator(
    task_id='PCFCheck_MNP_PORTING_BROADCAST',
    bash_command=PCFCheck_command_MNP_PORTING_BROADCAST,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MNP_PORTING_BROADCAST = EmailOperator(
    task_id='faile_MNP_PORTING_BROADCAST',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MNP_PORTING_BROADCAST),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_MNP_PORTING_BROADCAST >> PCF_MNP_PORTING_BROADCAST >> PCFCheck_MNP_PORTING_BROADCAST  >> Dedup_MNP_PORTING_BROADCAST >> Success
Branching_MNP_PORTING_BROADCAST >> Dedup2_MNP_PORTING_BROADCAST >> Success
Branching_MNP_PORTING_BROADCAST >> faile_MNP_PORTING_BROADCAST
Branching_MNP_PORTING_BROADCAST >> Success



feedname_NEWREG_BIOUPDT_POOL_DAILY = 'NEWREG_BIOUPDT_POOL_DAILY'.upper()
feedname2_NEWREG_BIOUPDT_POOL_DAILY = 'NEWREG_BIOUPDT_POOL_DAILY'.lower()

dedup_command_NEWREG_BIOUPDT_POOL_DAILY="bash /nas/share05/tools/BSLAutomation/Dedup_part.sh %s %s %s " % (d_1,feedname_NEWREG_BIOUPDT_POOL_DAILY,'false')
logging.info(dedup_command_NEWREG_BIOUPDT_POOL_DAILY)


pcf_command_NEWREG_BIOUPDT_POOL_DAILY = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_NEWREG_BIOUPDT_POOL_DAILY)
logging.info(pcf_command_NEWREG_BIOUPDT_POOL_DAILY)

PCFCheck_command_NEWREG_BIOUPDT_POOL_DAILY = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NEWREG_BIOUPDT_POOL_DAILY,run_date,sleeptime)
logging.info(PCFCheck_command_NEWREG_BIOUPDT_POOL_DAILY)

branchScript_NEWREG_BIOUPDT_POOL_DAILY = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5} -url {6}".format(feedname2_NEWREG_BIOUPDT_POOL_DAILY,d_1,sleeptime,pathcsv,run_date,hour_run,url)
logging.info(branchScript_NEWREG_BIOUPDT_POOL_DAILY)

def branch_NEWREG_BIOUPDT_POOL_DAILY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NEWREG_BIOUPDT_POOL_DAILY,feedname2_NEWREG_BIOUPDT_POOL_DAILY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NEWREG_BIOUPDT_POOL_DAILY)
    x = readcsv(filepath,feedname_NEWREG_BIOUPDT_POOL_DAILY)
    logging.info('branch_NEWREG_BIOUPDT_POOL_DAILY' + x)
    return x

Branching_NEWREG_BIOUPDT_POOL_DAILY = BranchPythonOperator(
    task_id='branchid_NEWREG_BIOUPDT_POOL_DAILY',
    python_callable=branch_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='PCF_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command= pcf_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='Dedup_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command= dedup_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='Dedup2_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command= dedup_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='PCFCheck_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command=PCFCheck_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_NEWREG_BIOUPDT_POOL_DAILY = EmailOperator(
    task_id='faile_NEWREG_BIOUPDT_POOL_DAILY',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_NEWREG_BIOUPDT_POOL_DAILY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_NEWREG_BIOUPDT_POOL_DAILY >> PCF_NEWREG_BIOUPDT_POOL_DAILY >> PCFCheck_NEWREG_BIOUPDT_POOL_DAILY  >> Dedup_NEWREG_BIOUPDT_POOL_DAILY >> Success
Branching_NEWREG_BIOUPDT_POOL_DAILY >> Dedup2_NEWREG_BIOUPDT_POOL_DAILY >> Success
Branching_NEWREG_BIOUPDT_POOL_DAILY >> faile_NEWREG_BIOUPDT_POOL_DAILY
Branching_NEWREG_BIOUPDT_POOL_DAILY >> Success


feedname_NEWREG_BIOUPDT_POOL_WEEKLY = 'NEWREG_BIOUPDT_POOL_WEEKLY'.upper()
feedname2_NEWREG_BIOUPDT_POOL_WEEKLY = 'NEWREG_BIOUPDT_POOL_WEEKLY'.lower()

dedup_command_NEWREG_BIOUPDT_POOL_WEEKLY="bash /nas/share05/tools/BSLAutomation/Dedup_part.sh %s %s %s " % (d_1,feedname_NEWREG_BIOUPDT_POOL_WEEKLY,'false')
logging.info(dedup_command_NEWREG_BIOUPDT_POOL_WEEKLY)


pcf_command_NEWREG_BIOUPDT_POOL_WEEKLY = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_NEWREG_BIOUPDT_POOL_WEEKLY)
logging.info(pcf_command_NEWREG_BIOUPDT_POOL_WEEKLY)

PCFCheck_command_NEWREG_BIOUPDT_POOL_WEEKLY = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NEWREG_BIOUPDT_POOL_WEEKLY,run_date,sleeptime)
logging.info(PCFCheck_command_NEWREG_BIOUPDT_POOL_WEEKLY)

branchScript_NEWREG_BIOUPDT_POOL_WEEKLY = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5} -url {6}".format(feedname2_NEWREG_BIOUPDT_POOL_WEEKLY,d_1,sleeptime,pathcsv,run_date,hour_run,url)
logging.info(branchScript_NEWREG_BIOUPDT_POOL_WEEKLY)

def branch_NEWREG_BIOUPDT_POOL_WEEKLY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NEWREG_BIOUPDT_POOL_WEEKLY,feedname2_NEWREG_BIOUPDT_POOL_WEEKLY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NEWREG_BIOUPDT_POOL_WEEKLY)
    x = readcsv(filepath,feedname_NEWREG_BIOUPDT_POOL_WEEKLY)
    logging.info('branch_NEWREG_BIOUPDT_POOL_WEEKLY' + x)
    return x

Branching_NEWREG_BIOUPDT_POOL_WEEKLY = BranchPythonOperator(
    task_id='branchid_NEWREG_BIOUPDT_POOL_WEEKLY',
    python_callable=branch_NEWREG_BIOUPDT_POOL_WEEKLY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_NEWREG_BIOUPDT_POOL_WEEKLY = BashOperator(
    task_id='PCF_NEWREG_BIOUPDT_POOL_WEEKLY',
    bash_command= pcf_command_NEWREG_BIOUPDT_POOL_WEEKLY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_NEWREG_BIOUPDT_POOL_WEEKLY = BashOperator(
    task_id='Dedup_NEWREG_BIOUPDT_POOL_WEEKLY',
    bash_command= dedup_command_NEWREG_BIOUPDT_POOL_WEEKLY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_NEWREG_BIOUPDT_POOL_WEEKLY = BashOperator(
    task_id='Dedup2_NEWREG_BIOUPDT_POOL_WEEKLY',
    bash_command= dedup_command_NEWREG_BIOUPDT_POOL_WEEKLY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_NEWREG_BIOUPDT_POOL_WEEKLY = BashOperator(
    task_id='PCFCheck_NEWREG_BIOUPDT_POOL_WEEKLY',
    bash_command=PCFCheck_command_NEWREG_BIOUPDT_POOL_WEEKLY,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_NEWREG_BIOUPDT_POOL_WEEKLY = EmailOperator(
    task_id='faile_NEWREG_BIOUPDT_POOL_WEEKLY',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_NEWREG_BIOUPDT_POOL_WEEKLY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_NEWREG_BIOUPDT_POOL_WEEKLY >> PCF_NEWREG_BIOUPDT_POOL_WEEKLY >> PCFCheck_NEWREG_BIOUPDT_POOL_WEEKLY  >> Dedup_NEWREG_BIOUPDT_POOL_WEEKLY >> Success
Branching_NEWREG_BIOUPDT_POOL_WEEKLY >> Dedup2_NEWREG_BIOUPDT_POOL_WEEKLY >> Success
Branching_NEWREG_BIOUPDT_POOL_WEEKLY >> faile_NEWREG_BIOUPDT_POOL_WEEKLY
Branching_NEWREG_BIOUPDT_POOL_WEEKLY >> Success


feedname_NGVS_CDR = 'NGVS_CDR'.upper()
feedname2_NGVS_CDR = 'NGVS_CDR'.lower()

dedup_command_NGVS_CDR="bash /nas/share05/tools/BSLAutomation/Dedup_part.sh %s %s %s " % (d_1,feedname_NGVS_CDR,'false')
logging.info(dedup_command_NGVS_CDR)


pcf_command_NGVS_CDR = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_NGVS_CDR)
logging.info(pcf_command_NGVS_CDR)

PCFCheck_command_NGVS_CDR = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NGVS_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_NGVS_CDR)

branchScript_NGVS_CDR = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5} -url {6}".format(feedname2_NGVS_CDR,d_1,sleeptime,pathcsv,run_date,hour_run,url)
logging.info(branchScript_NGVS_CDR)
##logging.info(branchScript_NGVS_CDR)

def branch_NGVS_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NGVS_CDR,feedname2_NGVS_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NGVS_CDR)
    x = readcsv(filepath,feedname_NGVS_CDR)
    logging.info('branch_NGVS_CDR' + x)
    return x

Branching_NGVS_CDR = BranchPythonOperator(
    task_id='branchid_NGVS_CDR',
    python_callable=branch_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_NGVS_CDR = BashOperator(
    task_id='PCF_NGVS_CDR',
    bash_command= pcf_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_NGVS_CDR = BashOperator(
    task_id='Dedup_NGVS_CDR',
    bash_command= dedup_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_NGVS_CDR = BashOperator(
    task_id='Dedup2_NGVS_CDR',
    bash_command= dedup_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_NGVS_CDR = BashOperator(
    task_id='PCFCheck_NGVS_CDR',
    bash_command=PCFCheck_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_NGVS_CDR = EmailOperator(
    task_id='faile_NGVS_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_NGVS_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_NGVS_CDR >> PCF_NGVS_CDR >> PCFCheck_NGVS_CDR  >> Dedup_NGVS_CDR >> Success
Branching_NGVS_CDR >> Dedup2_NGVS_CDR >> Success
Branching_NGVS_CDR >> faile_NGVS_CDR
Branching_NGVS_CDR >> Success


feedname_OFFER_DUMP = 'OFFER_DUMP'.upper()
feedname2_OFFER_DUMP = 'OFFER_DUMP'.lower()

dedup_command_OFFER_DUMP="bash /nas/share05/tools/BSLAutomation/Dedup_part.sh %s %s %s " % (d_1,feedname_OFFER_DUMP,'false')
logging.info(dedup_command_OFFER_DUMP)


pcf_command_OFFER_DUMP = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_OFFER_DUMP)
logging.info(pcf_command_OFFER_DUMP)

PCFCheck_command_OFFER_DUMP = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_OFFER_DUMP,run_date,sleeptime)
logging.info(PCFCheck_command_OFFER_DUMP)

branchScript_OFFER_DUMP = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5} -url {6}".format(feedname2_OFFER_DUMP,d_1,sleeptime,pathcsv,run_date,hour_run,url)
logging.info(branchScript_OFFER_DUMP)

def branch_OFFER_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_OFFER_DUMP,feedname2_OFFER_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_OFFER_DUMP)
    x = readcsv(filepath,feedname_OFFER_DUMP)
    logging.info('branch_OFFER_DUMP' + x)
    return x

Branching_OFFER_DUMP = BranchPythonOperator(
    task_id='branchid_OFFER_DUMP',
    python_callable=branch_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_OFFER_DUMP = BashOperator(
    task_id='PCF_OFFER_DUMP',
    bash_command= pcf_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_OFFER_DUMP = BashOperator(
    task_id='Dedup_OFFER_DUMP',
    bash_command= dedup_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_OFFER_DUMP = BashOperator(
    task_id='Dedup2_OFFER_DUMP',
    bash_command= dedup_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_OFFER_DUMP = BashOperator(
    task_id='PCFCheck_OFFER_DUMP',
    bash_command=PCFCheck_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_OFFER_DUMP = EmailOperator(
    task_id='faile_OFFER_DUMP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_OFFER_DUMP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_OFFER_DUMP >> PCF_OFFER_DUMP >> PCFCheck_OFFER_DUMP  >> Dedup_OFFER_DUMP >> Success
Branching_OFFER_DUMP >> Dedup2_OFFER_DUMP >> Success
Branching_OFFER_DUMP >> faile_OFFER_DUMP
Branching_OFFER_DUMP >> Success


feedname_WBS_PM_RATED_CDRS = 'WBS_PM_RATED_CDRS'.upper()
feedname2_WBS_PM_RATED_CDRS = 'WBS_PM_RATED_CDRS'.lower()

dedup_command_WBS_PM_RATED_CDRS="bash /nas/share05/tools/BSLAutomation/Dedup_part.sh %s %s %s " % (d_1,feedname_WBS_PM_RATED_CDRS,'false')
logging.info(dedup_command_WBS_PM_RATED_CDRS)


pcf_command_WBS_PM_RATED_CDRS = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/fileOps/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery_FixedFileOpsSummaryIssue.sql -wd /tmp/ -sd %s  -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004" % (d_1,d_1,feedname_WBS_PM_RATED_CDRS)
logging.info(pcf_command_WBS_PM_RATED_CDRS)

PCFCheck_command_WBS_PM_RATED_CDRS = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_WBS_PM_RATED_CDRS,run_date,sleeptime)
logging.info(PCFCheck_command_WBS_PM_RATED_CDRS)

branchScript_WBS_PM_RATED_CDRS = "scala -cp /nas/share05/tools/DQ/lib/presto-jdbc-0.99.jar:/nas/share05/tools/DQ/lib/checkvalidation-1.0.0.jar com.ligadata.cv.CheckValidationTable -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5} -url {6}".format(feedname2_WBS_PM_RATED_CDRS,d_1,sleeptime,pathcsv,run_date,hour_run,url)
logging.info(branchScript_WBS_PM_RATED_CDRS)

def branch_WBS_PM_RATED_CDRS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_WBS_PM_RATED_CDRS,feedname2_WBS_PM_RATED_CDRS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_WBS_PM_RATED_CDRS)
    x = readcsv(filepath,feedname_WBS_PM_RATED_CDRS)
    logging.info('branch_WBS_PM_RATED_CDRS' + x)
    return x

Branching_WBS_PM_RATED_CDRS = BranchPythonOperator(
    task_id='branchid_WBS_PM_RATED_CDRS',
    python_callable=branch_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_WBS_PM_RATED_CDRS = BashOperator(
    task_id='PCF_WBS_PM_RATED_CDRS',
    bash_command= pcf_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_WBS_PM_RATED_CDRS = BashOperator(
    task_id='Dedup_WBS_PM_RATED_CDRS',
    bash_command= dedup_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_WBS_PM_RATED_CDRS = BashOperator(
    task_id='Dedup2_WBS_PM_RATED_CDRS',
    bash_command= dedup_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_WBS_PM_RATED_CDRS = BashOperator(
    task_id='PCFCheck_WBS_PM_RATED_CDRS',
    bash_command=PCFCheck_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_WBS_PM_RATED_CDRS = EmailOperator(
    task_id='faile_WBS_PM_RATED_CDRS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_WBS_PM_RATED_CDRS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_WBS_PM_RATED_CDRS >> PCF_WBS_PM_RATED_CDRS >> PCFCheck_WBS_PM_RATED_CDRS  >> Dedup_WBS_PM_RATED_CDRS >> Success
Branching_WBS_PM_RATED_CDRS >> Dedup2_WBS_PM_RATED_CDRS >> Success
Branching_WBS_PM_RATED_CDRS >> faile_WBS_PM_RATED_CDRS
Branching_WBS_PM_RATED_CDRS >> Success

