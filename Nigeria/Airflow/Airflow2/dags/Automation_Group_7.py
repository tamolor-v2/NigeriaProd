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
d_1 = Variable.get("rerunDate7", deserialize_json=True)
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

dag = DAG('New_Automation_Group_7', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group7 -c config.json' %(d_1)
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


feedname_EDW_SIM_SWAP = 'EDW_SIM_SWAP'.upper()
feedname2_EDW_SIM_SWAP = 'EDW_SIM_SWAP'.lower()

dedup_command_EDW_SIM_SWAP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_EDW_SIM_SWAP)
logging.info(dedup_command_EDW_SIM_SWAP) 

pcf_command_EDW_SIM_SWAP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_EDW_SIM_SWAP)
logging.info(pcf_command_EDW_SIM_SWAP)

PCFCheck_command_EDW_SIM_SWAP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EDW_SIM_SWAP,run_date,sleeptime)
logging.info(PCFCheck_command_EDW_SIM_SWAP)

branchScript_EDW_SIM_SWAP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_EDW_SIM_SWAP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_EDW_SIM_SWAP)

def branch_EDW_SIM_SWAP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EDW_SIM_SWAP,feedname2_EDW_SIM_SWAP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EDW_SIM_SWAP)
    x = readcsv(filepath,feedname2_EDW_SIM_SWAP)
    logging.info('branch_EDW_SIM_SWAP' + x)
    return x

Branching_EDW_SIM_SWAP = BranchPythonOperator(
    task_id='branchid_EDW_SIM_SWAP',
    python_callable=branch_EDW_SIM_SWAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_EDW_SIM_SWAP = BashOperator(
    task_id='PCF_EDW_SIM_SWAP',
    bash_command= pcf_command_EDW_SIM_SWAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_EDW_SIM_SWAP = BashOperator(
    task_id='Dedup_EDW_SIM_SWAP',
    bash_command= dedup_command_EDW_SIM_SWAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_EDW_SIM_SWAP = BashOperator(
    task_id='Dedup2_EDW_SIM_SWAP',
    bash_command= dedup_command_EDW_SIM_SWAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_EDW_SIM_SWAP = BashOperator(
    task_id='PCFCheck_EDW_SIM_SWAP',
    bash_command=PCFCheck_command_EDW_SIM_SWAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_EDW_SIM_SWAP = EmailOperator(
    task_id='faile_EDW_SIM_SWAP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_EDW_SIM_SWAP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_EDW_SIM_SWAP: PythonOperator = PythonOperator(task_id="waitForFlush_EDW_SIM_SWAP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_EDW_SIM_SWAP >> PCF_EDW_SIM_SWAP >> PCFCheck_EDW_SIM_SWAP >> delay_python_task_EDW_SIM_SWAP >> Dedup_EDW_SIM_SWAP >> Validation_End 
Branching_EDW_SIM_SWAP >> Dedup2_EDW_SIM_SWAP >> Validation_End
Branching_EDW_SIM_SWAP >> faile_EDW_SIM_SWAP
Branching_EDW_SIM_SWAP >> Validation_End


feedname_TAS_SECONDARY_SALES_DATA = 'TAS_SECONDARY_SALES_DATA'.upper()
feedname2_TAS_SECONDARY_SALES_DATA = 'TAS_SECONDARY_SALES_DATA'.lower()

dedup_command_TAS_SECONDARY_SALES_DATA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_SECONDARY_SALES_DATA)
logging.info(dedup_command_TAS_SECONDARY_SALES_DATA) 

pcf_command_TAS_SECONDARY_SALES_DATA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_SECONDARY_SALES_DATA)
logging.info(pcf_command_TAS_SECONDARY_SALES_DATA)

PCFCheck_command_TAS_SECONDARY_SALES_DATA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_SECONDARY_SALES_DATA,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_SECONDARY_SALES_DATA)

branchScript_TAS_SECONDARY_SALES_DATA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_SECONDARY_SALES_DATA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_SECONDARY_SALES_DATA)

def branch_TAS_SECONDARY_SALES_DATA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_SECONDARY_SALES_DATA,feedname2_TAS_SECONDARY_SALES_DATA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_SECONDARY_SALES_DATA)
    x = readcsv(filepath,feedname2_TAS_SECONDARY_SALES_DATA)
    logging.info('branch_TAS_SECONDARY_SALES_DATA' + x)
    return x

Branching_TAS_SECONDARY_SALES_DATA = BranchPythonOperator(
    task_id='branchid_TAS_SECONDARY_SALES_DATA',
    python_callable=branch_TAS_SECONDARY_SALES_DATA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_SECONDARY_SALES_DATA = BashOperator(
    task_id='PCF_TAS_SECONDARY_SALES_DATA',
    bash_command= pcf_command_TAS_SECONDARY_SALES_DATA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_SECONDARY_SALES_DATA = BashOperator(
    task_id='Dedup_TAS_SECONDARY_SALES_DATA',
    bash_command= dedup_command_TAS_SECONDARY_SALES_DATA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_SECONDARY_SALES_DATA = BashOperator(
    task_id='Dedup2_TAS_SECONDARY_SALES_DATA',
    bash_command= dedup_command_TAS_SECONDARY_SALES_DATA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_SECONDARY_SALES_DATA = BashOperator(
    task_id='PCFCheck_TAS_SECONDARY_SALES_DATA',
    bash_command=PCFCheck_command_TAS_SECONDARY_SALES_DATA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_SECONDARY_SALES_DATA = EmailOperator(
    task_id='faile_TAS_SECONDARY_SALES_DATA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_SECONDARY_SALES_DATA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_SECONDARY_SALES_DATA: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_SECONDARY_SALES_DATA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_SECONDARY_SALES_DATA >> PCF_TAS_SECONDARY_SALES_DATA >> PCFCheck_TAS_SECONDARY_SALES_DATA >> delay_python_task_TAS_SECONDARY_SALES_DATA >> Dedup_TAS_SECONDARY_SALES_DATA >> Validation_End 
Branching_TAS_SECONDARY_SALES_DATA >> Dedup2_TAS_SECONDARY_SALES_DATA >> Validation_End
Branching_TAS_SECONDARY_SALES_DATA >> faile_TAS_SECONDARY_SALES_DATA
Branching_TAS_SECONDARY_SALES_DATA >> Validation_End


feedname_NIN_AUDIT_LINK = 'NIN_AUDIT_LINK'.upper()
feedname2_NIN_AUDIT_LINK = 'NIN_AUDIT_LINK'.lower()

dedup_command_NIN_AUDIT_LINK="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_NIN_AUDIT_LINK)
logging.info(dedup_command_NIN_AUDIT_LINK) 

pcf_command_NIN_AUDIT_LINK = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_NIN_AUDIT_LINK)
logging.info(pcf_command_NIN_AUDIT_LINK)

PCFCheck_command_NIN_AUDIT_LINK = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NIN_AUDIT_LINK,run_date,sleeptime)
logging.info(PCFCheck_command_NIN_AUDIT_LINK)

branchScript_NIN_AUDIT_LINK = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_NIN_AUDIT_LINK,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_NIN_AUDIT_LINK)

def branch_NIN_AUDIT_LINK():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NIN_AUDIT_LINK,feedname2_NIN_AUDIT_LINK)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NIN_AUDIT_LINK)
    x = readcsv(filepath,feedname2_NIN_AUDIT_LINK)
    logging.info('branch_NIN_AUDIT_LINK' + x)
    return x

Branching_NIN_AUDIT_LINK = BranchPythonOperator(
    task_id='branchid_NIN_AUDIT_LINK',
    python_callable=branch_NIN_AUDIT_LINK,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_NIN_AUDIT_LINK = BashOperator(
    task_id='PCF_NIN_AUDIT_LINK',
    bash_command= pcf_command_NIN_AUDIT_LINK,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_NIN_AUDIT_LINK = BashOperator(
    task_id='Dedup_NIN_AUDIT_LINK',
    bash_command= dedup_command_NIN_AUDIT_LINK,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_NIN_AUDIT_LINK = BashOperator(
    task_id='Dedup2_NIN_AUDIT_LINK',
    bash_command= dedup_command_NIN_AUDIT_LINK,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_NIN_AUDIT_LINK = BashOperator(
    task_id='PCFCheck_NIN_AUDIT_LINK',
    bash_command=PCFCheck_command_NIN_AUDIT_LINK,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_NIN_AUDIT_LINK = EmailOperator(
    task_id='faile_NIN_AUDIT_LINK',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_NIN_AUDIT_LINK),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_NIN_AUDIT_LINK: PythonOperator = PythonOperator(task_id="waitForFlush_NIN_AUDIT_LINK",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_NIN_AUDIT_LINK >> PCF_NIN_AUDIT_LINK >> PCFCheck_NIN_AUDIT_LINK >> delay_python_task_NIN_AUDIT_LINK >> Dedup_NIN_AUDIT_LINK >> Validation_End 
Branching_NIN_AUDIT_LINK >> Dedup2_NIN_AUDIT_LINK >> Validation_End
Branching_NIN_AUDIT_LINK >> faile_NIN_AUDIT_LINK
Branching_NIN_AUDIT_LINK >> Validation_End


feedname_IVR_SERVICE = 'IVR_SERVICE'.upper()
feedname2_IVR_SERVICE = 'IVR_SERVICE'.lower()

dedup_command_IVR_SERVICE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_IVR_SERVICE)
logging.info(dedup_command_IVR_SERVICE) 

pcf_command_IVR_SERVICE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_IVR_SERVICE)
logging.info(pcf_command_IVR_SERVICE)

PCFCheck_command_IVR_SERVICE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_IVR_SERVICE,run_date,sleeptime)
logging.info(PCFCheck_command_IVR_SERVICE)

branchScript_IVR_SERVICE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_IVR_SERVICE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_IVR_SERVICE)

def branch_IVR_SERVICE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_IVR_SERVICE,feedname2_IVR_SERVICE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_IVR_SERVICE)
    x = readcsv(filepath,feedname2_IVR_SERVICE)
    logging.info('branch_IVR_SERVICE' + x)
    return x

Branching_IVR_SERVICE = BranchPythonOperator(
    task_id='branchid_IVR_SERVICE',
    python_callable=branch_IVR_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_IVR_SERVICE = BashOperator(
    task_id='PCF_IVR_SERVICE',
    bash_command= pcf_command_IVR_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_IVR_SERVICE = BashOperator(
    task_id='Dedup_IVR_SERVICE',
    bash_command= dedup_command_IVR_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_IVR_SERVICE = BashOperator(
    task_id='Dedup2_IVR_SERVICE',
    bash_command= dedup_command_IVR_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_IVR_SERVICE = BashOperator(
    task_id='PCFCheck_IVR_SERVICE',
    bash_command=PCFCheck_command_IVR_SERVICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_IVR_SERVICE = EmailOperator(
    task_id='faile_IVR_SERVICE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_IVR_SERVICE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_IVR_SERVICE: PythonOperator = PythonOperator(task_id="waitForFlush_IVR_SERVICE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_IVR_SERVICE >> PCF_IVR_SERVICE >> PCFCheck_IVR_SERVICE >> delay_python_task_IVR_SERVICE >> Dedup_IVR_SERVICE >> Validation_End 
Branching_IVR_SERVICE >> Dedup2_IVR_SERVICE >> Validation_End
Branching_IVR_SERVICE >> faile_IVR_SERVICE
Branching_IVR_SERVICE >> Validation_End


feedname_MTNF_SURVEYS = 'MTNF_SURVEYS'.upper()
feedname2_MTNF_SURVEYS = 'MTNF_SURVEYS'.lower()

dedup_command_MTNF_SURVEYS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MTNF_SURVEYS)
logging.info(dedup_command_MTNF_SURVEYS) 

pcf_command_MTNF_SURVEYS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MTNF_SURVEYS)
logging.info(pcf_command_MTNF_SURVEYS)

PCFCheck_command_MTNF_SURVEYS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MTNF_SURVEYS,run_date,sleeptime)
logging.info(PCFCheck_command_MTNF_SURVEYS)

branchScript_MTNF_SURVEYS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MTNF_SURVEYS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MTNF_SURVEYS)

def branch_MTNF_SURVEYS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MTNF_SURVEYS,feedname2_MTNF_SURVEYS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MTNF_SURVEYS)
    x = readcsv(filepath,feedname2_MTNF_SURVEYS)
    logging.info('branch_MTNF_SURVEYS' + x)
    return x

Branching_MTNF_SURVEYS = BranchPythonOperator(
    task_id='branchid_MTNF_SURVEYS',
    python_callable=branch_MTNF_SURVEYS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MTNF_SURVEYS = BashOperator(
    task_id='PCF_MTNF_SURVEYS',
    bash_command= pcf_command_MTNF_SURVEYS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MTNF_SURVEYS = BashOperator(
    task_id='Dedup_MTNF_SURVEYS',
    bash_command= dedup_command_MTNF_SURVEYS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MTNF_SURVEYS = BashOperator(
    task_id='Dedup2_MTNF_SURVEYS',
    bash_command= dedup_command_MTNF_SURVEYS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MTNF_SURVEYS = BashOperator(
    task_id='PCFCheck_MTNF_SURVEYS',
    bash_command=PCFCheck_command_MTNF_SURVEYS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MTNF_SURVEYS = EmailOperator(
    task_id='faile_MTNF_SURVEYS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MTNF_SURVEYS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MTNF_SURVEYS: PythonOperator = PythonOperator(task_id="waitForFlush_MTNF_SURVEYS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MTNF_SURVEYS >> PCF_MTNF_SURVEYS >> PCFCheck_MTNF_SURVEYS >> delay_python_task_MTNF_SURVEYS >> Dedup_MTNF_SURVEYS >> Validation_End 
Branching_MTNF_SURVEYS >> Dedup2_MTNF_SURVEYS >> Validation_End
Branching_MTNF_SURVEYS >> faile_MTNF_SURVEYS
Branching_MTNF_SURVEYS >> Validation_End


feedname_RDS_ACCOUNT_HOLDER = 'RDS_ACCOUNT_HOLDER'.upper()
feedname2_RDS_ACCOUNT_HOLDER = 'RDS_ACCOUNT_HOLDER'.lower()

dedup_command_RDS_ACCOUNT_HOLDER="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_ACCOUNT_HOLDER)
logging.info(dedup_command_RDS_ACCOUNT_HOLDER) 

pcf_command_RDS_ACCOUNT_HOLDER = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_ACCOUNT_HOLDER)
logging.info(pcf_command_RDS_ACCOUNT_HOLDER)

PCFCheck_command_RDS_ACCOUNT_HOLDER = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_ACCOUNT_HOLDER,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_ACCOUNT_HOLDER)

branchScript_RDS_ACCOUNT_HOLDER = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_ACCOUNT_HOLDER,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_ACCOUNT_HOLDER)

def branch_RDS_ACCOUNT_HOLDER():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_ACCOUNT_HOLDER,feedname2_RDS_ACCOUNT_HOLDER)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_ACCOUNT_HOLDER)
    x = readcsv(filepath,feedname2_RDS_ACCOUNT_HOLDER)
    logging.info('branch_RDS_ACCOUNT_HOLDER' + x)
    return x

Branching_RDS_ACCOUNT_HOLDER = BranchPythonOperator(
    task_id='branchid_RDS_ACCOUNT_HOLDER',
    python_callable=branch_RDS_ACCOUNT_HOLDER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_ACCOUNT_HOLDER = BashOperator(
    task_id='PCF_RDS_ACCOUNT_HOLDER',
    bash_command= pcf_command_RDS_ACCOUNT_HOLDER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_ACCOUNT_HOLDER = BashOperator(
    task_id='Dedup_RDS_ACCOUNT_HOLDER',
    bash_command= dedup_command_RDS_ACCOUNT_HOLDER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_ACCOUNT_HOLDER = BashOperator(
    task_id='Dedup2_RDS_ACCOUNT_HOLDER',
    bash_command= dedup_command_RDS_ACCOUNT_HOLDER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_ACCOUNT_HOLDER = BashOperator(
    task_id='PCFCheck_RDS_ACCOUNT_HOLDER',
    bash_command=PCFCheck_command_RDS_ACCOUNT_HOLDER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_ACCOUNT_HOLDER = EmailOperator(
    task_id='faile_RDS_ACCOUNT_HOLDER',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_ACCOUNT_HOLDER),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_ACCOUNT_HOLDER: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_ACCOUNT_HOLDER",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_ACCOUNT_HOLDER >> PCF_RDS_ACCOUNT_HOLDER >> PCFCheck_RDS_ACCOUNT_HOLDER >> delay_python_task_RDS_ACCOUNT_HOLDER >> Dedup_RDS_ACCOUNT_HOLDER >> Validation_End 
Branching_RDS_ACCOUNT_HOLDER >> Dedup2_RDS_ACCOUNT_HOLDER >> Validation_End
Branching_RDS_ACCOUNT_HOLDER >> faile_RDS_ACCOUNT_HOLDER
Branching_RDS_ACCOUNT_HOLDER >> Validation_End


feedname_RDS_FINANCIALRECEIPT_DETAILS = 'RDS_FINANCIALRECEIPT_DETAILS'.upper()
feedname2_RDS_FINANCIALRECEIPT_DETAILS = 'RDS_FINANCIALRECEIPT_DETAILS'.lower()

dedup_command_RDS_FINANCIALRECEIPT_DETAILS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_FINANCIALRECEIPT_DETAILS)
logging.info(dedup_command_RDS_FINANCIALRECEIPT_DETAILS) 

pcf_command_RDS_FINANCIALRECEIPT_DETAILS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_FINANCIALRECEIPT_DETAILS)
logging.info(pcf_command_RDS_FINANCIALRECEIPT_DETAILS)

PCFCheck_command_RDS_FINANCIALRECEIPT_DETAILS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_FINANCIALRECEIPT_DETAILS,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_FINANCIALRECEIPT_DETAILS)

branchScript_RDS_FINANCIALRECEIPT_DETAILS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_FINANCIALRECEIPT_DETAILS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_FINANCIALRECEIPT_DETAILS)

def branch_RDS_FINANCIALRECEIPT_DETAILS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_FINANCIALRECEIPT_DETAILS,feedname2_RDS_FINANCIALRECEIPT_DETAILS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_FINANCIALRECEIPT_DETAILS)
    x = readcsv(filepath,feedname2_RDS_FINANCIALRECEIPT_DETAILS)
    logging.info('branch_RDS_FINANCIALRECEIPT_DETAILS' + x)
    return x

Branching_RDS_FINANCIALRECEIPT_DETAILS = BranchPythonOperator(
    task_id='branchid_RDS_FINANCIALRECEIPT_DETAILS',
    python_callable=branch_RDS_FINANCIALRECEIPT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_FINANCIALRECEIPT_DETAILS = BashOperator(
    task_id='PCF_RDS_FINANCIALRECEIPT_DETAILS',
    bash_command= pcf_command_RDS_FINANCIALRECEIPT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_FINANCIALRECEIPT_DETAILS = BashOperator(
    task_id='Dedup_RDS_FINANCIALRECEIPT_DETAILS',
    bash_command= dedup_command_RDS_FINANCIALRECEIPT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_FINANCIALRECEIPT_DETAILS = BashOperator(
    task_id='Dedup2_RDS_FINANCIALRECEIPT_DETAILS',
    bash_command= dedup_command_RDS_FINANCIALRECEIPT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_FINANCIALRECEIPT_DETAILS = BashOperator(
    task_id='PCFCheck_RDS_FINANCIALRECEIPT_DETAILS',
    bash_command=PCFCheck_command_RDS_FINANCIALRECEIPT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_FINANCIALRECEIPT_DETAILS = EmailOperator(
    task_id='faile_RDS_FINANCIALRECEIPT_DETAILS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_FINANCIALRECEIPT_DETAILS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_FINANCIALRECEIPT_DETAILS: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_FINANCIALRECEIPT_DETAILS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_FINANCIALRECEIPT_DETAILS >> PCF_RDS_FINANCIALRECEIPT_DETAILS >> PCFCheck_RDS_FINANCIALRECEIPT_DETAILS >> delay_python_task_RDS_FINANCIALRECEIPT_DETAILS >> Dedup_RDS_FINANCIALRECEIPT_DETAILS >> Validation_End 
Branching_RDS_FINANCIALRECEIPT_DETAILS >> Dedup2_RDS_FINANCIALRECEIPT_DETAILS >> Validation_End
Branching_RDS_FINANCIALRECEIPT_DETAILS >> faile_RDS_FINANCIALRECEIPT_DETAILS
Branching_RDS_FINANCIALRECEIPT_DETAILS >> Validation_End
