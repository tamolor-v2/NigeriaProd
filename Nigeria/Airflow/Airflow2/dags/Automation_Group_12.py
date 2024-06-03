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
d_1 = Variable.get("rerunDate12", deserialize_json=True)
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

dag = DAG('New_Automation_Group_12', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group12 -c config.json' %(d_1)
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


feedname_HSDP_DOI_LOG = 'HSDP_DOI_LOG'.upper()
feedname2_HSDP_DOI_LOG = 'HSDP_DOI_LOG'.lower()

dedup_command_HSDP_DOI_LOG="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_HSDP_DOI_LOG)
logging.info(dedup_command_HSDP_DOI_LOG) 

pcf_command_HSDP_DOI_LOG = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_HSDP_DOI_LOG)
logging.info(pcf_command_HSDP_DOI_LOG)

PCFCheck_command_HSDP_DOI_LOG = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_HSDP_DOI_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_HSDP_DOI_LOG)

branchScript_HSDP_DOI_LOG = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_HSDP_DOI_LOG,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_HSDP_DOI_LOG)

def branch_HSDP_DOI_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_HSDP_DOI_LOG,feedname2_HSDP_DOI_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_HSDP_DOI_LOG)
    x = readcsv(filepath,feedname2_HSDP_DOI_LOG)
    logging.info('branch_HSDP_DOI_LOG' + x)
    return x

Branching_HSDP_DOI_LOG = BranchPythonOperator(
    task_id='branchid_HSDP_DOI_LOG',
    python_callable=branch_HSDP_DOI_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_HSDP_DOI_LOG = BashOperator(
    task_id='PCF_HSDP_DOI_LOG',
    bash_command= pcf_command_HSDP_DOI_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_HSDP_DOI_LOG = BashOperator(
    task_id='Dedup_HSDP_DOI_LOG',
    bash_command= dedup_command_HSDP_DOI_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_HSDP_DOI_LOG = BashOperator(
    task_id='Dedup2_HSDP_DOI_LOG',
    bash_command= dedup_command_HSDP_DOI_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_HSDP_DOI_LOG = BashOperator(
    task_id='PCFCheck_HSDP_DOI_LOG',
    bash_command=PCFCheck_command_HSDP_DOI_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_HSDP_DOI_LOG = EmailOperator(
    task_id='faile_HSDP_DOI_LOG',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_HSDP_DOI_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_HSDP_DOI_LOG: PythonOperator = PythonOperator(task_id="waitForFlush_HSDP_DOI_LOG",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_HSDP_DOI_LOG >> PCF_HSDP_DOI_LOG >> PCFCheck_HSDP_DOI_LOG >> delay_python_task_HSDP_DOI_LOG >> Dedup_HSDP_DOI_LOG >> Validation_End 
Branching_HSDP_DOI_LOG >> Dedup2_HSDP_DOI_LOG >> Validation_End
Branching_HSDP_DOI_LOG >> faile_HSDP_DOI_LOG
Branching_HSDP_DOI_LOG >> Validation_End


feedname_FAMILYPACK_PROVIDER_CONSUMER = 'FAMILYPACK_PROVIDER_CONSUMER'.upper()
feedname2_FAMILYPACK_PROVIDER_CONSUMER = 'FAMILYPACK_PROVIDER_CONSUMER'.lower()

dedup_command_FAMILYPACK_PROVIDER_CONSUMER="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_FAMILYPACK_PROVIDER_CONSUMER)
logging.info(dedup_command_FAMILYPACK_PROVIDER_CONSUMER) 

pcf_command_FAMILYPACK_PROVIDER_CONSUMER = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_FAMILYPACK_PROVIDER_CONSUMER)
logging.info(pcf_command_FAMILYPACK_PROVIDER_CONSUMER)

PCFCheck_command_FAMILYPACK_PROVIDER_CONSUMER = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_FAMILYPACK_PROVIDER_CONSUMER,run_date,sleeptime)
logging.info(PCFCheck_command_FAMILYPACK_PROVIDER_CONSUMER)

branchScript_FAMILYPACK_PROVIDER_CONSUMER = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_FAMILYPACK_PROVIDER_CONSUMER,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_FAMILYPACK_PROVIDER_CONSUMER)

def branch_FAMILYPACK_PROVIDER_CONSUMER():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_FAMILYPACK_PROVIDER_CONSUMER,feedname2_FAMILYPACK_PROVIDER_CONSUMER)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_FAMILYPACK_PROVIDER_CONSUMER)
    x = readcsv(filepath,feedname2_FAMILYPACK_PROVIDER_CONSUMER)
    logging.info('branch_FAMILYPACK_PROVIDER_CONSUMER' + x)
    return x

Branching_FAMILYPACK_PROVIDER_CONSUMER = BranchPythonOperator(
    task_id='branchid_FAMILYPACK_PROVIDER_CONSUMER',
    python_callable=branch_FAMILYPACK_PROVIDER_CONSUMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_FAMILYPACK_PROVIDER_CONSUMER = BashOperator(
    task_id='PCF_FAMILYPACK_PROVIDER_CONSUMER',
    bash_command= pcf_command_FAMILYPACK_PROVIDER_CONSUMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_FAMILYPACK_PROVIDER_CONSUMER = BashOperator(
    task_id='Dedup_FAMILYPACK_PROVIDER_CONSUMER',
    bash_command= dedup_command_FAMILYPACK_PROVIDER_CONSUMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_FAMILYPACK_PROVIDER_CONSUMER = BashOperator(
    task_id='Dedup2_FAMILYPACK_PROVIDER_CONSUMER',
    bash_command= dedup_command_FAMILYPACK_PROVIDER_CONSUMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_FAMILYPACK_PROVIDER_CONSUMER = BashOperator(
    task_id='PCFCheck_FAMILYPACK_PROVIDER_CONSUMER',
    bash_command=PCFCheck_command_FAMILYPACK_PROVIDER_CONSUMER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_FAMILYPACK_PROVIDER_CONSUMER = EmailOperator(
    task_id='faile_FAMILYPACK_PROVIDER_CONSUMER',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_FAMILYPACK_PROVIDER_CONSUMER),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_FAMILYPACK_PROVIDER_CONSUMER: PythonOperator = PythonOperator(task_id="waitForFlush_FAMILYPACK_PROVIDER_CONSUMER",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_FAMILYPACK_PROVIDER_CONSUMER >> PCF_FAMILYPACK_PROVIDER_CONSUMER >> PCFCheck_FAMILYPACK_PROVIDER_CONSUMER >> delay_python_task_FAMILYPACK_PROVIDER_CONSUMER >> Dedup_FAMILYPACK_PROVIDER_CONSUMER >> Validation_End 
Branching_FAMILYPACK_PROVIDER_CONSUMER >> Dedup2_FAMILYPACK_PROVIDER_CONSUMER >> Validation_End
Branching_FAMILYPACK_PROVIDER_CONSUMER >> faile_FAMILYPACK_PROVIDER_CONSUMER
Branching_FAMILYPACK_PROVIDER_CONSUMER >> Validation_End


feedname_RDS_FINANCIALRECEIPT = 'RDS_FINANCIALRECEIPT'.upper()
feedname2_RDS_FINANCIALRECEIPT = 'RDS_FINANCIALRECEIPT'.lower()

dedup_command_RDS_FINANCIALRECEIPT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_FINANCIALRECEIPT)
logging.info(dedup_command_RDS_FINANCIALRECEIPT) 

pcf_command_RDS_FINANCIALRECEIPT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_FINANCIALRECEIPT)
logging.info(pcf_command_RDS_FINANCIALRECEIPT)

PCFCheck_command_RDS_FINANCIALRECEIPT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_FINANCIALRECEIPT,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_FINANCIALRECEIPT)

branchScript_RDS_FINANCIALRECEIPT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_FINANCIALRECEIPT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_FINANCIALRECEIPT)

def branch_RDS_FINANCIALRECEIPT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_FINANCIALRECEIPT,feedname2_RDS_FINANCIALRECEIPT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_FINANCIALRECEIPT)
    x = readcsv(filepath,feedname2_RDS_FINANCIALRECEIPT)
    logging.info('branch_RDS_FINANCIALRECEIPT' + x)
    return x

Branching_RDS_FINANCIALRECEIPT = BranchPythonOperator(
    task_id='branchid_RDS_FINANCIALRECEIPT',
    python_callable=branch_RDS_FINANCIALRECEIPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_FINANCIALRECEIPT = BashOperator(
    task_id='PCF_RDS_FINANCIALRECEIPT',
    bash_command= pcf_command_RDS_FINANCIALRECEIPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_FINANCIALRECEIPT = BashOperator(
    task_id='Dedup_RDS_FINANCIALRECEIPT',
    bash_command= dedup_command_RDS_FINANCIALRECEIPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_FINANCIALRECEIPT = BashOperator(
    task_id='Dedup2_RDS_FINANCIALRECEIPT',
    bash_command= dedup_command_RDS_FINANCIALRECEIPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_FINANCIALRECEIPT = BashOperator(
    task_id='PCFCheck_RDS_FINANCIALRECEIPT',
    bash_command=PCFCheck_command_RDS_FINANCIALRECEIPT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_FINANCIALRECEIPT = EmailOperator(
    task_id='faile_RDS_FINANCIALRECEIPT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_FINANCIALRECEIPT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_FINANCIALRECEIPT: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_FINANCIALRECEIPT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_FINANCIALRECEIPT >> PCF_RDS_FINANCIALRECEIPT >> PCFCheck_RDS_FINANCIALRECEIPT >> delay_python_task_RDS_FINANCIALRECEIPT >> Dedup_RDS_FINANCIALRECEIPT >> Validation_End 
Branching_RDS_FINANCIALRECEIPT >> Dedup2_RDS_FINANCIALRECEIPT >> Validation_End
Branching_RDS_FINANCIALRECEIPT >> faile_RDS_FINANCIALRECEIPT
Branching_RDS_FINANCIALRECEIPT >> Validation_End


feedname_RDS_ACCOUNT_REFERENCE = 'RDS_ACCOUNT_REFERENCE'.upper()
feedname2_RDS_ACCOUNT_REFERENCE = 'RDS_ACCOUNT_REFERENCE'.lower()

dedup_command_RDS_ACCOUNT_REFERENCE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_ACCOUNT_REFERENCE)
logging.info(dedup_command_RDS_ACCOUNT_REFERENCE) 

pcf_command_RDS_ACCOUNT_REFERENCE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_ACCOUNT_REFERENCE)
logging.info(pcf_command_RDS_ACCOUNT_REFERENCE)

PCFCheck_command_RDS_ACCOUNT_REFERENCE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_ACCOUNT_REFERENCE,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_ACCOUNT_REFERENCE)

branchScript_RDS_ACCOUNT_REFERENCE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_ACCOUNT_REFERENCE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_ACCOUNT_REFERENCE)

def branch_RDS_ACCOUNT_REFERENCE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_ACCOUNT_REFERENCE,feedname2_RDS_ACCOUNT_REFERENCE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_ACCOUNT_REFERENCE)
    x = readcsv(filepath,feedname2_RDS_ACCOUNT_REFERENCE)
    logging.info('branch_RDS_ACCOUNT_REFERENCE' + x)
    return x

Branching_RDS_ACCOUNT_REFERENCE = BranchPythonOperator(
    task_id='branchid_RDS_ACCOUNT_REFERENCE',
    python_callable=branch_RDS_ACCOUNT_REFERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_ACCOUNT_REFERENCE = BashOperator(
    task_id='PCF_RDS_ACCOUNT_REFERENCE',
    bash_command= pcf_command_RDS_ACCOUNT_REFERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_ACCOUNT_REFERENCE = BashOperator(
    task_id='Dedup_RDS_ACCOUNT_REFERENCE',
    bash_command= dedup_command_RDS_ACCOUNT_REFERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_ACCOUNT_REFERENCE = BashOperator(
    task_id='Dedup2_RDS_ACCOUNT_REFERENCE',
    bash_command= dedup_command_RDS_ACCOUNT_REFERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_ACCOUNT_REFERENCE = BashOperator(
    task_id='PCFCheck_RDS_ACCOUNT_REFERENCE',
    bash_command=PCFCheck_command_RDS_ACCOUNT_REFERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_ACCOUNT_REFERENCE = EmailOperator(
    task_id='faile_RDS_ACCOUNT_REFERENCE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_ACCOUNT_REFERENCE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_ACCOUNT_REFERENCE: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_ACCOUNT_REFERENCE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_ACCOUNT_REFERENCE >> PCF_RDS_ACCOUNT_REFERENCE >> PCFCheck_RDS_ACCOUNT_REFERENCE >> delay_python_task_RDS_ACCOUNT_REFERENCE >> Dedup_RDS_ACCOUNT_REFERENCE >> Validation_End 
Branching_RDS_ACCOUNT_REFERENCE >> Dedup2_RDS_ACCOUNT_REFERENCE >> Validation_End
Branching_RDS_ACCOUNT_REFERENCE >> faile_RDS_ACCOUNT_REFERENCE
Branching_RDS_ACCOUNT_REFERENCE >> Validation_End


feedname_RDS_AHIDENTITIES = 'RDS_AHIDENTITIES'.upper()
feedname2_RDS_AHIDENTITIES = 'RDS_AHIDENTITIES'.lower()

dedup_command_RDS_AHIDENTITIES="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_AHIDENTITIES)
logging.info(dedup_command_RDS_AHIDENTITIES) 

pcf_command_RDS_AHIDENTITIES = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_AHIDENTITIES)
logging.info(pcf_command_RDS_AHIDENTITIES)

PCFCheck_command_RDS_AHIDENTITIES = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_AHIDENTITIES,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_AHIDENTITIES)

branchScript_RDS_AHIDENTITIES = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_AHIDENTITIES,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_AHIDENTITIES)

def branch_RDS_AHIDENTITIES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_AHIDENTITIES,feedname2_RDS_AHIDENTITIES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_AHIDENTITIES)
    x = readcsv(filepath,feedname2_RDS_AHIDENTITIES)
    logging.info('branch_RDS_AHIDENTITIES' + x)
    return x

Branching_RDS_AHIDENTITIES = BranchPythonOperator(
    task_id='branchid_RDS_AHIDENTITIES',
    python_callable=branch_RDS_AHIDENTITIES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_AHIDENTITIES = BashOperator(
    task_id='PCF_RDS_AHIDENTITIES',
    bash_command= pcf_command_RDS_AHIDENTITIES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_AHIDENTITIES = BashOperator(
    task_id='Dedup_RDS_AHIDENTITIES',
    bash_command= dedup_command_RDS_AHIDENTITIES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_AHIDENTITIES = BashOperator(
    task_id='Dedup2_RDS_AHIDENTITIES',
    bash_command= dedup_command_RDS_AHIDENTITIES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_AHIDENTITIES = BashOperator(
    task_id='PCFCheck_RDS_AHIDENTITIES',
    bash_command=PCFCheck_command_RDS_AHIDENTITIES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_AHIDENTITIES = EmailOperator(
    task_id='faile_RDS_AHIDENTITIES',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_AHIDENTITIES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_AHIDENTITIES: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_AHIDENTITIES",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_AHIDENTITIES >> PCF_RDS_AHIDENTITIES >> PCFCheck_RDS_AHIDENTITIES >> delay_python_task_RDS_AHIDENTITIES >> Dedup_RDS_AHIDENTITIES >> Validation_End 
Branching_RDS_AHIDENTITIES >> Dedup2_RDS_AHIDENTITIES >> Validation_End
Branching_RDS_AHIDENTITIES >> faile_RDS_AHIDENTITIES
Branching_RDS_AHIDENTITIES >> Validation_End


feedname_RDS_AUDITTRAIL = 'RDS_AUDITTRAIL'.upper()
feedname2_RDS_AUDITTRAIL = 'RDS_AUDITTRAIL'.lower()

dedup_command_RDS_AUDITTRAIL="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_AUDITTRAIL)
logging.info(dedup_command_RDS_AUDITTRAIL) 

pcf_command_RDS_AUDITTRAIL = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_AUDITTRAIL)
logging.info(pcf_command_RDS_AUDITTRAIL)

PCFCheck_command_RDS_AUDITTRAIL = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_AUDITTRAIL,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_AUDITTRAIL)

branchScript_RDS_AUDITTRAIL = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_AUDITTRAIL,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_AUDITTRAIL)

def branch_RDS_AUDITTRAIL():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_AUDITTRAIL,feedname2_RDS_AUDITTRAIL)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_AUDITTRAIL)
    x = readcsv(filepath,feedname2_RDS_AUDITTRAIL)
    logging.info('branch_RDS_AUDITTRAIL' + x)
    return x

Branching_RDS_AUDITTRAIL = BranchPythonOperator(
    task_id='branchid_RDS_AUDITTRAIL',
    python_callable=branch_RDS_AUDITTRAIL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_AUDITTRAIL = BashOperator(
    task_id='PCF_RDS_AUDITTRAIL',
    bash_command= pcf_command_RDS_AUDITTRAIL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_AUDITTRAIL = BashOperator(
    task_id='Dedup_RDS_AUDITTRAIL',
    bash_command= dedup_command_RDS_AUDITTRAIL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_AUDITTRAIL = BashOperator(
    task_id='Dedup2_RDS_AUDITTRAIL',
    bash_command= dedup_command_RDS_AUDITTRAIL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_AUDITTRAIL = BashOperator(
    task_id='PCFCheck_RDS_AUDITTRAIL',
    bash_command=PCFCheck_command_RDS_AUDITTRAIL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_AUDITTRAIL = EmailOperator(
    task_id='faile_RDS_AUDITTRAIL',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_AUDITTRAIL),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_AUDITTRAIL: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_AUDITTRAIL",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_AUDITTRAIL >> PCF_RDS_AUDITTRAIL >> PCFCheck_RDS_AUDITTRAIL >> delay_python_task_RDS_AUDITTRAIL >> Dedup_RDS_AUDITTRAIL >> Validation_End 
Branching_RDS_AUDITTRAIL >> Dedup2_RDS_AUDITTRAIL >> Validation_End
Branching_RDS_AUDITTRAIL >> faile_RDS_AUDITTRAIL
Branching_RDS_AUDITTRAIL >> Validation_End
