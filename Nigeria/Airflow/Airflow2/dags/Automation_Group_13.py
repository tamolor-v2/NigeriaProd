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
d_1 = Variable.get("rerunDate13", deserialize_json=True)
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

dag = DAG('New_Automation_Group_13', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group13 -c config.json' %(d_1)
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


feedname_TAS_CLOSING_STOCK_BALANCE = 'TAS_CLOSING_STOCK_BALANCE'.upper()
feedname2_TAS_CLOSING_STOCK_BALANCE = 'TAS_CLOSING_STOCK_BALANCE'.lower()

dedup_command_TAS_CLOSING_STOCK_BALANCE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_CLOSING_STOCK_BALANCE)
logging.info(dedup_command_TAS_CLOSING_STOCK_BALANCE) 

pcf_command_TAS_CLOSING_STOCK_BALANCE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_CLOSING_STOCK_BALANCE)
logging.info(pcf_command_TAS_CLOSING_STOCK_BALANCE)

PCFCheck_command_TAS_CLOSING_STOCK_BALANCE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_CLOSING_STOCK_BALANCE,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_CLOSING_STOCK_BALANCE)

branchScript_TAS_CLOSING_STOCK_BALANCE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_CLOSING_STOCK_BALANCE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_CLOSING_STOCK_BALANCE)

def branch_TAS_CLOSING_STOCK_BALANCE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_CLOSING_STOCK_BALANCE,feedname2_TAS_CLOSING_STOCK_BALANCE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_CLOSING_STOCK_BALANCE)
    x = readcsv(filepath,feedname2_TAS_CLOSING_STOCK_BALANCE)
    logging.info('branch_TAS_CLOSING_STOCK_BALANCE' + x)
    return x

Branching_TAS_CLOSING_STOCK_BALANCE = BranchPythonOperator(
    task_id='branchid_TAS_CLOSING_STOCK_BALANCE',
    python_callable=branch_TAS_CLOSING_STOCK_BALANCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_CLOSING_STOCK_BALANCE = BashOperator(
    task_id='PCF_TAS_CLOSING_STOCK_BALANCE',
    bash_command= pcf_command_TAS_CLOSING_STOCK_BALANCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_CLOSING_STOCK_BALANCE = BashOperator(
    task_id='Dedup_TAS_CLOSING_STOCK_BALANCE',
    bash_command= dedup_command_TAS_CLOSING_STOCK_BALANCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_CLOSING_STOCK_BALANCE = BashOperator(
    task_id='Dedup2_TAS_CLOSING_STOCK_BALANCE',
    bash_command= dedup_command_TAS_CLOSING_STOCK_BALANCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_CLOSING_STOCK_BALANCE = BashOperator(
    task_id='PCFCheck_TAS_CLOSING_STOCK_BALANCE',
    bash_command=PCFCheck_command_TAS_CLOSING_STOCK_BALANCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_CLOSING_STOCK_BALANCE = EmailOperator(
    task_id='faile_TAS_CLOSING_STOCK_BALANCE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_CLOSING_STOCK_BALANCE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_CLOSING_STOCK_BALANCE: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_CLOSING_STOCK_BALANCE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_CLOSING_STOCK_BALANCE >> PCF_TAS_CLOSING_STOCK_BALANCE >> PCFCheck_TAS_CLOSING_STOCK_BALANCE >> delay_python_task_TAS_CLOSING_STOCK_BALANCE >> Dedup_TAS_CLOSING_STOCK_BALANCE >> Validation_End 
Branching_TAS_CLOSING_STOCK_BALANCE >> Dedup2_TAS_CLOSING_STOCK_BALANCE >> Validation_End
Branching_TAS_CLOSING_STOCK_BALANCE >> faile_TAS_CLOSING_STOCK_BALANCE
Branching_TAS_CLOSING_STOCK_BALANCE >> Validation_End


feedname_TAS_PRIMARY_SALE = 'TAS_PRIMARY_SALE'.upper()
feedname2_TAS_PRIMARY_SALE = 'TAS_PRIMARY_SALE'.lower()

dedup_command_TAS_PRIMARY_SALE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_PRIMARY_SALE)
logging.info(dedup_command_TAS_PRIMARY_SALE) 

pcf_command_TAS_PRIMARY_SALE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_PRIMARY_SALE)
logging.info(pcf_command_TAS_PRIMARY_SALE)

PCFCheck_command_TAS_PRIMARY_SALE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_PRIMARY_SALE,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_PRIMARY_SALE)

branchScript_TAS_PRIMARY_SALE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_PRIMARY_SALE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_PRIMARY_SALE)

def branch_TAS_PRIMARY_SALE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_PRIMARY_SALE,feedname2_TAS_PRIMARY_SALE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_PRIMARY_SALE)
    x = readcsv(filepath,feedname2_TAS_PRIMARY_SALE)
    logging.info('branch_TAS_PRIMARY_SALE' + x)
    return x

Branching_TAS_PRIMARY_SALE = BranchPythonOperator(
    task_id='branchid_TAS_PRIMARY_SALE',
    python_callable=branch_TAS_PRIMARY_SALE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_PRIMARY_SALE = BashOperator(
    task_id='PCF_TAS_PRIMARY_SALE',
    bash_command= pcf_command_TAS_PRIMARY_SALE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_PRIMARY_SALE = BashOperator(
    task_id='Dedup_TAS_PRIMARY_SALE',
    bash_command= dedup_command_TAS_PRIMARY_SALE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_PRIMARY_SALE = BashOperator(
    task_id='Dedup2_TAS_PRIMARY_SALE',
    bash_command= dedup_command_TAS_PRIMARY_SALE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_PRIMARY_SALE = BashOperator(
    task_id='PCFCheck_TAS_PRIMARY_SALE',
    bash_command=PCFCheck_command_TAS_PRIMARY_SALE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_PRIMARY_SALE = EmailOperator(
    task_id='faile_TAS_PRIMARY_SALE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_PRIMARY_SALE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_PRIMARY_SALE: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_PRIMARY_SALE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_PRIMARY_SALE >> PCF_TAS_PRIMARY_SALE >> PCFCheck_TAS_PRIMARY_SALE >> delay_python_task_TAS_PRIMARY_SALE >> Dedup_TAS_PRIMARY_SALE >> Validation_End 
Branching_TAS_PRIMARY_SALE >> Dedup2_TAS_PRIMARY_SALE >> Validation_End
Branching_TAS_PRIMARY_SALE >> faile_TAS_PRIMARY_SALE
Branching_TAS_PRIMARY_SALE >> Validation_End


feedname_TAS_RETAIL_MASTER = 'TAS_RETAIL_MASTER'.upper()
feedname2_TAS_RETAIL_MASTER = 'TAS_RETAIL_MASTER'.lower()

dedup_command_TAS_RETAIL_MASTER="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_RETAIL_MASTER)
logging.info(dedup_command_TAS_RETAIL_MASTER) 

pcf_command_TAS_RETAIL_MASTER = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_RETAIL_MASTER)
logging.info(pcf_command_TAS_RETAIL_MASTER)

PCFCheck_command_TAS_RETAIL_MASTER = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_RETAIL_MASTER,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_RETAIL_MASTER)

branchScript_TAS_RETAIL_MASTER = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_RETAIL_MASTER,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_RETAIL_MASTER)

def branch_TAS_RETAIL_MASTER():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_RETAIL_MASTER,feedname2_TAS_RETAIL_MASTER)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_RETAIL_MASTER)
    x = readcsv(filepath,feedname2_TAS_RETAIL_MASTER)
    logging.info('branch_TAS_RETAIL_MASTER' + x)
    return x

Branching_TAS_RETAIL_MASTER = BranchPythonOperator(
    task_id='branchid_TAS_RETAIL_MASTER',
    python_callable=branch_TAS_RETAIL_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_RETAIL_MASTER = BashOperator(
    task_id='PCF_TAS_RETAIL_MASTER',
    bash_command= pcf_command_TAS_RETAIL_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_RETAIL_MASTER = BashOperator(
    task_id='Dedup_TAS_RETAIL_MASTER',
    bash_command= dedup_command_TAS_RETAIL_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_RETAIL_MASTER = BashOperator(
    task_id='Dedup2_TAS_RETAIL_MASTER',
    bash_command= dedup_command_TAS_RETAIL_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_RETAIL_MASTER = BashOperator(
    task_id='PCFCheck_TAS_RETAIL_MASTER',
    bash_command=PCFCheck_command_TAS_RETAIL_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_RETAIL_MASTER = EmailOperator(
    task_id='faile_TAS_RETAIL_MASTER',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_RETAIL_MASTER),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_RETAIL_MASTER: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_RETAIL_MASTER",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_RETAIL_MASTER >> PCF_TAS_RETAIL_MASTER >> PCFCheck_TAS_RETAIL_MASTER >> delay_python_task_TAS_RETAIL_MASTER >> Dedup_TAS_RETAIL_MASTER >> Validation_End 
Branching_TAS_RETAIL_MASTER >> Dedup2_TAS_RETAIL_MASTER >> Validation_End
Branching_TAS_RETAIL_MASTER >> faile_TAS_RETAIL_MASTER
Branching_TAS_RETAIL_MASTER >> Validation_End


feedname_SECURED = 'SECURED'.upper()
feedname2_SECURED = 'SECURED'.lower()

dedup_command_SECURED="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SECURED)
logging.info(dedup_command_SECURED) 

pcf_command_SECURED = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SECURED)
logging.info(pcf_command_SECURED)

PCFCheck_command_SECURED = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SECURED,run_date,sleeptime)
logging.info(PCFCheck_command_SECURED)

branchScript_SECURED = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SECURED,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SECURED)

def branch_SECURED():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SECURED,feedname2_SECURED)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SECURED)
    x = readcsv(filepath,feedname2_SECURED)
    logging.info('branch_SECURED' + x)
    return x

Branching_SECURED = BranchPythonOperator(
    task_id='branchid_SECURED',
    python_callable=branch_SECURED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SECURED = BashOperator(
    task_id='PCF_SECURED',
    bash_command= pcf_command_SECURED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SECURED = BashOperator(
    task_id='Dedup_SECURED',
    bash_command= dedup_command_SECURED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SECURED = BashOperator(
    task_id='Dedup2_SECURED',
    bash_command= dedup_command_SECURED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SECURED = BashOperator(
    task_id='PCFCheck_SECURED',
    bash_command=PCFCheck_command_SECURED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SECURED = EmailOperator(
    task_id='faile_SECURED',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SECURED),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SECURED: PythonOperator = PythonOperator(task_id="waitForFlush_SECURED",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SECURED >> PCF_SECURED >> PCFCheck_SECURED >> delay_python_task_SECURED >> Dedup_SECURED >> Validation_End 
Branching_SECURED >> Dedup2_SECURED >> Validation_End
Branching_SECURED >> faile_SECURED
Branching_SECURED >> Validation_End


feedname_GET_PRE2POST_CONSENT_DETAILS = 'GET_PRE2POST_CONSENT_DETAILS'.upper()
feedname2_GET_PRE2POST_CONSENT_DETAILS = 'GET_PRE2POST_CONSENT_DETAILS'.lower()

dedup_command_GET_PRE2POST_CONSENT_DETAILS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_GET_PRE2POST_CONSENT_DETAILS)
logging.info(dedup_command_GET_PRE2POST_CONSENT_DETAILS) 

pcf_command_GET_PRE2POST_CONSENT_DETAILS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_GET_PRE2POST_CONSENT_DETAILS)
logging.info(pcf_command_GET_PRE2POST_CONSENT_DETAILS)

PCFCheck_command_GET_PRE2POST_CONSENT_DETAILS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_GET_PRE2POST_CONSENT_DETAILS,run_date,sleeptime)
logging.info(PCFCheck_command_GET_PRE2POST_CONSENT_DETAILS)

branchScript_GET_PRE2POST_CONSENT_DETAILS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_GET_PRE2POST_CONSENT_DETAILS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_GET_PRE2POST_CONSENT_DETAILS)

def branch_GET_PRE2POST_CONSENT_DETAILS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_GET_PRE2POST_CONSENT_DETAILS,feedname2_GET_PRE2POST_CONSENT_DETAILS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_GET_PRE2POST_CONSENT_DETAILS)
    x = readcsv(filepath,feedname2_GET_PRE2POST_CONSENT_DETAILS)
    logging.info('branch_GET_PRE2POST_CONSENT_DETAILS' + x)
    return x

Branching_GET_PRE2POST_CONSENT_DETAILS = BranchPythonOperator(
    task_id='branchid_GET_PRE2POST_CONSENT_DETAILS',
    python_callable=branch_GET_PRE2POST_CONSENT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_GET_PRE2POST_CONSENT_DETAILS = BashOperator(
    task_id='PCF_GET_PRE2POST_CONSENT_DETAILS',
    bash_command= pcf_command_GET_PRE2POST_CONSENT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_GET_PRE2POST_CONSENT_DETAILS = BashOperator(
    task_id='Dedup_GET_PRE2POST_CONSENT_DETAILS',
    bash_command= dedup_command_GET_PRE2POST_CONSENT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_GET_PRE2POST_CONSENT_DETAILS = BashOperator(
    task_id='Dedup2_GET_PRE2POST_CONSENT_DETAILS',
    bash_command= dedup_command_GET_PRE2POST_CONSENT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_GET_PRE2POST_CONSENT_DETAILS = BashOperator(
    task_id='PCFCheck_GET_PRE2POST_CONSENT_DETAILS',
    bash_command=PCFCheck_command_GET_PRE2POST_CONSENT_DETAILS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_GET_PRE2POST_CONSENT_DETAILS = EmailOperator(
    task_id='faile_GET_PRE2POST_CONSENT_DETAILS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_GET_PRE2POST_CONSENT_DETAILS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_GET_PRE2POST_CONSENT_DETAILS: PythonOperator = PythonOperator(task_id="waitForFlush_GET_PRE2POST_CONSENT_DETAILS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_GET_PRE2POST_CONSENT_DETAILS >> PCF_GET_PRE2POST_CONSENT_DETAILS >> PCFCheck_GET_PRE2POST_CONSENT_DETAILS >> delay_python_task_GET_PRE2POST_CONSENT_DETAILS >> Dedup_GET_PRE2POST_CONSENT_DETAILS >> Validation_End 
Branching_GET_PRE2POST_CONSENT_DETAILS >> Dedup2_GET_PRE2POST_CONSENT_DETAILS >> Validation_End
Branching_GET_PRE2POST_CONSENT_DETAILS >> faile_GET_PRE2POST_CONSENT_DETAILS
Branching_GET_PRE2POST_CONSENT_DETAILS >> Validation_End
