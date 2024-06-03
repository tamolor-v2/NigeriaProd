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
Email='test@ligadata.com'
phases = ['Success','Dedup2_','PCF_','faile_']

pathStatus = Variable.get("pathStatus", deserialize_json=True)
sleeptime = Variable.get("sleeptime", deserialize_json=True)
ValidationP2Code = Variable.get("ValidationP2Code", deserialize_json=True)
dedup_hash_command= Variable.get("dedup_hash_command", deserialize_json=True)
pcf_command_non= Variable.get("pcf_command_non", deserialize_json=True)
PCFCheck_command= Variable.get("PCFCheck_command", deserialize_json=True)
branchScript= Variable.get("branchScript", deserialize_json=True)


default_args = {
    'owner': 'VR',
    'depends_on_past': False,
    'start_date': datetime(2020,4,15),
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
    
#dag = DAG('VR_TEST_20200914', default_args=default_args, catchup=False, schedule_interval='30 3 * * *' )
dag = DAG('VR_TEST_20200914', default_args=default_args, catchup=False, schedule_interval=None )


#ValidationP2Code = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh {0} all ValidationTool_BSL_ALL.conf true 2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log__Airflow_{1}.txt'
logging.info(ValidationP2Code)

#dedup_command="bash /nas/share05/tools/DQ/airflow_tools/Dedup_part.sh {0} {1} {2}"

#pcf_command = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.5.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f {1} --logQueries --ignoreDotFilesCheck -p 1 --move"

#PCFCheck_command = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname {0} -date {1} -sleeptime {2}" 

#branchScript = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}"


Validation = BashOperator(
task_id='Validation',
bash_command= ValidationP2Code.format(d_1,dayStr),
trigger_rule='all_success',
run_as_user='daasuser',
priority_weight = 100,
dag=dag,
)


Success = EmailOperator(
    task_id='Success',
    to=Email,
    subject='Success',
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

feedname_DYA_DAILY_ACTIVATION = 'DYA_DAILY_ACTIVATION'.upper()
feedname2_DYA_DAILY_ACTIVATION = 'DYA_DAILY_ACTIVATION'.lower()

def branch_DYA_DAILY_ACTIVATION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_DYA_DAILY_ACTIVATION,feedname2_DYA_DAILY_ACTIVATION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_DYA_DAILY_ACTIVATION,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_DYA_DAILY_ACTIVATION)
    logging.info('branch_DYA_DAILY_ACTIVATION' + x)
    return x

Branching_DYA_DAILY_ACTIVATION = BranchPythonOperator(
    task_id='branchid_DYA_DAILY_ACTIVATION',
    python_callable=branch_DYA_DAILY_ACTIVATION,
    dag=dag,
    run_as_user='daasuser'
)

PCF_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='PCF_DYA_DAILY_ACTIVATION',
    bash_command= pcf_command_non.format(d_1,feedname_DYA_DAILY_ACTIVATION),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='Dedup_DYA_DAILY_ACTIVATION',
    bash_command= dedup_hash_command.format(feedname_DYA_DAILY_ACTIVATION),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='Dedup2_DYA_DAILY_ACTIVATION',
    bash_command= dedup_hash_command.format(feedname_DYA_DAILY_ACTIVATION),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='PCFCheck_DYA_DAILY_ACTIVATION',
    bash_command=PCFCheck_command.format(feedname_DYA_DAILY_ACTIVATION,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_DYA_DAILY_ACTIVATION = EmailOperator(
    task_id='faile_DYA_DAILY_ACTIVATION',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_DYA_DAILY_ACTIVATION),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_DYA_DAILY_ACTIVATION >> PCF_DYA_DAILY_ACTIVATION >> PCFCheck_DYA_DAILY_ACTIVATION  >> Dedup_DYA_DAILY_ACTIVATION >> Success
Validation >> Branching_DYA_DAILY_ACTIVATION >> Success
Validation >> Branching_DYA_DAILY_ACTIVATION >> Dedup2_DYA_DAILY_ACTIVATION >> Success
Validation >> Branching_DYA_DAILY_ACTIVATION >> faile_DYA_DAILY_ACTIVATION


feedname_AGL_BILL_ANALYZER_MONTHLY = 'AGL_BILL_ANALYZER_MONTHLY'.upper()
feedname2_AGL_BILL_ANALYZER_MONTHLY = 'AGL_BILL_ANALYZER_MONTHLY'.lower()

def branch_AGL_BILL_ANALYZER_MONTHLY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_AGL_BILL_ANALYZER_MONTHLY,feedname2_AGL_BILL_ANALYZER_MONTHLY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_AGL_BILL_ANALYZER_MONTHLY,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_AGL_BILL_ANALYZER_MONTHLY)
    logging.info('branch_AGL_BILL_ANALYZER_MONTHLY' + x)
    return x

Branching_AGL_BILL_ANALYZER_MONTHLY = BranchPythonOperator(
    task_id='branchid_AGL_BILL_ANALYZER_MONTHLY',
    python_callable=branch_AGL_BILL_ANALYZER_MONTHLY,
    dag=dag,
    run_as_user='daasuser'
)

PCF_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='PCF_AGL_BILL_ANALYZER_MONTHLY',
    bash_command= pcf_command_non.format(d_1,feedname_AGL_BILL_ANALYZER_MONTHLY),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='Dedup_AGL_BILL_ANALYZER_MONTHLY',
    bash_command= dedup_hash_command.format(feedname_AGL_BILL_ANALYZER_MONTHLY),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='Dedup2_AGL_BILL_ANALYZER_MONTHLY',
    bash_command= dedup_hash_command.format(feedname_AGL_BILL_ANALYZER_MONTHLY),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_AGL_BILL_ANALYZER_MONTHLY = BashOperator(
    task_id='PCFCheck_AGL_BILL_ANALYZER_MONTHLY',
    bash_command=PCFCheck_command.format(feedname_AGL_BILL_ANALYZER_MONTHLY,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_AGL_BILL_ANALYZER_MONTHLY = EmailOperator(
    task_id='faile_AGL_BILL_ANALYZER_MONTHLY',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_AGL_BILL_ANALYZER_MONTHLY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_AGL_BILL_ANALYZER_MONTHLY >> PCF_AGL_BILL_ANALYZER_MONTHLY >> PCFCheck_AGL_BILL_ANALYZER_MONTHLY  >> Dedup_AGL_BILL_ANALYZER_MONTHLY >> Success
Validation >> Branching_AGL_BILL_ANALYZER_MONTHLY >> Success
Validation >> Branching_AGL_BILL_ANALYZER_MONTHLY >> Dedup2_AGL_BILL_ANALYZER_MONTHLY >> Success
Validation >> Branching_AGL_BILL_ANALYZER_MONTHLY >> faile_AGL_BILL_ANALYZER_MONTHLY


feedname_BILL_RUN_STATISTICS_TAB = 'BILL_RUN_STATISTICS_TAB'.upper()
feedname2_BILL_RUN_STATISTICS_TAB = 'BILL_RUN_STATISTICS_TAB'.lower()

def branch_BILL_RUN_STATISTICS_TAB():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_BILL_RUN_STATISTICS_TAB,feedname2_BILL_RUN_STATISTICS_TAB)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_BILL_RUN_STATISTICS_TAB,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_BILL_RUN_STATISTICS_TAB)
    logging.info('branch_BILL_RUN_STATISTICS_TAB' + x)
    return x

Branching_BILL_RUN_STATISTICS_TAB = BranchPythonOperator(
    task_id='branchid_BILL_RUN_STATISTICS_TAB',
    python_callable=branch_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser'
)

PCF_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='PCF_BILL_RUN_STATISTICS_TAB',
    bash_command= pcf_command_non.format(d_1,feedname_BILL_RUN_STATISTICS_TAB),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='Dedup_BILL_RUN_STATISTICS_TAB',
    bash_command= dedup_hash_command.format(feedname_BILL_RUN_STATISTICS_TAB),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='Dedup2_BILL_RUN_STATISTICS_TAB',
    bash_command= dedup_hash_command.format(feedname_BILL_RUN_STATISTICS_TAB),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='PCFCheck_BILL_RUN_STATISTICS_TAB',
    bash_command=PCFCheck_command.format(feedname_BILL_RUN_STATISTICS_TAB,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_BILL_RUN_STATISTICS_TAB = EmailOperator(
    task_id='faile_BILL_RUN_STATISTICS_TAB',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_BILL_RUN_STATISTICS_TAB),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_BILL_RUN_STATISTICS_TAB >> PCF_BILL_RUN_STATISTICS_TAB >> PCFCheck_BILL_RUN_STATISTICS_TAB  >> Dedup_BILL_RUN_STATISTICS_TAB >> Success
Validation >> Branching_BILL_RUN_STATISTICS_TAB >> Success
Validation >> Branching_BILL_RUN_STATISTICS_TAB >> Dedup2_BILL_RUN_STATISTICS_TAB >> Success
Validation >> Branching_BILL_RUN_STATISTICS_TAB >> faile_BILL_RUN_STATISTICS_TAB


feedname_BUDGET_YEAR_AMOUNT = 'BUDGET_YEAR_AMOUNT'.upper()
feedname2_BUDGET_YEAR_AMOUNT = 'BUDGET_YEAR_AMOUNT'.lower()

def branch_BUDGET_YEAR_AMOUNT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_BUDGET_YEAR_AMOUNT,feedname2_BUDGET_YEAR_AMOUNT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_BUDGET_YEAR_AMOUNT,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_BUDGET_YEAR_AMOUNT)
    logging.info('branch_BUDGET_YEAR_AMOUNT' + x)
    return x

Branching_BUDGET_YEAR_AMOUNT = BranchPythonOperator(
    task_id='branchid_BUDGET_YEAR_AMOUNT',
    python_callable=branch_BUDGET_YEAR_AMOUNT,
    dag=dag,
    run_as_user='daasuser'
)

PCF_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='PCF_BUDGET_YEAR_AMOUNT',
    bash_command= pcf_command_non.format(d_1,feedname_BUDGET_YEAR_AMOUNT),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='Dedup_BUDGET_YEAR_AMOUNT',
    bash_command= dedup_hash_command.format(feedname_BUDGET_YEAR_AMOUNT),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='Dedup2_BUDGET_YEAR_AMOUNT',
    bash_command= dedup_hash_command.format(feedname_BUDGET_YEAR_AMOUNT),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_BUDGET_YEAR_AMOUNT = BashOperator(
    task_id='PCFCheck_BUDGET_YEAR_AMOUNT',
    bash_command=PCFCheck_command.format(feedname_BUDGET_YEAR_AMOUNT,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_BUDGET_YEAR_AMOUNT = EmailOperator(
    task_id='faile_BUDGET_YEAR_AMOUNT',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_BUDGET_YEAR_AMOUNT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_BUDGET_YEAR_AMOUNT >> PCF_BUDGET_YEAR_AMOUNT >> PCFCheck_BUDGET_YEAR_AMOUNT  >> Dedup_BUDGET_YEAR_AMOUNT >> Success
Validation >> Branching_BUDGET_YEAR_AMOUNT >> Success
Validation >> Branching_BUDGET_YEAR_AMOUNT >> Dedup2_BUDGET_YEAR_AMOUNT >> Success
Validation >> Branching_BUDGET_YEAR_AMOUNT >> faile_BUDGET_YEAR_AMOUNT


feedname_CB_SCHEDULES = 'CB_SCHEDULES'.upper()
feedname2_CB_SCHEDULES = 'CB_SCHEDULES'.lower()

def branch_CB_SCHEDULES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CB_SCHEDULES,feedname2_CB_SCHEDULES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CB_SCHEDULES,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CB_SCHEDULES)
    logging.info('branch_CB_SCHEDULES' + x)
    return x

Branching_CB_SCHEDULES = BranchPythonOperator(
    task_id='branchid_CB_SCHEDULES',
    python_callable=branch_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CB_SCHEDULES = BashOperator(
    task_id='PCF_CB_SCHEDULES',
    bash_command= pcf_command_non.format(d_1,feedname_CB_SCHEDULES),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_CB_SCHEDULES = BashOperator(
    task_id='Dedup_CB_SCHEDULES',
    bash_command= dedup_hash_command.format(feedname_CB_SCHEDULES),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CB_SCHEDULES = BashOperator(
    task_id='Dedup2_CB_SCHEDULES',
    bash_command= dedup_hash_command.format(feedname_CB_SCHEDULES),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CB_SCHEDULES = BashOperator(
    task_id='PCFCheck_CB_SCHEDULES',
    bash_command=PCFCheck_command.format(feedname_CB_SCHEDULES,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CB_SCHEDULES = EmailOperator(
    task_id='faile_CB_SCHEDULES',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_SCHEDULES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CB_SCHEDULES >> PCF_CB_SCHEDULES >> PCFCheck_CB_SCHEDULES  >> Dedup_CB_SCHEDULES >> Success
Validation >> Branching_CB_SCHEDULES >> Success
Validation >> Branching_CB_SCHEDULES >> Dedup2_CB_SCHEDULES >> Success
Validation >> Branching_CB_SCHEDULES >> faile_CB_SCHEDULES


feedname_CB_SUBS_POS_SERVICES = 'CB_SUBS_POS_SERVICES'.upper()
feedname2_CB_SUBS_POS_SERVICES = 'CB_SUBS_POS_SERVICES'.lower()

def branch_CB_SUBS_POS_SERVICES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CB_SUBS_POS_SERVICES,feedname2_CB_SUBS_POS_SERVICES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CB_SUBS_POS_SERVICES,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CB_SUBS_POS_SERVICES)
    logging.info('branch_CB_SUBS_POS_SERVICES' + x)
    return x

Branching_CB_SUBS_POS_SERVICES = BranchPythonOperator(
    task_id='branchid_CB_SUBS_POS_SERVICES',
    python_callable=branch_CB_SUBS_POS_SERVICES,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='PCF_CB_SUBS_POS_SERVICES',
    bash_command= pcf_command_non.format(d_1,feedname_CB_SUBS_POS_SERVICES),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='Dedup_CB_SUBS_POS_SERVICES',
    bash_command= dedup_hash_command.format(feedname_CB_SUBS_POS_SERVICES),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='Dedup2_CB_SUBS_POS_SERVICES',
    bash_command= dedup_hash_command.format(feedname_CB_SUBS_POS_SERVICES),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CB_SUBS_POS_SERVICES = BashOperator(
    task_id='PCFCheck_CB_SUBS_POS_SERVICES',
    bash_command=PCFCheck_command.format(feedname_CB_SUBS_POS_SERVICES,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CB_SUBS_POS_SERVICES = EmailOperator(
    task_id='faile_CB_SUBS_POS_SERVICES',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_SUBS_POS_SERVICES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CB_SUBS_POS_SERVICES >> PCF_CB_SUBS_POS_SERVICES >> PCFCheck_CB_SUBS_POS_SERVICES  >> Dedup_CB_SUBS_POS_SERVICES >> Success
Validation >> Branching_CB_SUBS_POS_SERVICES >> Success
Validation >> Branching_CB_SUBS_POS_SERVICES >> Dedup2_CB_SUBS_POS_SERVICES >> Success
Validation >> Branching_CB_SUBS_POS_SERVICES >> faile_CB_SUBS_POS_SERVICES


feedname_CHANGE_REPORT_MTN = 'CHANGE_REPORT_MTN'.upper()
feedname2_CHANGE_REPORT_MTN = 'CHANGE_REPORT_MTN'.lower()

def branch_CHANGE_REPORT_MTN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CHANGE_REPORT_MTN,feedname2_CHANGE_REPORT_MTN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CHANGE_REPORT_MTN,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CHANGE_REPORT_MTN)
    logging.info('branch_CHANGE_REPORT_MTN' + x)
    return x

Branching_CHANGE_REPORT_MTN = BranchPythonOperator(
    task_id='branchid_CHANGE_REPORT_MTN',
    python_callable=branch_CHANGE_REPORT_MTN,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CHANGE_REPORT_MTN = BashOperator(
    task_id='PCF_CHANGE_REPORT_MTN',
    bash_command= pcf_command_non.format(d_1,feedname_CHANGE_REPORT_MTN),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_CHANGE_REPORT_MTN = BashOperator(
    task_id='Dedup_CHANGE_REPORT_MTN',
    bash_command= dedup_hash_command.format(feedname_CHANGE_REPORT_MTN),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CHANGE_REPORT_MTN = BashOperator(
    task_id='Dedup2_CHANGE_REPORT_MTN',
    bash_command= dedup_hash_command.format(feedname_CHANGE_REPORT_MTN),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CHANGE_REPORT_MTN = BashOperator(
    task_id='PCFCheck_CHANGE_REPORT_MTN',
    bash_command=PCFCheck_command.format(feedname_CHANGE_REPORT_MTN,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CHANGE_REPORT_MTN = EmailOperator(
    task_id='faile_CHANGE_REPORT_MTN',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CHANGE_REPORT_MTN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CHANGE_REPORT_MTN >> PCF_CHANGE_REPORT_MTN >> PCFCheck_CHANGE_REPORT_MTN  >> Dedup_CHANGE_REPORT_MTN >> Success
Validation >> Branching_CHANGE_REPORT_MTN >> Success
Validation >> Branching_CHANGE_REPORT_MTN >> Dedup2_CHANGE_REPORT_MTN >> Success
Validation >> Branching_CHANGE_REPORT_MTN >> faile_CHANGE_REPORT_MTN


feedname_CLIENT_ACTIVITY_LOG = 'CLIENT_ACTIVITY_LOG'.upper()
feedname2_CLIENT_ACTIVITY_LOG = 'CLIENT_ACTIVITY_LOG'.lower()

def branch_CLIENT_ACTIVITY_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CLIENT_ACTIVITY_LOG,feedname2_CLIENT_ACTIVITY_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CLIENT_ACTIVITY_LOG,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CLIENT_ACTIVITY_LOG)
    logging.info('branch_CLIENT_ACTIVITY_LOG' + x)
    return x

Branching_CLIENT_ACTIVITY_LOG = BranchPythonOperator(
    task_id='branchid_CLIENT_ACTIVITY_LOG',
    python_callable=branch_CLIENT_ACTIVITY_LOG,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='PCF_CLIENT_ACTIVITY_LOG',
    bash_command= pcf_command_non.format(d_1,feedname_CLIENT_ACTIVITY_LOG),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='Dedup_CLIENT_ACTIVITY_LOG',
    bash_command= dedup_hash_command.format(feedname_CLIENT_ACTIVITY_LOG),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='Dedup2_CLIENT_ACTIVITY_LOG',
    bash_command= dedup_hash_command.format(feedname_CLIENT_ACTIVITY_LOG),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CLIENT_ACTIVITY_LOG = BashOperator(
    task_id='PCFCheck_CLIENT_ACTIVITY_LOG',
    bash_command=PCFCheck_command.format(feedname_CLIENT_ACTIVITY_LOG,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CLIENT_ACTIVITY_LOG = EmailOperator(
    task_id='faile_CLIENT_ACTIVITY_LOG',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CLIENT_ACTIVITY_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CLIENT_ACTIVITY_LOG >> PCF_CLIENT_ACTIVITY_LOG >> PCFCheck_CLIENT_ACTIVITY_LOG  >> Dedup_CLIENT_ACTIVITY_LOG >> Success
Validation >> Branching_CLIENT_ACTIVITY_LOG >> Success
Validation >> Branching_CLIENT_ACTIVITY_LOG >> Dedup2_CLIENT_ACTIVITY_LOG >> Success
Validation >> Branching_CLIENT_ACTIVITY_LOG >> faile_CLIENT_ACTIVITY_LOG


feedname_HPD_HELP_DESK = 'HPD_HELP_DESK'.upper()
feedname2_HPD_HELP_DESK = 'HPD_HELP_DESK'.lower()

def branch_HPD_HELP_DESK():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_HPD_HELP_DESK,feedname2_HPD_HELP_DESK)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_HPD_HELP_DESK,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_HPD_HELP_DESK)
    logging.info('branch_HPD_HELP_DESK' + x)
    return x

Branching_HPD_HELP_DESK = BranchPythonOperator(
    task_id='branchid_HPD_HELP_DESK',
    python_callable=branch_HPD_HELP_DESK,
    dag=dag,
    run_as_user='daasuser'
)

PCF_HPD_HELP_DESK = BashOperator(
    task_id='PCF_HPD_HELP_DESK',
    bash_command= pcf_command_non.format(d_1,feedname_HPD_HELP_DESK),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_HPD_HELP_DESK = BashOperator(
    task_id='Dedup_HPD_HELP_DESK',
    bash_command= dedup_hash_command.format(feedname_HPD_HELP_DESK),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_HPD_HELP_DESK = BashOperator(
    task_id='Dedup2_HPD_HELP_DESK',
    bash_command= dedup_hash_command.format(feedname_HPD_HELP_DESK),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_HPD_HELP_DESK = BashOperator(
    task_id='PCFCheck_HPD_HELP_DESK',
    bash_command=PCFCheck_command.format(feedname_HPD_HELP_DESK,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_HPD_HELP_DESK = EmailOperator(
    task_id='faile_HPD_HELP_DESK',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_HPD_HELP_DESK),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_HPD_HELP_DESK >> PCF_HPD_HELP_DESK >> PCFCheck_HPD_HELP_DESK  >> Dedup_HPD_HELP_DESK >> Success
Validation >> Branching_HPD_HELP_DESK >> Success
Validation >> Branching_HPD_HELP_DESK >> Dedup2_HPD_HELP_DESK >> Success
Validation >> Branching_HPD_HELP_DESK >> faile_HPD_HELP_DESK


feedname_MSO_ACCURACY_STATISTICS = 'MSO_ACCURACY_STATISTICS'.upper()
feedname2_MSO_ACCURACY_STATISTICS = 'MSO_ACCURACY_STATISTICS'.lower()

def branch_MSO_ACCURACY_STATISTICS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MSO_ACCURACY_STATISTICS,feedname2_MSO_ACCURACY_STATISTICS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MSO_ACCURACY_STATISTICS,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MSO_ACCURACY_STATISTICS)
    logging.info('branch_MSO_ACCURACY_STATISTICS' + x)
    return x

Branching_MSO_ACCURACY_STATISTICS = BranchPythonOperator(
    task_id='branchid_MSO_ACCURACY_STATISTICS',
    python_callable=branch_MSO_ACCURACY_STATISTICS,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='PCF_MSO_ACCURACY_STATISTICS',
    bash_command= pcf_command_non.format(d_1,feedname_MSO_ACCURACY_STATISTICS),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='Dedup_MSO_ACCURACY_STATISTICS',
    bash_command= dedup_hash_command.format(feedname_MSO_ACCURACY_STATISTICS),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='Dedup2_MSO_ACCURACY_STATISTICS',
    bash_command= dedup_hash_command.format(feedname_MSO_ACCURACY_STATISTICS),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MSO_ACCURACY_STATISTICS = BashOperator(
    task_id='PCFCheck_MSO_ACCURACY_STATISTICS',
    bash_command=PCFCheck_command.format(feedname_MSO_ACCURACY_STATISTICS,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MSO_ACCURACY_STATISTICS = EmailOperator(
    task_id='faile_MSO_ACCURACY_STATISTICS',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_ACCURACY_STATISTICS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MSO_ACCURACY_STATISTICS >> PCF_MSO_ACCURACY_STATISTICS >> PCFCheck_MSO_ACCURACY_STATISTICS  >> Dedup_MSO_ACCURACY_STATISTICS >> Success
Validation >> Branching_MSO_ACCURACY_STATISTICS >> Success
Validation >> Branching_MSO_ACCURACY_STATISTICS >> Dedup2_MSO_ACCURACY_STATISTICS >> Success
Validation >> Branching_MSO_ACCURACY_STATISTICS >> faile_MSO_ACCURACY_STATISTICS


feedname_MSO_PROCESS_AVG_TIME = 'MSO_PROCESS_AVG_TIME'.upper()
feedname2_MSO_PROCESS_AVG_TIME = 'MSO_PROCESS_AVG_TIME'.lower()

def branch_MSO_PROCESS_AVG_TIME():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MSO_PROCESS_AVG_TIME,feedname2_MSO_PROCESS_AVG_TIME)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MSO_PROCESS_AVG_TIME,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MSO_PROCESS_AVG_TIME)
    logging.info('branch_MSO_PROCESS_AVG_TIME' + x)
    return x

Branching_MSO_PROCESS_AVG_TIME = BranchPythonOperator(
    task_id='branchid_MSO_PROCESS_AVG_TIME',
    python_callable=branch_MSO_PROCESS_AVG_TIME,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='PCF_MSO_PROCESS_AVG_TIME',
    bash_command= pcf_command_non.format(d_1,feedname_MSO_PROCESS_AVG_TIME),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='Dedup_MSO_PROCESS_AVG_TIME',
    bash_command= dedup_hash_command.format(feedname_MSO_PROCESS_AVG_TIME),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='Dedup2_MSO_PROCESS_AVG_TIME',
    bash_command= dedup_hash_command.format(feedname_MSO_PROCESS_AVG_TIME),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MSO_PROCESS_AVG_TIME = BashOperator(
    task_id='PCFCheck_MSO_PROCESS_AVG_TIME',
    bash_command=PCFCheck_command.format(feedname_MSO_PROCESS_AVG_TIME,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MSO_PROCESS_AVG_TIME = EmailOperator(
    task_id='faile_MSO_PROCESS_AVG_TIME',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_PROCESS_AVG_TIME),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MSO_PROCESS_AVG_TIME >> PCF_MSO_PROCESS_AVG_TIME >> PCFCheck_MSO_PROCESS_AVG_TIME  >> Dedup_MSO_PROCESS_AVG_TIME >> Success
Validation >> Branching_MSO_PROCESS_AVG_TIME >> Success
Validation >> Branching_MSO_PROCESS_AVG_TIME >> Dedup2_MSO_PROCESS_AVG_TIME >> Success
Validation >> Branching_MSO_PROCESS_AVG_TIME >> faile_MSO_PROCESS_AVG_TIME


feedname_MTN_OVERVIEW_COMM_RPT = 'MTN_OVERVIEW_COMM_RPT'.upper()
feedname2_MTN_OVERVIEW_COMM_RPT = 'MTN_OVERVIEW_COMM_RPT'.lower()

def branch_MTN_OVERVIEW_COMM_RPT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MTN_OVERVIEW_COMM_RPT,feedname2_MTN_OVERVIEW_COMM_RPT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MTN_OVERVIEW_COMM_RPT,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MTN_OVERVIEW_COMM_RPT)
    logging.info('branch_MTN_OVERVIEW_COMM_RPT' + x)
    return x

Branching_MTN_OVERVIEW_COMM_RPT = BranchPythonOperator(
    task_id='branchid_MTN_OVERVIEW_COMM_RPT',
    python_callable=branch_MTN_OVERVIEW_COMM_RPT,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='PCF_MTN_OVERVIEW_COMM_RPT',
    bash_command= pcf_command_non.format(d_1,feedname_MTN_OVERVIEW_COMM_RPT),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='Dedup_MTN_OVERVIEW_COMM_RPT',
    bash_command= dedup_hash_command.format(feedname_MTN_OVERVIEW_COMM_RPT),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='Dedup2_MTN_OVERVIEW_COMM_RPT',
    bash_command= dedup_hash_command.format(feedname_MTN_OVERVIEW_COMM_RPT),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MTN_OVERVIEW_COMM_RPT = BashOperator(
    task_id='PCFCheck_MTN_OVERVIEW_COMM_RPT',
    bash_command=PCFCheck_command.format(feedname_MTN_OVERVIEW_COMM_RPT,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MTN_OVERVIEW_COMM_RPT = EmailOperator(
    task_id='faile_MTN_OVERVIEW_COMM_RPT',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MTN_OVERVIEW_COMM_RPT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MTN_OVERVIEW_COMM_RPT >> PCF_MTN_OVERVIEW_COMM_RPT >> PCFCheck_MTN_OVERVIEW_COMM_RPT  >> Dedup_MTN_OVERVIEW_COMM_RPT >> Success
Validation >> Branching_MTN_OVERVIEW_COMM_RPT >> Success
Validation >> Branching_MTN_OVERVIEW_COMM_RPT >> Dedup2_MTN_OVERVIEW_COMM_RPT >> Success
Validation >> Branching_MTN_OVERVIEW_COMM_RPT >> faile_MTN_OVERVIEW_COMM_RPT


feedname_MTN_PR_DETAILS = 'MTN_PR_DETAILS'.upper()
feedname2_MTN_PR_DETAILS = 'MTN_PR_DETAILS'.lower()

def branch_MTN_PR_DETAILS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MTN_PR_DETAILS,feedname2_MTN_PR_DETAILS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MTN_PR_DETAILS,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MTN_PR_DETAILS)
    logging.info('branch_MTN_PR_DETAILS' + x)
    return x

Branching_MTN_PR_DETAILS = BranchPythonOperator(
    task_id='branchid_MTN_PR_DETAILS',
    python_callable=branch_MTN_PR_DETAILS,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MTN_PR_DETAILS = BashOperator(
    task_id='PCF_MTN_PR_DETAILS',
    bash_command= pcf_command_non.format(d_1,feedname_MTN_PR_DETAILS),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_MTN_PR_DETAILS = BashOperator(
    task_id='Dedup_MTN_PR_DETAILS',
    bash_command= dedup_hash_command.format(feedname_MTN_PR_DETAILS),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MTN_PR_DETAILS = BashOperator(
    task_id='Dedup2_MTN_PR_DETAILS',
    bash_command= dedup_hash_command.format(feedname_MTN_PR_DETAILS),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MTN_PR_DETAILS = BashOperator(
    task_id='PCFCheck_MTN_PR_DETAILS',
    bash_command=PCFCheck_command.format(feedname_MTN_PR_DETAILS,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MTN_PR_DETAILS = EmailOperator(
    task_id='faile_MTN_PR_DETAILS',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MTN_PR_DETAILS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MTN_PR_DETAILS >> PCF_MTN_PR_DETAILS >> PCFCheck_MTN_PR_DETAILS  >> Dedup_MTN_PR_DETAILS >> Success
Validation >> Branching_MTN_PR_DETAILS >> Success
Validation >> Branching_MTN_PR_DETAILS >> Dedup2_MTN_PR_DETAILS >> Success
Validation >> Branching_MTN_PR_DETAILS >> faile_MTN_PR_DETAILS


feedname_OTP_STATUS = 'OTP_STATUS'.upper()
feedname2_OTP_STATUS = 'OTP_STATUS'.lower()

def branch_OTP_STATUS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_OTP_STATUS,feedname2_OTP_STATUS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_OTP_STATUS,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_OTP_STATUS)
    logging.info('branch_OTP_STATUS' + x)
    return x

Branching_OTP_STATUS = BranchPythonOperator(
    task_id='branchid_OTP_STATUS',
    python_callable=branch_OTP_STATUS,
    dag=dag,
    run_as_user='daasuser'
)

PCF_OTP_STATUS = BashOperator(
    task_id='PCF_OTP_STATUS',
    bash_command= pcf_command_non.format(d_1,feedname_OTP_STATUS),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_OTP_STATUS = BashOperator(
    task_id='Dedup_OTP_STATUS',
    bash_command= dedup_hash_command.format(feedname_OTP_STATUS),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_OTP_STATUS = BashOperator(
    task_id='Dedup2_OTP_STATUS',
    bash_command= dedup_hash_command.format(feedname_OTP_STATUS),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_OTP_STATUS = BashOperator(
    task_id='PCFCheck_OTP_STATUS',
    bash_command=PCFCheck_command.format(feedname_OTP_STATUS,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_OTP_STATUS = EmailOperator(
    task_id='faile_OTP_STATUS',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_OTP_STATUS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_OTP_STATUS >> PCF_OTP_STATUS >> PCFCheck_OTP_STATUS  >> Dedup_OTP_STATUS >> Success
Validation >> Branching_OTP_STATUS >> Success
Validation >> Branching_OTP_STATUS >> Dedup2_OTP_STATUS >> Success
Validation >> Branching_OTP_STATUS >> faile_OTP_STATUS


feedname_SERV_STATUS_SEAMFIX_REF = 'SERV_STATUS_SEAMFIX_REF'.upper()
feedname2_SERV_STATUS_SEAMFIX_REF = 'SERV_STATUS_SEAMFIX_REF'.lower()

def branch_SERV_STATUS_SEAMFIX_REF():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SERV_STATUS_SEAMFIX_REF,feedname2_SERV_STATUS_SEAMFIX_REF)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SERV_STATUS_SEAMFIX_REF,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SERV_STATUS_SEAMFIX_REF)
    logging.info('branch_SERV_STATUS_SEAMFIX_REF' + x)
    return x

Branching_SERV_STATUS_SEAMFIX_REF = BranchPythonOperator(
    task_id='branchid_SERV_STATUS_SEAMFIX_REF',
    python_callable=branch_SERV_STATUS_SEAMFIX_REF,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='PCF_SERV_STATUS_SEAMFIX_REF',
    bash_command= pcf_command_non.format(d_1,feedname_SERV_STATUS_SEAMFIX_REF),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='Dedup_SERV_STATUS_SEAMFIX_REF',
    bash_command= dedup_hash_command.format(feedname_SERV_STATUS_SEAMFIX_REF),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='Dedup2_SERV_STATUS_SEAMFIX_REF',
    bash_command= dedup_hash_command.format(feedname_SERV_STATUS_SEAMFIX_REF),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SERV_STATUS_SEAMFIX_REF = BashOperator(
    task_id='PCFCheck_SERV_STATUS_SEAMFIX_REF',
    bash_command=PCFCheck_command.format(feedname_SERV_STATUS_SEAMFIX_REF,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SERV_STATUS_SEAMFIX_REF = EmailOperator(
    task_id='faile_SERV_STATUS_SEAMFIX_REF',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SERV_STATUS_SEAMFIX_REF),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SERV_STATUS_SEAMFIX_REF >> PCF_SERV_STATUS_SEAMFIX_REF >> PCFCheck_SERV_STATUS_SEAMFIX_REF  >> Dedup_SERV_STATUS_SEAMFIX_REF >> Success
Validation >> Branching_SERV_STATUS_SEAMFIX_REF >> Success
Validation >> Branching_SERV_STATUS_SEAMFIX_REF >> Dedup2_SERV_STATUS_SEAMFIX_REF >> Success
Validation >> Branching_SERV_STATUS_SEAMFIX_REF >> faile_SERV_STATUS_SEAMFIX_REF


feedname_SMS_ACTIVATION_REQUEST_JOIN = 'SMS_ACTIVATION_REQUEST_JOIN'.upper()
feedname2_SMS_ACTIVATION_REQUEST_JOIN = 'SMS_ACTIVATION_REQUEST_JOIN'.lower()

def branch_SMS_ACTIVATION_REQUEST_JOIN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMS_ACTIVATION_REQUEST_JOIN,feedname2_SMS_ACTIVATION_REQUEST_JOIN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMS_ACTIVATION_REQUEST_JOIN,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMS_ACTIVATION_REQUEST_JOIN)
    logging.info('branch_SMS_ACTIVATION_REQUEST_JOIN' + x)
    return x

Branching_SMS_ACTIVATION_REQUEST_JOIN = BranchPythonOperator(
    task_id='branchid_SMS_ACTIVATION_REQUEST_JOIN',
    python_callable=branch_SMS_ACTIVATION_REQUEST_JOIN,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='PCF_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command= pcf_command_non.format(d_1,feedname_SMS_ACTIVATION_REQUEST_JOIN),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='Dedup_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command= dedup_hash_command.format(feedname_SMS_ACTIVATION_REQUEST_JOIN),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='Dedup2_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command= dedup_hash_command.format(feedname_SMS_ACTIVATION_REQUEST_JOIN),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMS_ACTIVATION_REQUEST_JOIN = BashOperator(
    task_id='PCFCheck_SMS_ACTIVATION_REQUEST_JOIN',
    bash_command=PCFCheck_command.format(feedname_SMS_ACTIVATION_REQUEST_JOIN,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMS_ACTIVATION_REQUEST_JOIN = EmailOperator(
    task_id='faile_SMS_ACTIVATION_REQUEST_JOIN',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMS_ACTIVATION_REQUEST_JOIN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMS_ACTIVATION_REQUEST_JOIN >> PCF_SMS_ACTIVATION_REQUEST_JOIN >> PCFCheck_SMS_ACTIVATION_REQUEST_JOIN  >> Dedup_SMS_ACTIVATION_REQUEST_JOIN >> Success
Validation >> Branching_SMS_ACTIVATION_REQUEST_JOIN >> Success
Validation >> Branching_SMS_ACTIVATION_REQUEST_JOIN >> Dedup2_SMS_ACTIVATION_REQUEST_JOIN >> Success
Validation >> Branching_SMS_ACTIVATION_REQUEST_JOIN >> faile_SMS_ACTIVATION_REQUEST_JOIN


feedname_SMS_ACTIVATION_VW = 'SMS_ACTIVATION_VW'.upper()
feedname2_SMS_ACTIVATION_VW = 'SMS_ACTIVATION_VW'.lower()

def branch_SMS_ACTIVATION_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMS_ACTIVATION_VW,feedname2_SMS_ACTIVATION_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMS_ACTIVATION_VW,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMS_ACTIVATION_VW)
    logging.info('branch_SMS_ACTIVATION_VW' + x)
    return x

Branching_SMS_ACTIVATION_VW = BranchPythonOperator(
    task_id='branchid_SMS_ACTIVATION_VW',
    python_callable=branch_SMS_ACTIVATION_VW,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMS_ACTIVATION_VW = BashOperator(
    task_id='PCF_SMS_ACTIVATION_VW',
    bash_command= pcf_command_non.format(d_1,feedname_SMS_ACTIVATION_VW),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_SMS_ACTIVATION_VW = BashOperator(
    task_id='Dedup_SMS_ACTIVATION_VW',
    bash_command= dedup_hash_command.format(feedname_SMS_ACTIVATION_VW),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMS_ACTIVATION_VW = BashOperator(
    task_id='Dedup2_SMS_ACTIVATION_VW',
    bash_command= dedup_hash_command.format(feedname_SMS_ACTIVATION_VW),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMS_ACTIVATION_VW = BashOperator(
    task_id='PCFCheck_SMS_ACTIVATION_VW',
    bash_command=PCFCheck_command.format(feedname_SMS_ACTIVATION_VW,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMS_ACTIVATION_VW = EmailOperator(
    task_id='faile_SMS_ACTIVATION_VW',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMS_ACTIVATION_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMS_ACTIVATION_VW >> PCF_SMS_ACTIVATION_VW >> PCFCheck_SMS_ACTIVATION_VW  >> Dedup_SMS_ACTIVATION_VW >> Success
Validation >> Branching_SMS_ACTIVATION_VW >> Success
Validation >> Branching_SMS_ACTIVATION_VW >> Dedup2_SMS_ACTIVATION_VW >> Success
Validation >> Branching_SMS_ACTIVATION_VW >> faile_SMS_ACTIVATION_VW


feedname_VALIDATION_PASSED = 'VALIDATION_PASSED'.upper()
feedname2_VALIDATION_PASSED = 'VALIDATION_PASSED'.lower()

def branch_VALIDATION_PASSED():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_VALIDATION_PASSED,feedname2_VALIDATION_PASSED)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_VALIDATION_PASSED,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_VALIDATION_PASSED)
    logging.info('branch_VALIDATION_PASSED' + x)
    return x

Branching_VALIDATION_PASSED = BranchPythonOperator(
    task_id='branchid_VALIDATION_PASSED',
    python_callable=branch_VALIDATION_PASSED,
    dag=dag,
    run_as_user='daasuser'
)

PCF_VALIDATION_PASSED = BashOperator(
    task_id='PCF_VALIDATION_PASSED',
    bash_command= pcf_command_non.format(d_1,feedname_VALIDATION_PASSED),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_VALIDATION_PASSED = BashOperator(
    task_id='Dedup_VALIDATION_PASSED',
    bash_command= dedup_hash_command.format(feedname_VALIDATION_PASSED),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_VALIDATION_PASSED = BashOperator(
    task_id='Dedup2_VALIDATION_PASSED',
    bash_command= dedup_hash_command.format(feedname_VALIDATION_PASSED),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_VALIDATION_PASSED = BashOperator(
    task_id='PCFCheck_VALIDATION_PASSED',
    bash_command=PCFCheck_command.format(feedname_VALIDATION_PASSED,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_VALIDATION_PASSED = EmailOperator(
    task_id='faile_VALIDATION_PASSED',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_VALIDATION_PASSED),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_VALIDATION_PASSED >> PCF_VALIDATION_PASSED >> PCFCheck_VALIDATION_PASSED  >> Dedup_VALIDATION_PASSED >> Success
Validation >> Branching_VALIDATION_PASSED >> Success
Validation >> Branching_VALIDATION_PASSED >> Dedup2_VALIDATION_PASSED >> Success
Validation >> Branching_VALIDATION_PASSED >> faile_VALIDATION_PASSED


feedname_SMS_ACTIVATION_REQUEST_KPI = 'SMS_ACTIVATION_REQUEST_KPI'.upper()
feedname2_SMS_ACTIVATION_REQUEST_KPI = 'SMS_ACTIVATION_REQUEST_KPI'.lower()

def branch_SMS_ACTIVATION_REQUEST_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMS_ACTIVATION_REQUEST_KPI,feedname2_SMS_ACTIVATION_REQUEST_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMS_ACTIVATION_REQUEST_KPI,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMS_ACTIVATION_REQUEST_KPI)
    logging.info('branch_SMS_ACTIVATION_REQUEST_KPI' + x)
    return x

Branching_SMS_ACTIVATION_REQUEST_KPI = BranchPythonOperator(
    task_id='branchid_SMS_ACTIVATION_REQUEST_KPI',
    python_callable=branch_SMS_ACTIVATION_REQUEST_KPI,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='PCF_SMS_ACTIVATION_REQUEST_KPI',
    bash_command= pcf_command_non.format(d_1,feedname_SMS_ACTIVATION_REQUEST_KPI),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='Dedup_SMS_ACTIVATION_REQUEST_KPI',
    bash_command= dedup_hash_command.format(feedname_SMS_ACTIVATION_REQUEST_KPI),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='Dedup2_SMS_ACTIVATION_REQUEST_KPI',
    bash_command= dedup_hash_command.format(feedname_SMS_ACTIVATION_REQUEST_KPI),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMS_ACTIVATION_REQUEST_KPI = BashOperator(
    task_id='PCFCheck_SMS_ACTIVATION_REQUEST_KPI',
    bash_command=PCFCheck_command.format(feedname_SMS_ACTIVATION_REQUEST_KPI,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMS_ACTIVATION_REQUEST_KPI = EmailOperator(
    task_id='faile_SMS_ACTIVATION_REQUEST_KPI',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMS_ACTIVATION_REQUEST_KPI),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMS_ACTIVATION_REQUEST_KPI >> PCF_SMS_ACTIVATION_REQUEST_KPI >> PCFCheck_SMS_ACTIVATION_REQUEST_KPI  >> Dedup_SMS_ACTIVATION_REQUEST_KPI >> Success
Validation >> Branching_SMS_ACTIVATION_REQUEST_KPI >> Success
Validation >> Branching_SMS_ACTIVATION_REQUEST_KPI >> Dedup2_SMS_ACTIVATION_REQUEST_KPI >> Success
Validation >> Branching_SMS_ACTIVATION_REQUEST_KPI >> faile_SMS_ACTIVATION_REQUEST_KPI


feedname_RECHARGE_SUCCESS_RATE = 'RECHARGE_SUCCESS_RATE'.upper()
feedname2_RECHARGE_SUCCESS_RATE = 'RECHARGE_SUCCESS_RATE'.lower()

def branch_RECHARGE_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_RECHARGE_SUCCESS_RATE,feedname2_RECHARGE_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_RECHARGE_SUCCESS_RATE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_RECHARGE_SUCCESS_RATE)
    logging.info('branch_RECHARGE_SUCCESS_RATE' + x)
    return x

Branching_RECHARGE_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_RECHARGE_SUCCESS_RATE',
    python_callable=branch_RECHARGE_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='PCF_RECHARGE_SUCCESS_RATE',
    bash_command= pcf_command_non.format(d_1,feedname_RECHARGE_SUCCESS_RATE),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='Dedup_RECHARGE_SUCCESS_RATE',
    bash_command= dedup_hash_command.format(feedname_RECHARGE_SUCCESS_RATE),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_RECHARGE_SUCCESS_RATE',
    bash_command= dedup_hash_command.format(feedname_RECHARGE_SUCCESS_RATE),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_RECHARGE_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_RECHARGE_SUCCESS_RATE',
    bash_command=PCFCheck_command.format(feedname_RECHARGE_SUCCESS_RATE,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_RECHARGE_SUCCESS_RATE = EmailOperator(
    task_id='faile_RECHARGE_SUCCESS_RATE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_RECHARGE_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_RECHARGE_SUCCESS_RATE >> PCF_RECHARGE_SUCCESS_RATE >> PCFCheck_RECHARGE_SUCCESS_RATE  >> Dedup_RECHARGE_SUCCESS_RATE >> Success
Validation >> Branching_RECHARGE_SUCCESS_RATE >> Success
Validation >> Branching_RECHARGE_SUCCESS_RATE >> Dedup2_RECHARGE_SUCCESS_RATE >> Success
Validation >> Branching_RECHARGE_SUCCESS_RATE >> faile_RECHARGE_SUCCESS_RATE


feedname_USSD_ERROR_CODE_BREAKDOWN = 'USSD_ERROR_CODE_BREAKDOWN'.upper()
feedname2_USSD_ERROR_CODE_BREAKDOWN = 'USSD_ERROR_CODE_BREAKDOWN'.lower()

def branch_USSD_ERROR_CODE_BREAKDOWN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_USSD_ERROR_CODE_BREAKDOWN,feedname2_USSD_ERROR_CODE_BREAKDOWN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_USSD_ERROR_CODE_BREAKDOWN,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_USSD_ERROR_CODE_BREAKDOWN)
    logging.info('branch_USSD_ERROR_CODE_BREAKDOWN' + x)
    return x

Branching_USSD_ERROR_CODE_BREAKDOWN = BranchPythonOperator(
    task_id='branchid_USSD_ERROR_CODE_BREAKDOWN',
    python_callable=branch_USSD_ERROR_CODE_BREAKDOWN,
    dag=dag,
    run_as_user='daasuser'
)

PCF_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='PCF_USSD_ERROR_CODE_BREAKDOWN',
    bash_command= pcf_command_non.format(d_1,feedname_USSD_ERROR_CODE_BREAKDOWN),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='Dedup_USSD_ERROR_CODE_BREAKDOWN',
    bash_command= dedup_hash_command.format(feedname_USSD_ERROR_CODE_BREAKDOWN),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='Dedup2_USSD_ERROR_CODE_BREAKDOWN',
    bash_command= dedup_hash_command.format(feedname_USSD_ERROR_CODE_BREAKDOWN),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_USSD_ERROR_CODE_BREAKDOWN = BashOperator(
    task_id='PCFCheck_USSD_ERROR_CODE_BREAKDOWN',
    bash_command=PCFCheck_command.format(feedname_USSD_ERROR_CODE_BREAKDOWN,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_USSD_ERROR_CODE_BREAKDOWN = EmailOperator(
    task_id='faile_USSD_ERROR_CODE_BREAKDOWN',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_USSD_ERROR_CODE_BREAKDOWN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_USSD_ERROR_CODE_BREAKDOWN >> PCF_USSD_ERROR_CODE_BREAKDOWN >> PCFCheck_USSD_ERROR_CODE_BREAKDOWN  >> Dedup_USSD_ERROR_CODE_BREAKDOWN >> Success
Validation >> Branching_USSD_ERROR_CODE_BREAKDOWN >> Success
Validation >> Branching_USSD_ERROR_CODE_BREAKDOWN >> Dedup2_USSD_ERROR_CODE_BREAKDOWN >> Success
Validation >> Branching_USSD_ERROR_CODE_BREAKDOWN >> faile_USSD_ERROR_CODE_BREAKDOWN


feedname_AUDIT_LOGS = 'AUDIT_LOGS'.upper()
feedname2_AUDIT_LOGS = 'AUDIT_LOGS'.lower()

def branch_AUDIT_LOGS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_AUDIT_LOGS,feedname2_AUDIT_LOGS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_AUDIT_LOGS,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_AUDIT_LOGS)
    logging.info('branch_AUDIT_LOGS' + x)
    return x

Branching_AUDIT_LOGS = BranchPythonOperator(
    task_id='branchid_AUDIT_LOGS',
    python_callable=branch_AUDIT_LOGS,
    dag=dag,
    run_as_user='daasuser'
)

PCF_AUDIT_LOGS = BashOperator(
    task_id='PCF_AUDIT_LOGS',
    bash_command= pcf_command_non.format(d_1,feedname_AUDIT_LOGS),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_AUDIT_LOGS = BashOperator(
    task_id='Dedup_AUDIT_LOGS',
    bash_command= dedup_hash_command.format(feedname_AUDIT_LOGS),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_AUDIT_LOGS = BashOperator(
    task_id='Dedup2_AUDIT_LOGS',
    bash_command= dedup_hash_command.format(feedname_AUDIT_LOGS),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_AUDIT_LOGS = BashOperator(
    task_id='PCFCheck_AUDIT_LOGS',
    bash_command=PCFCheck_command.format(feedname_AUDIT_LOGS,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_AUDIT_LOGS = EmailOperator(
    task_id='faile_AUDIT_LOGS',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_AUDIT_LOGS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_AUDIT_LOGS >> PCF_AUDIT_LOGS >> PCFCheck_AUDIT_LOGS  >> Dedup_AUDIT_LOGS >> Success
Validation >> Branching_AUDIT_LOGS >> Success
Validation >> Branching_AUDIT_LOGS >> Dedup2_AUDIT_LOGS >> Success
Validation >> Branching_AUDIT_LOGS >> faile_AUDIT_LOGS


feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = 'DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE'.upper()
feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = 'DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE'.lower()

def branch_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE)
    logging.info('branch_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE' + x)
    return x

Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BranchPythonOperator(
    task_id='branchid_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    python_callable=branch_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='PCF_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command= pcf_command_non.format(d_1,feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='Dedup_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command= dedup_hash_command.format(feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='Dedup2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command= dedup_hash_command.format(feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = BashOperator(
    task_id='PCFCheck_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    bash_command=PCFCheck_command.format(feedname_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE = EmailOperator(
    task_id='faile_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> PCF_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> PCFCheck_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE  >> Dedup_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Success
Validation >> Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Success
Validation >> Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Dedup2_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> Success
Validation >> Branching_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE >> faile_DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE


feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = 'SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE'.upper()
feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = 'SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE'.lower()

def branch_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE)
    logging.info('branch_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE' + x)
    return x

Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    python_callable=branch_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='PCF_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command= pcf_command_non.format(d_1,feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='Dedup_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command= dedup_hash_command.format(feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command= dedup_hash_command.format(feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    bash_command=PCFCheck_command.format(feedname_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE = EmailOperator(
    task_id='faile_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> PCF_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> PCFCheck_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE  >> Dedup_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Success
Validation >> Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Success
Validation >> Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Dedup2_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> Success
Validation >> Branching_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE >> faile_SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE


feedname_EMM_DELIVERY_KPI = 'EMM_DELIVERY_KPI'.upper()
feedname2_EMM_DELIVERY_KPI = 'EMM_DELIVERY_KPI'.lower()

def branch_EMM_DELIVERY_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_EMM_DELIVERY_KPI,feedname2_EMM_DELIVERY_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_EMM_DELIVERY_KPI,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_EMM_DELIVERY_KPI)
    logging.info('branch_EMM_DELIVERY_KPI' + x)
    return x

Branching_EMM_DELIVERY_KPI = BranchPythonOperator(
    task_id='branchid_EMM_DELIVERY_KPI',
    python_callable=branch_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser'
)

PCF_EMM_DELIVERY_KPI = BashOperator(
    task_id='PCF_EMM_DELIVERY_KPI',
    bash_command= pcf_command_non.format(d_1,feedname_EMM_DELIVERY_KPI),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_EMM_DELIVERY_KPI = BashOperator(
    task_id='Dedup_EMM_DELIVERY_KPI',
    bash_command= dedup_hash_command.format(feedname_EMM_DELIVERY_KPI),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_EMM_DELIVERY_KPI = BashOperator(
    task_id='Dedup2_EMM_DELIVERY_KPI',
    bash_command= dedup_hash_command.format(feedname_EMM_DELIVERY_KPI),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_EMM_DELIVERY_KPI = BashOperator(
    task_id='PCFCheck_EMM_DELIVERY_KPI',
    bash_command=PCFCheck_command.format(feedname_EMM_DELIVERY_KPI,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_EMM_DELIVERY_KPI = EmailOperator(
    task_id='faile_EMM_DELIVERY_KPI',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_EMM_DELIVERY_KPI),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_EMM_DELIVERY_KPI >> PCF_EMM_DELIVERY_KPI >> PCFCheck_EMM_DELIVERY_KPI  >> Dedup_EMM_DELIVERY_KPI >> Success
Validation >> Branching_EMM_DELIVERY_KPI >> Success
Validation >> Branching_EMM_DELIVERY_KPI >> Dedup2_EMM_DELIVERY_KPI >> Success
Validation >> Branching_EMM_DELIVERY_KPI >> faile_EMM_DELIVERY_KPI


feedname_FINANCIAL_LOG = 'FINANCIAL_LOG'.upper()
feedname2_FINANCIAL_LOG = 'FINANCIAL_LOG'.lower()

def branch_FINANCIAL_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_FINANCIAL_LOG,feedname2_FINANCIAL_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_FINANCIAL_LOG,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_FINANCIAL_LOG)
    logging.info('branch_FINANCIAL_LOG' + x)
    return x

Branching_FINANCIAL_LOG = BranchPythonOperator(
    task_id='branchid_FINANCIAL_LOG',
    python_callable=branch_FINANCIAL_LOG,
    dag=dag,
    run_as_user='daasuser'
)

PCF_FINANCIAL_LOG = BashOperator(
    task_id='PCF_FINANCIAL_LOG',
    bash_command= pcf_command_non.format(d_1,feedname_FINANCIAL_LOG),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_FINANCIAL_LOG = BashOperator(
    task_id='Dedup_FINANCIAL_LOG',
    bash_command= dedup_hash_command.format(feedname_FINANCIAL_LOG),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_FINANCIAL_LOG = BashOperator(
    task_id='Dedup2_FINANCIAL_LOG',
    bash_command= dedup_hash_command.format(feedname_FINANCIAL_LOG),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_FINANCIAL_LOG = BashOperator(
    task_id='PCFCheck_FINANCIAL_LOG',
    bash_command=PCFCheck_command.format(feedname_FINANCIAL_LOG,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_FINANCIAL_LOG = EmailOperator(
    task_id='faile_FINANCIAL_LOG',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_FINANCIAL_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_FINANCIAL_LOG >> PCF_FINANCIAL_LOG >> PCFCheck_FINANCIAL_LOG  >> Dedup_FINANCIAL_LOG >> Success
Validation >> Branching_FINANCIAL_LOG >> Success
Validation >> Branching_FINANCIAL_LOG >> Dedup2_FINANCIAL_LOG >> Success
Validation >> Branching_FINANCIAL_LOG >> faile_FINANCIAL_LOG


feedname_MANUAL_KPI = 'MANUAL_KPI'.upper()
feedname2_MANUAL_KPI = 'MANUAL_KPI'.lower()

def branch_MANUAL_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MANUAL_KPI,feedname2_MANUAL_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MANUAL_KPI,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MANUAL_KPI)
    logging.info('branch_MANUAL_KPI' + x)
    return x

Branching_MANUAL_KPI = BranchPythonOperator(
    task_id='branchid_MANUAL_KPI',
    python_callable=branch_MANUAL_KPI,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MANUAL_KPI = BashOperator(
    task_id='PCF_MANUAL_KPI',
    bash_command= pcf_command_non.format(d_1,feedname_MANUAL_KPI),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_MANUAL_KPI = BashOperator(
    task_id='Dedup_MANUAL_KPI',
    bash_command= dedup_hash_command.format(feedname_MANUAL_KPI),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MANUAL_KPI = BashOperator(
    task_id='Dedup2_MANUAL_KPI',
    bash_command= dedup_hash_command.format(feedname_MANUAL_KPI),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MANUAL_KPI = BashOperator(
    task_id='PCFCheck_MANUAL_KPI',
    bash_command=PCFCheck_command.format(feedname_MANUAL_KPI,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MANUAL_KPI = EmailOperator(
    task_id='faile_MANUAL_KPI',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MANUAL_KPI),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MANUAL_KPI >> PCF_MANUAL_KPI >> PCFCheck_MANUAL_KPI  >> Dedup_MANUAL_KPI >> Success
Validation >> Branching_MANUAL_KPI >> Success
Validation >> Branching_MANUAL_KPI >> Dedup2_MANUAL_KPI >> Success
Validation >> Branching_MANUAL_KPI >> faile_MANUAL_KPI


feedname_ECW_TRANSACTION = 'ECW_TRANSACTION'.upper()
feedname2_ECW_TRANSACTION = 'ECW_TRANSACTION'.lower()

def branch_ECW_TRANSACTION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ECW_TRANSACTION,feedname2_ECW_TRANSACTION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ECW_TRANSACTION,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ECW_TRANSACTION)
    logging.info('branch_ECW_TRANSACTION' + x)
    return x

Branching_ECW_TRANSACTION = BranchPythonOperator(
    task_id='branchid_ECW_TRANSACTION',
    python_callable=branch_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ECW_TRANSACTION = BashOperator(
    task_id='PCF_ECW_TRANSACTION',
    bash_command= pcf_command_non.format(d_1,feedname_ECW_TRANSACTION),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ECW_TRANSACTION = BashOperator(
    task_id='Dedup_ECW_TRANSACTION',
    bash_command= dedup_hash_command.format(feedname_ECW_TRANSACTION),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ECW_TRANSACTION = BashOperator(
    task_id='Dedup2_ECW_TRANSACTION',
    bash_command= dedup_hash_command.format(feedname_ECW_TRANSACTION),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ECW_TRANSACTION = BashOperator(
    task_id='PCFCheck_ECW_TRANSACTION',
    bash_command=PCFCheck_command.format(feedname_ECW_TRANSACTION,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ECW_TRANSACTION = EmailOperator(
    task_id='faile_ECW_TRANSACTION',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ECW_TRANSACTION),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ECW_TRANSACTION >> PCF_ECW_TRANSACTION >> PCFCheck_ECW_TRANSACTION  >> Dedup_ECW_TRANSACTION >> Success
Validation >> Branching_ECW_TRANSACTION >> Success
Validation >> Branching_ECW_TRANSACTION >> Dedup2_ECW_TRANSACTION >> Success
Validation >> Branching_ECW_TRANSACTION >> faile_ECW_TRANSACTION


feedname_ESM_DYA_METRIC = 'ESM_DYA_METRIC'.upper()
feedname2_ESM_DYA_METRIC = 'ESM_DYA_METRIC'.lower()

def branch_ESM_DYA_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_DYA_METRIC,feedname2_ESM_DYA_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_DYA_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_DYA_METRIC)
    logging.info('branch_ESM_DYA_METRIC' + x)
    return x

Branching_ESM_DYA_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_DYA_METRIC',
    python_callable=branch_ESM_DYA_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_DYA_METRIC = BashOperator(
    task_id='PCF_ESM_DYA_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_DYA_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_DYA_METRIC = BashOperator(
    task_id='Dedup_ESM_DYA_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_DYA_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_DYA_METRIC = BashOperator(
    task_id='Dedup2_ESM_DYA_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_DYA_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_DYA_METRIC = BashOperator(
    task_id='PCFCheck_ESM_DYA_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_DYA_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_DYA_METRIC = EmailOperator(
    task_id='faile_ESM_DYA_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_DYA_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_DYA_METRIC >> PCF_ESM_DYA_METRIC >> PCFCheck_ESM_DYA_METRIC  >> Dedup_ESM_DYA_METRIC >> Success
Validation >> Branching_ESM_DYA_METRIC >> Success
Validation >> Branching_ESM_DYA_METRIC >> Dedup2_ESM_DYA_METRIC >> Success
Validation >> Branching_ESM_DYA_METRIC >> faile_ESM_DYA_METRIC


feedname_ESM_PMT_METRIC = 'ESM_PMT_METRIC'.upper()
feedname2_ESM_PMT_METRIC = 'ESM_PMT_METRIC'.lower()

def branch_ESM_PMT_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_PMT_METRIC,feedname2_ESM_PMT_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_PMT_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_PMT_METRIC)
    logging.info('branch_ESM_PMT_METRIC' + x)
    return x

Branching_ESM_PMT_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_PMT_METRIC',
    python_callable=branch_ESM_PMT_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_PMT_METRIC = BashOperator(
    task_id='PCF_ESM_PMT_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_PMT_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_PMT_METRIC = BashOperator(
    task_id='Dedup_ESM_PMT_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_PMT_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_PMT_METRIC = BashOperator(
    task_id='Dedup2_ESM_PMT_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_PMT_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_PMT_METRIC = BashOperator(
    task_id='PCFCheck_ESM_PMT_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_PMT_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_PMT_METRIC = EmailOperator(
    task_id='faile_ESM_PMT_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_PMT_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_PMT_METRIC >> PCF_ESM_PMT_METRIC >> PCFCheck_ESM_PMT_METRIC  >> Dedup_ESM_PMT_METRIC >> Success
Validation >> Branching_ESM_PMT_METRIC >> Success
Validation >> Branching_ESM_PMT_METRIC >> Dedup2_ESM_PMT_METRIC >> Success
Validation >> Branching_ESM_PMT_METRIC >> faile_ESM_PMT_METRIC


feedname_ESM_REFILL_METRIC = 'ESM_REFILL_METRIC'.upper()
feedname2_ESM_REFILL_METRIC = 'ESM_REFILL_METRIC'.lower()

def branch_ESM_REFILL_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_REFILL_METRIC,feedname2_ESM_REFILL_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_REFILL_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_REFILL_METRIC)
    logging.info('branch_ESM_REFILL_METRIC' + x)
    return x

Branching_ESM_REFILL_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_REFILL_METRIC',
    python_callable=branch_ESM_REFILL_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_REFILL_METRIC = BashOperator(
    task_id='PCF_ESM_REFILL_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_REFILL_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_REFILL_METRIC = BashOperator(
    task_id='Dedup_ESM_REFILL_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_REFILL_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_REFILL_METRIC = BashOperator(
    task_id='Dedup2_ESM_REFILL_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_REFILL_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_REFILL_METRIC = BashOperator(
    task_id='PCFCheck_ESM_REFILL_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_REFILL_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_REFILL_METRIC = EmailOperator(
    task_id='faile_ESM_REFILL_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_REFILL_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_REFILL_METRIC >> PCF_ESM_REFILL_METRIC >> PCFCheck_ESM_REFILL_METRIC  >> Dedup_ESM_REFILL_METRIC >> Success
Validation >> Branching_ESM_REFILL_METRIC >> Success
Validation >> Branching_ESM_REFILL_METRIC >> Dedup2_ESM_REFILL_METRIC >> Success
Validation >> Branching_ESM_REFILL_METRIC >> faile_ESM_REFILL_METRIC


feedname_ESM_SIM_REG_METRIC = 'ESM_SIM_REG_METRIC'.upper()
feedname2_ESM_SIM_REG_METRIC = 'ESM_SIM_REG_METRIC'.lower()

def branch_ESM_SIM_REG_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_SIM_REG_METRIC,feedname2_ESM_SIM_REG_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_SIM_REG_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_SIM_REG_METRIC)
    logging.info('branch_ESM_SIM_REG_METRIC' + x)
    return x

Branching_ESM_SIM_REG_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_SIM_REG_METRIC',
    python_callable=branch_ESM_SIM_REG_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_SIM_REG_METRIC = BashOperator(
    task_id='PCF_ESM_SIM_REG_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_SIM_REG_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_SIM_REG_METRIC = BashOperator(
    task_id='Dedup_ESM_SIM_REG_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_SIM_REG_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_SIM_REG_METRIC = BashOperator(
    task_id='Dedup2_ESM_SIM_REG_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_SIM_REG_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_SIM_REG_METRIC = BashOperator(
    task_id='PCFCheck_ESM_SIM_REG_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_SIM_REG_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_SIM_REG_METRIC = EmailOperator(
    task_id='faile_ESM_SIM_REG_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_SIM_REG_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_SIM_REG_METRIC >> PCF_ESM_SIM_REG_METRIC >> PCFCheck_ESM_SIM_REG_METRIC  >> Dedup_ESM_SIM_REG_METRIC >> Success
Validation >> Branching_ESM_SIM_REG_METRIC >> Success
Validation >> Branching_ESM_SIM_REG_METRIC >> Dedup2_ESM_SIM_REG_METRIC >> Success
Validation >> Branching_ESM_SIM_REG_METRIC >> faile_ESM_SIM_REG_METRIC


feedname_ESM_SUB_METRIC = 'ESM_SUB_METRIC'.upper()
feedname2_ESM_SUB_METRIC = 'ESM_SUB_METRIC'.lower()

def branch_ESM_SUB_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_SUB_METRIC,feedname2_ESM_SUB_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_SUB_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_SUB_METRIC)
    logging.info('branch_ESM_SUB_METRIC' + x)
    return x

Branching_ESM_SUB_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_SUB_METRIC',
    python_callable=branch_ESM_SUB_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_SUB_METRIC = BashOperator(
    task_id='PCF_ESM_SUB_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_SUB_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_SUB_METRIC = BashOperator(
    task_id='Dedup_ESM_SUB_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_SUB_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_SUB_METRIC = BashOperator(
    task_id='Dedup2_ESM_SUB_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_SUB_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_SUB_METRIC = BashOperator(
    task_id='PCFCheck_ESM_SUB_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_SUB_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_SUB_METRIC = EmailOperator(
    task_id='faile_ESM_SUB_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_SUB_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_SUB_METRIC >> PCF_ESM_SUB_METRIC >> PCFCheck_ESM_SUB_METRIC  >> Dedup_ESM_SUB_METRIC >> Success
Validation >> Branching_ESM_SUB_METRIC >> Success
Validation >> Branching_ESM_SUB_METRIC >> Dedup2_ESM_SUB_METRIC >> Success
Validation >> Branching_ESM_SUB_METRIC >> faile_ESM_SUB_METRIC


feedname_ESM_UNSUB_METRIC = 'ESM_UNSUB_METRIC'.upper()
feedname2_ESM_UNSUB_METRIC = 'ESM_UNSUB_METRIC'.lower()

def branch_ESM_UNSUB_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_UNSUB_METRIC,feedname2_ESM_UNSUB_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_UNSUB_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_UNSUB_METRIC)
    logging.info('branch_ESM_UNSUB_METRIC' + x)
    return x

Branching_ESM_UNSUB_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_UNSUB_METRIC',
    python_callable=branch_ESM_UNSUB_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_UNSUB_METRIC = BashOperator(
    task_id='PCF_ESM_UNSUB_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_UNSUB_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_UNSUB_METRIC = BashOperator(
    task_id='Dedup_ESM_UNSUB_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_UNSUB_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_UNSUB_METRIC = BashOperator(
    task_id='Dedup2_ESM_UNSUB_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_UNSUB_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_UNSUB_METRIC = BashOperator(
    task_id='PCFCheck_ESM_UNSUB_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_UNSUB_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_UNSUB_METRIC = EmailOperator(
    task_id='faile_ESM_UNSUB_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_UNSUB_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_UNSUB_METRIC >> PCF_ESM_UNSUB_METRIC >> PCFCheck_ESM_UNSUB_METRIC  >> Dedup_ESM_UNSUB_METRIC >> Success
Validation >> Branching_ESM_UNSUB_METRIC >> Success
Validation >> Branching_ESM_UNSUB_METRIC >> Dedup2_ESM_UNSUB_METRIC >> Success
Validation >> Branching_ESM_UNSUB_METRIC >> faile_ESM_UNSUB_METRIC


feedname_ESM_VTU_VENDING_METRIC = 'ESM_VTU_VENDING_METRIC'.upper()
feedname2_ESM_VTU_VENDING_METRIC = 'ESM_VTU_VENDING_METRIC'.lower()

def branch_ESM_VTU_VENDING_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_ESM_VTU_VENDING_METRIC,feedname2_ESM_VTU_VENDING_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_ESM_VTU_VENDING_METRIC,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_ESM_VTU_VENDING_METRIC)
    logging.info('branch_ESM_VTU_VENDING_METRIC' + x)
    return x

Branching_ESM_VTU_VENDING_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_VTU_VENDING_METRIC',
    python_callable=branch_ESM_VTU_VENDING_METRIC,
    dag=dag,
    run_as_user='daasuser'
)

PCF_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='PCF_ESM_VTU_VENDING_METRIC',
    bash_command= pcf_command_non.format(d_1,feedname_ESM_VTU_VENDING_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='Dedup_ESM_VTU_VENDING_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_VTU_VENDING_METRIC),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='Dedup2_ESM_VTU_VENDING_METRIC',
    bash_command= dedup_hash_command.format(feedname_ESM_VTU_VENDING_METRIC),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='PCFCheck_ESM_VTU_VENDING_METRIC',
    bash_command=PCFCheck_command.format(feedname_ESM_VTU_VENDING_METRIC,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_ESM_VTU_VENDING_METRIC = EmailOperator(
    task_id='faile_ESM_VTU_VENDING_METRIC',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_VTU_VENDING_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_ESM_VTU_VENDING_METRIC >> PCF_ESM_VTU_VENDING_METRIC >> PCFCheck_ESM_VTU_VENDING_METRIC  >> Dedup_ESM_VTU_VENDING_METRIC >> Success
Validation >> Branching_ESM_VTU_VENDING_METRIC >> Success
Validation >> Branching_ESM_VTU_VENDING_METRIC >> Dedup2_ESM_VTU_VENDING_METRIC >> Success
Validation >> Branching_ESM_VTU_VENDING_METRIC >> faile_ESM_VTU_VENDING_METRIC


feedname_SMART_APP_CDR = 'SMART_APP_CDR'.upper()
feedname2_SMART_APP_CDR = 'SMART_APP_CDR'.lower()

def branch_SMART_APP_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMART_APP_CDR,feedname2_SMART_APP_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMART_APP_CDR,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMART_APP_CDR)
    logging.info('branch_SMART_APP_CDR' + x)
    return x

Branching_SMART_APP_CDR = BranchPythonOperator(
    task_id='branchid_SMART_APP_CDR',
    python_callable=branch_SMART_APP_CDR,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMART_APP_CDR = BashOperator(
    task_id='PCF_SMART_APP_CDR',
    bash_command= pcf_command_non.format(d_1,feedname_SMART_APP_CDR),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_SMART_APP_CDR = BashOperator(
    task_id='Dedup_SMART_APP_CDR',
    bash_command= dedup_hash_command.format(feedname_SMART_APP_CDR),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMART_APP_CDR = BashOperator(
    task_id='Dedup2_SMART_APP_CDR',
    bash_command= dedup_hash_command.format(feedname_SMART_APP_CDR),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMART_APP_CDR = BashOperator(
    task_id='PCFCheck_SMART_APP_CDR',
    bash_command=PCFCheck_command.format(feedname_SMART_APP_CDR,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMART_APP_CDR = EmailOperator(
    task_id='faile_SMART_APP_CDR',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMART_APP_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMART_APP_CDR >> PCF_SMART_APP_CDR >> PCFCheck_SMART_APP_CDR  >> Dedup_SMART_APP_CDR >> Success
Validation >> Branching_SMART_APP_CDR >> Success
Validation >> Branching_SMART_APP_CDR >> Dedup2_SMART_APP_CDR >> Success
Validation >> Branching_SMART_APP_CDR >> faile_SMART_APP_CDR


feedname_HSDP_RENEWAL_BASE = 'HSDP_RENEWAL_BASE'.upper()
feedname2_HSDP_RENEWAL_BASE = 'HSDP_RENEWAL_BASE'.lower()

def branch_HSDP_RENEWAL_BASE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_HSDP_RENEWAL_BASE,feedname2_HSDP_RENEWAL_BASE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_HSDP_RENEWAL_BASE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_HSDP_RENEWAL_BASE)
    logging.info('branch_HSDP_RENEWAL_BASE' + x)
    return x

Branching_HSDP_RENEWAL_BASE = BranchPythonOperator(
    task_id='branchid_HSDP_RENEWAL_BASE',
    python_callable=branch_HSDP_RENEWAL_BASE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_HSDP_RENEWAL_BASE = BashOperator(
    task_id='PCF_HSDP_RENEWAL_BASE',
    bash_command= pcf_command_non.format(d_1,feedname_HSDP_RENEWAL_BASE),
    dag=dag,
    run_as_user='daasuser'
)

Dedup_HSDP_RENEWAL_BASE = BashOperator(
    task_id='Dedup_HSDP_RENEWAL_BASE',
    bash_command= dedup_hash_command.format(feedname_HSDP_RENEWAL_BASE),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_HSDP_RENEWAL_BASE = BashOperator(
    task_id='Dedup2_HSDP_RENEWAL_BASE',
    bash_command= dedup_hash_command.format(feedname_HSDP_RENEWAL_BASE),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_HSDP_RENEWAL_BASE = BashOperator(
    task_id='PCFCheck_HSDP_RENEWAL_BASE',
    bash_command=PCFCheck_command.format(feedname_HSDP_RENEWAL_BASE,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_HSDP_RENEWAL_BASE = EmailOperator(
    task_id='faile_HSDP_RENEWAL_BASE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_HSDP_RENEWAL_BASE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_HSDP_RENEWAL_BASE >> PCF_HSDP_RENEWAL_BASE >> PCFCheck_HSDP_RENEWAL_BASE  >> Dedup_HSDP_RENEWAL_BASE >> Success
Validation >> Branching_HSDP_RENEWAL_BASE >> Success
Validation >> Branching_HSDP_RENEWAL_BASE >> Dedup2_HSDP_RENEWAL_BASE >> Success
Validation >> Branching_HSDP_RENEWAL_BASE >> faile_HSDP_RENEWAL_BASE
