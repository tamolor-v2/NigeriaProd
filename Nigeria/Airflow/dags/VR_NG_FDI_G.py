import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
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
Email='test@ligadata.com'
phases = ['Success','Dedup2_','PCF_','faile_']

pathStatus = Variable.get("pathStatus", deserialize_json=True)
sleeptime = Variable.get("sleeptime", deserialize_json=True)
ValidationFDICode = Variable.get("ValidationFDICode", deserialize_json=True)
dedup_command_n1= Variable.get("dedup_command_n1", deserialize_json=True)
dedup_command= Variable.get("dedup_command", deserialize_json=True)
pcf_command= Variable.get("pcf_command", deserialize_json=True)
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
    
dag = DAG('VR_FDI_F', default_args=default_args, catchup=False, schedule_interval='30 16 * * *' )

#ValidationFDICode = ' bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh {0} all ValidationTool_BSL_ALL.conf true 2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log__Airflow_{1}.txt'
logging.info(ValidationFDICode)

#dedup_command="bash /nas/share05/tools/DQ/airflow_tools/Dedup_part.sh {0} {1} {2}"

#pcf_command = "java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.5.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f {1} --logQueries --ignoreDotFilesCheck -p 1 --move"

#PCFCheck_command = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM && hive -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname {0} -date {1} -sleeptime {2}" 

#branchScript = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname {0} -date {1} -sleeptime {2} -retry 200 -outPath {3} -rundate {4} -hour {5}"


Validation = BashOperator(
task_id='Validation',
bash_command= ValidationFDICode.format(d_1,dayStr),
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

feedname_cs5_air_adj_da = 'cs5_air_adj_da'.upper()
feedname2_cs5_air_adj_da = 'cs5_air_adj_da'.lower()

def branch_cs5_air_adj_da():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_air_adj_da,feedname2_cs5_air_adj_da)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_air_adj_da,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_air_adj_da)
    logging.info('branch_cs5_air_adj_da' + x)
    return x

Branching_cs5_air_adj_da = BranchPythonOperator(
    task_id='branchid_cs5_air_adj_da',
    python_callable=branch_cs5_air_adj_da,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_air_adj_da = BashOperator(
    task_id='PCF_cs5_air_adj_da',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_adj_da),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_air_adj_da = BashOperator(
    task_id='Dedup_cs5_air_adj_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_adj_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_air_adj_da = BashOperator(
    task_id='Dedup2_cs5_air_adj_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_adj_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_air_adj_da = BashOperator(
    task_id='PCFCheck_cs5_air_adj_da',
    bash_command=PCFCheck_command.format(feedname_cs5_air_adj_da,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_air_adj_da = EmailOperator(
    task_id='faile_cs5_air_adj_da',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_air_adj_da),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_air_adj_da >> PCF_cs5_air_adj_da >> PCFCheck_cs5_air_adj_da  >> Dedup_cs5_air_adj_da >> Success
Branching_cs5_air_adj_da >> Success
Branching_cs5_air_adj_da >> Dedup2_cs5_air_adj_da >> Success
Branching_cs5_air_adj_da >> faile_cs5_air_adj_da


feedname_cs5_air_refill_ac = 'cs5_air_refill_ac'.upper()
feedname2_cs5_air_refill_ac = 'cs5_air_refill_ac'.lower()

def branch_cs5_air_refill_ac():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_air_refill_ac,feedname2_cs5_air_refill_ac)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_air_refill_ac,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_air_refill_ac)
    logging.info('branch_cs5_air_refill_ac' + x)
    return x

Branching_cs5_air_refill_ac = BranchPythonOperator(
    task_id='branchid_cs5_air_refill_ac',
    python_callable=branch_cs5_air_refill_ac,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_air_refill_ac = BashOperator(
    task_id='PCF_cs5_air_refill_ac',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_refill_ac),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_air_refill_ac = BashOperator(
    task_id='Dedup_cs5_air_refill_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_refill_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_air_refill_ac = BashOperator(
    task_id='Dedup2_cs5_air_refill_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_refill_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_air_refill_ac = BashOperator(
    task_id='PCFCheck_cs5_air_refill_ac',
    bash_command=PCFCheck_command.format(feedname_cs5_air_refill_ac,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_air_refill_ac = EmailOperator(
    task_id='faile_cs5_air_refill_ac',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_air_refill_ac),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_air_refill_ac >> PCF_cs5_air_refill_ac >> PCFCheck_cs5_air_refill_ac  >> Dedup_cs5_air_refill_ac >> Success
Branching_cs5_air_refill_ac >> Success
Branching_cs5_air_refill_ac >> Dedup2_cs5_air_refill_ac >> Success
Branching_cs5_air_refill_ac >> faile_cs5_air_refill_ac


feedname_cs5_air_refill_da = 'cs5_air_refill_da'.upper()
feedname2_cs5_air_refill_da = 'cs5_air_refill_da'.lower()

def branch_cs5_air_refill_da():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_air_refill_da,feedname2_cs5_air_refill_da)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_air_refill_da,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_air_refill_da)
    logging.info('branch_cs5_air_refill_da' + x)
    return x

Branching_cs5_air_refill_da = BranchPythonOperator(
    task_id='branchid_cs5_air_refill_da',
    python_callable=branch_cs5_air_refill_da,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_air_refill_da = BashOperator(
    task_id='PCF_cs5_air_refill_da',
    bash_command= pcf_command.format(d_1,feedname_cs5_air_refill_da),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_air_refill_da = BashOperator(
    task_id='Dedup_cs5_air_refill_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_refill_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_air_refill_da = BashOperator(
    task_id='Dedup2_cs5_air_refill_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_air_refill_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_air_refill_da = BashOperator(
    task_id='PCFCheck_cs5_air_refill_da',
    bash_command=PCFCheck_command.format(feedname_cs5_air_refill_da,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_air_refill_da = EmailOperator(
    task_id='faile_cs5_air_refill_da',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_air_refill_da),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_air_refill_da >> PCF_cs5_air_refill_da >> PCFCheck_cs5_air_refill_da  >> Dedup_cs5_air_refill_da >> Success
Branching_cs5_air_refill_da >> Success
Branching_cs5_air_refill_da >> Dedup2_cs5_air_refill_da >> Success
Branching_cs5_air_refill_da >> faile_cs5_air_refill_da


feedname_cs5_ccn_gprs_ac = 'cs5_ccn_gprs_ac'.upper()
feedname2_cs5_ccn_gprs_ac = 'cs5_ccn_gprs_ac'.lower()

def branch_cs5_ccn_gprs_ac():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_ccn_gprs_ac,feedname2_cs5_ccn_gprs_ac)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_gprs_ac,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_gprs_ac)
    logging.info('branch_cs5_ccn_gprs_ac' + x)
    return x

Branching_cs5_ccn_gprs_ac = BranchPythonOperator(
    task_id='branchid_cs5_ccn_gprs_ac',
    python_callable=branch_cs5_ccn_gprs_ac,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_gprs_ac = BashOperator(
    task_id='PCF_cs5_ccn_gprs_ac',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_gprs_ac),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_gprs_ac = BashOperator(
    task_id='Dedup_cs5_ccn_gprs_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_gprs_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_gprs_ac = BashOperator(
    task_id='Dedup2_cs5_ccn_gprs_ac',
    bash_command= dedup_command_n1.format(run_date, feedname_cs5_ccn_gprs_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_gprs_ac = BashOperator(
    task_id='PCFCheck_cs5_ccn_gprs_ac',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_gprs_ac,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_gprs_ac = EmailOperator(
    task_id='faile_cs5_ccn_gprs_ac',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_gprs_ac),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_ccn_gprs_ac >> PCF_cs5_ccn_gprs_ac >> PCFCheck_cs5_ccn_gprs_ac  >> Dedup_cs5_ccn_gprs_ac >> Success
Branching_cs5_ccn_gprs_ac >> Success
Branching_cs5_ccn_gprs_ac >> Dedup2_cs5_ccn_gprs_ac >> Success
Branching_cs5_ccn_gprs_ac >> faile_cs5_ccn_gprs_ac


feedname_cs5_ccn_gprs_da = 'cs5_ccn_gprs_da'.upper()
feedname2_cs5_ccn_gprs_da = 'cs5_ccn_gprs_da'.lower()

def branch_cs5_ccn_gprs_da():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_ccn_gprs_da,feedname2_cs5_ccn_gprs_da)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_gprs_da,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_gprs_da)
    logging.info('branch_cs5_ccn_gprs_da' + x)
    return x

Branching_cs5_ccn_gprs_da = BranchPythonOperator(
    task_id='branchid_cs5_ccn_gprs_da',
    python_callable=branch_cs5_ccn_gprs_da,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_gprs_da = BashOperator(
    task_id='PCF_cs5_ccn_gprs_da',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_gprs_da),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_gprs_da = BashOperator(
    task_id='Dedup_cs5_ccn_gprs_da',
    bash_command= dedup_command_n1.format(run_date, feedname_cs5_ccn_gprs_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_gprs_da = BashOperator(
    task_id='Dedup2_cs5_ccn_gprs_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_gprs_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_gprs_da = BashOperator(
    task_id='PCFCheck_cs5_ccn_gprs_da',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_gprs_da,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_gprs_da = EmailOperator(
    task_id='faile_cs5_ccn_gprs_da',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_gprs_da),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_ccn_gprs_da >> PCF_cs5_ccn_gprs_da >> PCFCheck_cs5_ccn_gprs_da  >> Dedup_cs5_ccn_gprs_da >> Success
Branching_cs5_ccn_gprs_da >> Success
Branching_cs5_ccn_gprs_da >> Dedup2_cs5_ccn_gprs_da >> Success
Branching_cs5_ccn_gprs_da >> faile_cs5_ccn_gprs_da


feedname_cs5_ccn_sms_ac = 'cs5_ccn_sms_ac'.upper()
feedname2_cs5_ccn_sms_ac = 'cs5_ccn_sms_ac'.lower()

def branch_cs5_ccn_sms_ac():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_ccn_sms_ac,feedname2_cs5_ccn_sms_ac)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_sms_ac,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_sms_ac)
    logging.info('branch_cs5_ccn_sms_ac' + x)
    return x

Branching_cs5_ccn_sms_ac = BranchPythonOperator(
    task_id='branchid_cs5_ccn_sms_ac',
    python_callable=branch_cs5_ccn_sms_ac,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_sms_ac = BashOperator(
    task_id='PCF_cs5_ccn_sms_ac',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_sms_ac),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_sms_ac = BashOperator(
    task_id='Dedup_cs5_ccn_sms_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_sms_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_sms_ac = BashOperator(
    task_id='Dedup2_cs5_ccn_sms_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_sms_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_sms_ac = BashOperator(
    task_id='PCFCheck_cs5_ccn_sms_ac',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_sms_ac,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_sms_ac = EmailOperator(
    task_id='faile_cs5_ccn_sms_ac',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_sms_ac),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_ccn_sms_ac >> PCF_cs5_ccn_sms_ac >> PCFCheck_cs5_ccn_sms_ac  >> Dedup_cs5_ccn_sms_ac >> Success
Branching_cs5_ccn_sms_ac >> Success
Branching_cs5_ccn_sms_ac >> Dedup2_cs5_ccn_sms_ac >> Success
Branching_cs5_ccn_sms_ac >> faile_cs5_ccn_sms_ac


feedname_cs5_ccn_sms_da = 'cs5_ccn_sms_da'.upper()
feedname2_cs5_ccn_sms_da = 'cs5_ccn_sms_da'.lower()

def branch_cs5_ccn_sms_da():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_ccn_sms_da,feedname2_cs5_ccn_sms_da)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_sms_da,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_sms_da)
    logging.info('branch_cs5_ccn_sms_da' + x)
    return x

Branching_cs5_ccn_sms_da = BranchPythonOperator(
    task_id='branchid_cs5_ccn_sms_da',
    python_callable=branch_cs5_ccn_sms_da,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_sms_da = BashOperator(
    task_id='PCF_cs5_ccn_sms_da',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_sms_da),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_sms_da = BashOperator(
    task_id='Dedup_cs5_ccn_sms_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_sms_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_sms_da = BashOperator(
    task_id='Dedup2_cs5_ccn_sms_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_sms_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_sms_da = BashOperator(
    task_id='PCFCheck_cs5_ccn_sms_da',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_sms_da,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_sms_da = EmailOperator(
    task_id='faile_cs5_ccn_sms_da',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_sms_da),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_ccn_sms_da >> PCF_cs5_ccn_sms_da >> PCFCheck_cs5_ccn_sms_da  >> Dedup_cs5_ccn_sms_da >> Success
Branching_cs5_ccn_sms_da >> Success
Branching_cs5_ccn_sms_da >> Dedup2_cs5_ccn_sms_da >> Success
Branching_cs5_ccn_sms_da >> faile_cs5_ccn_sms_da


feedname_cs5_ccn_voice_ac = 'cs5_ccn_voice_ac'.upper()
feedname2_cs5_ccn_voice_ac = 'cs5_ccn_voice_ac'.lower()

def branch_cs5_ccn_voice_ac():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_ccn_voice_ac,feedname2_cs5_ccn_voice_ac)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_voice_ac,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_voice_ac)
    logging.info('branch_cs5_ccn_voice_ac' + x)
    return x

Branching_cs5_ccn_voice_ac = BranchPythonOperator(
    task_id='branchid_cs5_ccn_voice_ac',
    python_callable=branch_cs5_ccn_voice_ac,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_voice_ac = BashOperator(
    task_id='PCF_cs5_ccn_voice_ac',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_voice_ac),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_voice_ac = BashOperator(
    task_id='Dedup_cs5_ccn_voice_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_voice_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_voice_ac = BashOperator(
    task_id='Dedup2_cs5_ccn_voice_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_voice_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_voice_ac = BashOperator(
    task_id='PCFCheck_cs5_ccn_voice_ac',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_voice_ac,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_voice_ac = EmailOperator(
    task_id='faile_cs5_ccn_voice_ac',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_voice_ac),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_ccn_voice_ac >> PCF_cs5_ccn_voice_ac >> PCFCheck_cs5_ccn_voice_ac  >> Dedup_cs5_ccn_voice_ac >> Success
Branching_cs5_ccn_voice_ac >> Success
Branching_cs5_ccn_voice_ac >> Dedup2_cs5_ccn_voice_ac >> Success
Branching_cs5_ccn_voice_ac >> faile_cs5_ccn_voice_ac


feedname_cs5_ccn_voice_da = 'cs5_ccn_voice_da'.upper()
feedname2_cs5_ccn_voice_da = 'cs5_ccn_voice_da'.lower()

def branch_cs5_ccn_voice_da():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_ccn_voice_da,feedname2_cs5_ccn_voice_da)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_ccn_voice_da,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_ccn_voice_da)
    logging.info('branch_cs5_ccn_voice_da' + x)
    return x

Branching_cs5_ccn_voice_da = BranchPythonOperator(
    task_id='branchid_cs5_ccn_voice_da',
    python_callable=branch_cs5_ccn_voice_da,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_ccn_voice_da = BashOperator(
    task_id='PCF_cs5_ccn_voice_da',
    bash_command= pcf_command.format(d_1,feedname_cs5_ccn_voice_da),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_ccn_voice_da = BashOperator(
    task_id='Dedup_cs5_ccn_voice_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_voice_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_ccn_voice_da = BashOperator(
    task_id='Dedup2_cs5_ccn_voice_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_ccn_voice_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_ccn_voice_da = BashOperator(
    task_id='PCFCheck_cs5_ccn_voice_da',
    bash_command=PCFCheck_command.format(feedname_cs5_ccn_voice_da,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_ccn_voice_da = EmailOperator(
    task_id='faile_cs5_ccn_voice_da',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_ccn_voice_da),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_ccn_voice_da >> PCF_cs5_ccn_voice_da >> PCFCheck_cs5_ccn_voice_da  >> Dedup_cs5_ccn_voice_da >> Success
Branching_cs5_ccn_voice_da >> Success
Branching_cs5_ccn_voice_da >> Dedup2_cs5_ccn_voice_da >> Success
Branching_cs5_ccn_voice_da >> faile_cs5_ccn_voice_da


feedname_cs5_sdp_acc_adj_ac = 'cs5_sdp_acc_adj_ac'.upper()
feedname2_cs5_sdp_acc_adj_ac = 'cs5_sdp_acc_adj_ac'.lower()

def branch_cs5_sdp_acc_adj_ac():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_sdp_acc_adj_ac,feedname2_cs5_sdp_acc_adj_ac)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_sdp_acc_adj_ac,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_sdp_acc_adj_ac)
    logging.info('branch_cs5_sdp_acc_adj_ac' + x)
    return x

Branching_cs5_sdp_acc_adj_ac = BranchPythonOperator(
    task_id='branchid_cs5_sdp_acc_adj_ac',
    python_callable=branch_cs5_sdp_acc_adj_ac,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_sdp_acc_adj_ac = BashOperator(
    task_id='PCF_cs5_sdp_acc_adj_ac',
    bash_command= pcf_command.format(d_1,feedname_cs5_sdp_acc_adj_ac),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_sdp_acc_adj_ac = BashOperator(
    task_id='Dedup_cs5_sdp_acc_adj_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_sdp_acc_adj_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_sdp_acc_adj_ac = BashOperator(
    task_id='Dedup2_cs5_sdp_acc_adj_ac',
    bash_command= dedup_command.format(run_date, feedname_cs5_sdp_acc_adj_ac, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_sdp_acc_adj_ac = BashOperator(
    task_id='PCFCheck_cs5_sdp_acc_adj_ac',
    bash_command=PCFCheck_command.format(feedname_cs5_sdp_acc_adj_ac,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_sdp_acc_adj_ac = EmailOperator(
    task_id='faile_cs5_sdp_acc_adj_ac',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_sdp_acc_adj_ac),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_sdp_acc_adj_ac >> PCF_cs5_sdp_acc_adj_ac >> PCFCheck_cs5_sdp_acc_adj_ac  >> Dedup_cs5_sdp_acc_adj_ac >> Success
Branching_cs5_sdp_acc_adj_ac >> Success
Branching_cs5_sdp_acc_adj_ac >> Dedup2_cs5_sdp_acc_adj_ac >> Success
Branching_cs5_sdp_acc_adj_ac >> faile_cs5_sdp_acc_adj_ac


feedname_cs5_sdp_acc_adj_da = 'cs5_sdp_acc_adj_da'.upper()
feedname2_cs5_sdp_acc_adj_da = 'cs5_sdp_acc_adj_da'.lower()

def branch_cs5_sdp_acc_adj_da():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_cs5_sdp_acc_adj_da,feedname2_cs5_sdp_acc_adj_da)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_cs5_sdp_acc_adj_da,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_cs5_sdp_acc_adj_da)
    logging.info('branch_cs5_sdp_acc_adj_da' + x)
    return x

Branching_cs5_sdp_acc_adj_da = BranchPythonOperator(
    task_id='branchid_cs5_sdp_acc_adj_da',
    python_callable=branch_cs5_sdp_acc_adj_da,
    dag=dag,
    run_as_user='daasuser'
)

PCF_cs5_sdp_acc_adj_da = BashOperator(
    task_id='PCF_cs5_sdp_acc_adj_da',
    bash_command= pcf_command.format(d_1,feedname_cs5_sdp_acc_adj_da),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_cs5_sdp_acc_adj_da = BashOperator(
    task_id='Dedup_cs5_sdp_acc_adj_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_sdp_acc_adj_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_cs5_sdp_acc_adj_da = BashOperator(
    task_id='Dedup2_cs5_sdp_acc_adj_da',
    bash_command= dedup_command.format(run_date, feedname_cs5_sdp_acc_adj_da, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_cs5_sdp_acc_adj_da = BashOperator(
    task_id='PCFCheck_cs5_sdp_acc_adj_da',
    bash_command=PCFCheck_command.format(feedname_cs5_sdp_acc_adj_da,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_cs5_sdp_acc_adj_da = EmailOperator(
    task_id='faile_cs5_sdp_acc_adj_da',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_cs5_sdp_acc_adj_da),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_cs5_sdp_acc_adj_da >> PCF_cs5_sdp_acc_adj_da >> PCFCheck_cs5_sdp_acc_adj_da  >> Dedup_cs5_sdp_acc_adj_da >> Success
Branching_cs5_sdp_acc_adj_da >> Success
Branching_cs5_sdp_acc_adj_da >> Dedup2_cs5_sdp_acc_adj_da >> Success
Branching_cs5_sdp_acc_adj_da >> faile_cs5_sdp_acc_adj_da


feedname_msc_daas = 'msc_daas'.upper()
feedname2_msc_daas = 'msc_daas'.lower()

def branch_msc_daas():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_msc_daas,feedname2_msc_daas)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_msc_daas,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_msc_daas)
    logging.info('branch_msc_daas' + x)
    return x

Branching_msc_daas = BranchPythonOperator(
    task_id='branchid_msc_daas',
    python_callable=branch_msc_daas,
    dag=dag,
    run_as_user='daasuser'
)

PCF_msc_daas = BashOperator(
    task_id='PCF_msc_daas',
    bash_command= pcf_command.format(d_1,feedname_msc_daas),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_msc_daas = BashOperator(
    task_id='Dedup_msc_daas',
    bash_command= dedup_command.format(run_date, feedname_msc_daas, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_msc_daas = BashOperator(
    task_id='Dedup2_msc_daas',
    bash_command= dedup_command.format(run_date, feedname_msc_daas, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_msc_daas = BashOperator(
    task_id='PCFCheck_msc_daas',
    bash_command=PCFCheck_command.format(feedname_msc_daas,run_date,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_msc_daas = EmailOperator(
    task_id='faile_msc_daas',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_msc_daas),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Branching_msc_daas >> PCF_msc_daas >> PCFCheck_msc_daas  >> Dedup_msc_daas >> Success
Branching_msc_daas >> Success
Branching_msc_daas >> Dedup2_msc_daas >> Success
Branching_msc_daas >> faile_msc_daas

