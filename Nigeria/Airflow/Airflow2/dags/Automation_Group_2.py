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
d_1 = Variable.get("rerunDate2", deserialize_json=True)
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

dag = DAG('New_Automation_Group_2', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group2 -c config.json' %(d_1)
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


feedname_SGSN_CDR = 'SGSN_CDR'.upper()
feedname2_SGSN_CDR = 'SGSN_CDR'.lower()

dedup_command_SGSN_CDR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SGSN_CDR)
logging.info(dedup_command_SGSN_CDR) 

pcf_command_SGSN_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SGSN_CDR)
logging.info(pcf_command_SGSN_CDR)

PCFCheck_command_SGSN_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SGSN_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_SGSN_CDR)

branchScript_SGSN_CDR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SGSN_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SGSN_CDR)

def branch_SGSN_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SGSN_CDR,feedname2_SGSN_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SGSN_CDR)
    x = readcsv(filepath,feedname2_SGSN_CDR)
    logging.info('branch_SGSN_CDR' + x)
    return x

Branching_SGSN_CDR = BranchPythonOperator(
    task_id='branchid_SGSN_CDR',
    python_callable=branch_SGSN_CDR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SGSN_CDR = BashOperator(
    task_id='PCF_SGSN_CDR',
    bash_command= pcf_command_SGSN_CDR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SGSN_CDR = BashOperator(
    task_id='Dedup_SGSN_CDR',
    bash_command= dedup_command_SGSN_CDR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SGSN_CDR = BashOperator(
    task_id='Dedup2_SGSN_CDR',
    bash_command= dedup_command_SGSN_CDR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SGSN_CDR = BashOperator(
    task_id='PCFCheck_SGSN_CDR',
    bash_command=PCFCheck_command_SGSN_CDR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_SGSN_CDR = EmailOperator(
    task_id='faile_SGSN_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SGSN_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SGSN_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_SGSN_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SGSN_CDR >> PCF_SGSN_CDR >> PCFCheck_SGSN_CDR >> delay_python_task_SGSN_CDR >> Dedup_SGSN_CDR >> Validation_End 
Branching_SGSN_CDR >> Dedup2_SGSN_CDR >> Validation_End
Branching_SGSN_CDR >> faile_SGSN_CDR
Branching_SGSN_CDR >> Validation_End


feedname_MA_MONITOR = 'MA_MONITOR'.upper()
feedname2_MA_MONITOR = 'MA_MONITOR'.lower()

dedup_command_MA_MONITOR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MA_MONITOR)
logging.info(dedup_command_MA_MONITOR) 

pcf_command_MA_MONITOR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_MA_MONITOR)
logging.info(pcf_command_MA_MONITOR)

PCFCheck_command_MA_MONITOR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MA_MONITOR,run_date,sleeptime)
logging.info(PCFCheck_command_MA_MONITOR)

branchScript_MA_MONITOR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MA_MONITOR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MA_MONITOR)

def branch_MA_MONITOR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MA_MONITOR,feedname2_MA_MONITOR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MA_MONITOR)
    x = readcsv(filepath,feedname2_MA_MONITOR)
    logging.info('branch_MA_MONITOR' + x)
    return x

Branching_MA_MONITOR = BranchPythonOperator(
    task_id='branchid_MA_MONITOR',
    python_callable=branch_MA_MONITOR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MA_MONITOR = BashOperator(
    task_id='PCF_MA_MONITOR',
    bash_command= pcf_command_MA_MONITOR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MA_MONITOR = BashOperator(
    task_id='Dedup_MA_MONITOR',
    bash_command= dedup_command_MA_MONITOR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MA_MONITOR = BashOperator(
    task_id='Dedup2_MA_MONITOR',
    bash_command= dedup_command_MA_MONITOR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MA_MONITOR = BashOperator(
    task_id='PCFCheck_MA_MONITOR',
    bash_command=PCFCheck_command_MA_MONITOR,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_MA_MONITOR = EmailOperator(
    task_id='faile_MA_MONITOR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MA_MONITOR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MA_MONITOR: PythonOperator = PythonOperator(task_id="waitForFlush_MA_MONITOR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MA_MONITOR >> PCF_MA_MONITOR >> PCFCheck_MA_MONITOR >> delay_python_task_MA_MONITOR >> Dedup_MA_MONITOR >> Validation_End 
Branching_MA_MONITOR >> Dedup2_MA_MONITOR >> Validation_End
Branching_MA_MONITOR >> faile_MA_MONITOR
Branching_MA_MONITOR >> Validation_End


feedname_SMSC = 'SMSC'.upper()
feedname2_SMSC = 'SMSC'.lower()

dedup_command_SMSC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SMSC)
logging.info(dedup_command_SMSC) 

pcf_command_SMSC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SMSC)
logging.info(pcf_command_SMSC)

PCFCheck_command_SMSC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMSC,run_date,sleeptime)
logging.info(PCFCheck_command_SMSC)

branchScript_SMSC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SMSC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SMSC)

def branch_SMSC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMSC,feedname2_SMSC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMSC)
    x = readcsv(filepath,feedname2_SMSC)
    logging.info('branch_SMSC' + x)
    return x

Branching_SMSC = BranchPythonOperator(
    task_id='branchid_SMSC',
    python_callable=branch_SMSC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMSC = BashOperator(
    task_id='PCF_SMSC',
    bash_command= pcf_command_SMSC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SMSC = BashOperator(
    task_id='Dedup_SMSC',
    bash_command= dedup_command_SMSC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SMSC = BashOperator(
    task_id='Dedup2_SMSC',
    bash_command= dedup_command_SMSC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SMSC = BashOperator(
    task_id='PCFCheck_SMSC',
    bash_command=PCFCheck_command_SMSC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_SMSC = EmailOperator(
    task_id='faile_SMSC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMSC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SMSC: PythonOperator = PythonOperator(task_id="waitForFlush_SMSC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SMSC >> PCF_SMSC >> PCFCheck_SMSC >> delay_python_task_SMSC >> Dedup_SMSC >> Validation_End 
Branching_SMSC >> Dedup2_SMSC >> Validation_End
Branching_SMSC >> faile_SMSC
Branching_SMSC >> Validation_End


feedname_CS5_AIR_ADJ_DA = 'CS5_AIR_ADJ_DA'.upper()
feedname2_CS5_AIR_ADJ_DA = 'CS5_AIR_ADJ_DA'.lower()

dedup_command_CS5_AIR_ADJ_DA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_AIR_ADJ_DA)
logging.info(dedup_command_CS5_AIR_ADJ_DA) 

pcf_command_CS5_AIR_ADJ_DA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_AIR_ADJ_DA)
logging.info(pcf_command_CS5_AIR_ADJ_DA)

PCFCheck_command_CS5_AIR_ADJ_DA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_AIR_ADJ_DA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_AIR_ADJ_DA)

branchScript_CS5_AIR_ADJ_DA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_AIR_ADJ_DA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_AIR_ADJ_DA)

def branch_CS5_AIR_ADJ_DA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_AIR_ADJ_DA,feedname2_CS5_AIR_ADJ_DA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_AIR_ADJ_DA)
    x = readcsv(filepath,feedname2_CS5_AIR_ADJ_DA)
    logging.info('branch_CS5_AIR_ADJ_DA' + x)
    return x

Branching_CS5_AIR_ADJ_DA = BranchPythonOperator(
    task_id='branchid_CS5_AIR_ADJ_DA',
    python_callable=branch_CS5_AIR_ADJ_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_AIR_ADJ_DA = BashOperator(
    task_id='PCF_CS5_AIR_ADJ_DA',
    bash_command= pcf_command_CS5_AIR_ADJ_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_AIR_ADJ_DA = BashOperator(
    task_id='Dedup_CS5_AIR_ADJ_DA',
    bash_command= dedup_command_CS5_AIR_ADJ_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_AIR_ADJ_DA = BashOperator(
    task_id='Dedup2_CS5_AIR_ADJ_DA',
    bash_command= dedup_command_CS5_AIR_ADJ_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_AIR_ADJ_DA = BashOperator(
    task_id='PCFCheck_CS5_AIR_ADJ_DA',
    bash_command=PCFCheck_command_CS5_AIR_ADJ_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_AIR_ADJ_DA = EmailOperator(
    task_id='faile_CS5_AIR_ADJ_DA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_AIR_ADJ_DA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_AIR_ADJ_DA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_AIR_ADJ_DA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_AIR_ADJ_DA >> PCF_CS5_AIR_ADJ_DA >> PCFCheck_CS5_AIR_ADJ_DA >> delay_python_task_CS5_AIR_ADJ_DA >> Dedup_CS5_AIR_ADJ_DA >> Validation_End 
Branching_CS5_AIR_ADJ_DA >> Dedup2_CS5_AIR_ADJ_DA >> Validation_End
Branching_CS5_AIR_ADJ_DA >> faile_CS5_AIR_ADJ_DA
Branching_CS5_AIR_ADJ_DA >> Validation_End


feedname_CS5_AIR_REFILL_AC = 'CS5_AIR_REFILL_AC'.upper()
feedname2_CS5_AIR_REFILL_AC = 'CS5_AIR_REFILL_AC'.lower()

dedup_command_CS5_AIR_REFILL_AC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_AIR_REFILL_AC)
logging.info(dedup_command_CS5_AIR_REFILL_AC) 

pcf_command_CS5_AIR_REFILL_AC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_AIR_REFILL_AC)
logging.info(pcf_command_CS5_AIR_REFILL_AC)

PCFCheck_command_CS5_AIR_REFILL_AC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_AIR_REFILL_AC,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_AIR_REFILL_AC)

branchScript_CS5_AIR_REFILL_AC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_AIR_REFILL_AC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_AIR_REFILL_AC)

def branch_CS5_AIR_REFILL_AC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_AIR_REFILL_AC,feedname2_CS5_AIR_REFILL_AC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_AIR_REFILL_AC)
    x = readcsv(filepath,feedname2_CS5_AIR_REFILL_AC)
    logging.info('branch_CS5_AIR_REFILL_AC' + x)
    return x

Branching_CS5_AIR_REFILL_AC = BranchPythonOperator(
    task_id='branchid_CS5_AIR_REFILL_AC',
    python_callable=branch_CS5_AIR_REFILL_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_AIR_REFILL_AC = BashOperator(
    task_id='PCF_CS5_AIR_REFILL_AC',
    bash_command= pcf_command_CS5_AIR_REFILL_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_AIR_REFILL_AC = BashOperator(
    task_id='Dedup_CS5_AIR_REFILL_AC',
    bash_command= dedup_command_CS5_AIR_REFILL_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_AIR_REFILL_AC = BashOperator(
    task_id='Dedup2_CS5_AIR_REFILL_AC',
    bash_command= dedup_command_CS5_AIR_REFILL_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_AIR_REFILL_AC = BashOperator(
    task_id='PCFCheck_CS5_AIR_REFILL_AC',
    bash_command=PCFCheck_command_CS5_AIR_REFILL_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_AIR_REFILL_AC = EmailOperator(
    task_id='faile_CS5_AIR_REFILL_AC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_AIR_REFILL_AC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_AIR_REFILL_AC: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_AIR_REFILL_AC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_AIR_REFILL_AC >> PCF_CS5_AIR_REFILL_AC >> PCFCheck_CS5_AIR_REFILL_AC >> delay_python_task_CS5_AIR_REFILL_AC >> Dedup_CS5_AIR_REFILL_AC >> Validation_End 
Branching_CS5_AIR_REFILL_AC >> Dedup2_CS5_AIR_REFILL_AC >> Validation_End
Branching_CS5_AIR_REFILL_AC >> faile_CS5_AIR_REFILL_AC
Branching_CS5_AIR_REFILL_AC >> Validation_End


feedname_CS5_AIR_REFILL_DA = 'CS5_AIR_REFILL_DA'.upper()
feedname2_CS5_AIR_REFILL_DA = 'CS5_AIR_REFILL_DA'.lower()

dedup_command_CS5_AIR_REFILL_DA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_AIR_REFILL_DA)
logging.info(dedup_command_CS5_AIR_REFILL_DA) 

pcf_command_CS5_AIR_REFILL_DA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_AIR_REFILL_DA)
logging.info(pcf_command_CS5_AIR_REFILL_DA)

PCFCheck_command_CS5_AIR_REFILL_DA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_AIR_REFILL_DA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_AIR_REFILL_DA)

branchScript_CS5_AIR_REFILL_DA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_AIR_REFILL_DA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_AIR_REFILL_DA)

def branch_CS5_AIR_REFILL_DA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_AIR_REFILL_DA,feedname2_CS5_AIR_REFILL_DA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_AIR_REFILL_DA)
    x = readcsv(filepath,feedname2_CS5_AIR_REFILL_DA)
    logging.info('branch_CS5_AIR_REFILL_DA' + x)
    return x

Branching_CS5_AIR_REFILL_DA = BranchPythonOperator(
    task_id='branchid_CS5_AIR_REFILL_DA',
    python_callable=branch_CS5_AIR_REFILL_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_AIR_REFILL_DA = BashOperator(
    task_id='PCF_CS5_AIR_REFILL_DA',
    bash_command= pcf_command_CS5_AIR_REFILL_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_AIR_REFILL_DA = BashOperator(
    task_id='Dedup_CS5_AIR_REFILL_DA',
    bash_command= dedup_command_CS5_AIR_REFILL_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_AIR_REFILL_DA = BashOperator(
    task_id='Dedup2_CS5_AIR_REFILL_DA',
    bash_command= dedup_command_CS5_AIR_REFILL_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_AIR_REFILL_DA = BashOperator(
    task_id='PCFCheck_CS5_AIR_REFILL_DA',
    bash_command=PCFCheck_command_CS5_AIR_REFILL_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_AIR_REFILL_DA = EmailOperator(
    task_id='faile_CS5_AIR_REFILL_DA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_AIR_REFILL_DA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_AIR_REFILL_DA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_AIR_REFILL_DA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_AIR_REFILL_DA >> PCF_CS5_AIR_REFILL_DA >> PCFCheck_CS5_AIR_REFILL_DA >> delay_python_task_CS5_AIR_REFILL_DA >> Dedup_CS5_AIR_REFILL_DA >> Validation_End 
Branching_CS5_AIR_REFILL_DA >> Dedup2_CS5_AIR_REFILL_DA >> Validation_End
Branching_CS5_AIR_REFILL_DA >> faile_CS5_AIR_REFILL_DA
Branching_CS5_AIR_REFILL_DA >> Validation_End


feedname_CS5_CCN_SMS_AC = 'CS5_CCN_SMS_AC'.upper()
feedname2_CS5_CCN_SMS_AC = 'CS5_CCN_SMS_AC'.lower()

dedup_command_CS5_CCN_SMS_AC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_SMS_AC)
logging.info(dedup_command_CS5_CCN_SMS_AC) 

pcf_command_CS5_CCN_SMS_AC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_SMS_AC)
logging.info(pcf_command_CS5_CCN_SMS_AC)

PCFCheck_command_CS5_CCN_SMS_AC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_SMS_AC,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_SMS_AC)

branchScript_CS5_CCN_SMS_AC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_SMS_AC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_SMS_AC)

def branch_CS5_CCN_SMS_AC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_SMS_AC,feedname2_CS5_CCN_SMS_AC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_SMS_AC)
    x = readcsv(filepath,feedname2_CS5_CCN_SMS_AC)
    logging.info('branch_CS5_CCN_SMS_AC' + x)
    return x

Branching_CS5_CCN_SMS_AC = BranchPythonOperator(
    task_id='branchid_CS5_CCN_SMS_AC',
    python_callable=branch_CS5_CCN_SMS_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_CCN_SMS_AC = BashOperator(
    task_id='PCF_CS5_CCN_SMS_AC',
    bash_command= pcf_command_CS5_CCN_SMS_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_CCN_SMS_AC = BashOperator(
    task_id='Dedup_CS5_CCN_SMS_AC',
    bash_command= dedup_command_CS5_CCN_SMS_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_CCN_SMS_AC = BashOperator(
    task_id='Dedup2_CS5_CCN_SMS_AC',
    bash_command= dedup_command_CS5_CCN_SMS_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_CCN_SMS_AC = BashOperator(
    task_id='PCFCheck_CS5_CCN_SMS_AC',
    bash_command=PCFCheck_command_CS5_CCN_SMS_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_CCN_SMS_AC = EmailOperator(
    task_id='faile_CS5_CCN_SMS_AC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_SMS_AC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_SMS_AC: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_SMS_AC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_SMS_AC >> PCF_CS5_CCN_SMS_AC >> PCFCheck_CS5_CCN_SMS_AC >> delay_python_task_CS5_CCN_SMS_AC >> Dedup_CS5_CCN_SMS_AC >> Validation_End 
Branching_CS5_CCN_SMS_AC >> Dedup2_CS5_CCN_SMS_AC >> Validation_End
Branching_CS5_CCN_SMS_AC >> faile_CS5_CCN_SMS_AC
Branching_CS5_CCN_SMS_AC >> Validation_End


feedname_CS5_CCN_SMS_DA = 'CS5_CCN_SMS_DA'.upper()
feedname2_CS5_CCN_SMS_DA = 'CS5_CCN_SMS_DA'.lower()

dedup_command_CS5_CCN_SMS_DA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_SMS_DA)
logging.info(dedup_command_CS5_CCN_SMS_DA) 

pcf_command_CS5_CCN_SMS_DA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_SMS_DA)
logging.info(pcf_command_CS5_CCN_SMS_DA)

PCFCheck_command_CS5_CCN_SMS_DA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_SMS_DA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_SMS_DA)

branchScript_CS5_CCN_SMS_DA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_SMS_DA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_SMS_DA)

def branch_CS5_CCN_SMS_DA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_SMS_DA,feedname2_CS5_CCN_SMS_DA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_SMS_DA)
    x = readcsv(filepath,feedname2_CS5_CCN_SMS_DA)
    logging.info('branch_CS5_CCN_SMS_DA' + x)
    return x

Branching_CS5_CCN_SMS_DA = BranchPythonOperator(
    task_id='branchid_CS5_CCN_SMS_DA',
    python_callable=branch_CS5_CCN_SMS_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_CCN_SMS_DA = BashOperator(
    task_id='PCF_CS5_CCN_SMS_DA',
    bash_command= pcf_command_CS5_CCN_SMS_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_CCN_SMS_DA = BashOperator(
    task_id='Dedup_CS5_CCN_SMS_DA',
    bash_command= dedup_command_CS5_CCN_SMS_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_CCN_SMS_DA = BashOperator(
    task_id='Dedup2_CS5_CCN_SMS_DA',
    bash_command= dedup_command_CS5_CCN_SMS_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_CCN_SMS_DA = BashOperator(
    task_id='PCFCheck_CS5_CCN_SMS_DA',
    bash_command=PCFCheck_command_CS5_CCN_SMS_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_CCN_SMS_DA = EmailOperator(
    task_id='faile_CS5_CCN_SMS_DA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_SMS_DA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_SMS_DA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_SMS_DA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_SMS_DA >> PCF_CS5_CCN_SMS_DA >> PCFCheck_CS5_CCN_SMS_DA >> delay_python_task_CS5_CCN_SMS_DA >> Dedup_CS5_CCN_SMS_DA >> Validation_End 
Branching_CS5_CCN_SMS_DA >> Dedup2_CS5_CCN_SMS_DA >> Validation_End
Branching_CS5_CCN_SMS_DA >> faile_CS5_CCN_SMS_DA
Branching_CS5_CCN_SMS_DA >> Validation_End


feedname_CS5_CCN_VOICE_AC = 'CS5_CCN_VOICE_AC'.upper()
feedname2_CS5_CCN_VOICE_AC = 'CS5_CCN_VOICE_AC'.lower()

dedup_command_CS5_CCN_VOICE_AC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_VOICE_AC)
logging.info(dedup_command_CS5_CCN_VOICE_AC) 

pcf_command_CS5_CCN_VOICE_AC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_VOICE_AC)
logging.info(pcf_command_CS5_CCN_VOICE_AC)

PCFCheck_command_CS5_CCN_VOICE_AC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_VOICE_AC,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_VOICE_AC)

branchScript_CS5_CCN_VOICE_AC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_VOICE_AC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_VOICE_AC)

def branch_CS5_CCN_VOICE_AC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_VOICE_AC,feedname2_CS5_CCN_VOICE_AC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_VOICE_AC)
    x = readcsv(filepath,feedname2_CS5_CCN_VOICE_AC)
    logging.info('branch_CS5_CCN_VOICE_AC' + x)
    return x

Branching_CS5_CCN_VOICE_AC = BranchPythonOperator(
    task_id='branchid_CS5_CCN_VOICE_AC',
    python_callable=branch_CS5_CCN_VOICE_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_CCN_VOICE_AC = BashOperator(
    task_id='PCF_CS5_CCN_VOICE_AC',
    bash_command= pcf_command_CS5_CCN_VOICE_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_CCN_VOICE_AC = BashOperator(
    task_id='Dedup_CS5_CCN_VOICE_AC',
    bash_command= dedup_command_CS5_CCN_VOICE_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_CCN_VOICE_AC = BashOperator(
    task_id='Dedup2_CS5_CCN_VOICE_AC',
    bash_command= dedup_command_CS5_CCN_VOICE_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_CCN_VOICE_AC = BashOperator(
    task_id='PCFCheck_CS5_CCN_VOICE_AC',
    bash_command=PCFCheck_command_CS5_CCN_VOICE_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_CCN_VOICE_AC = EmailOperator(
    task_id='faile_CS5_CCN_VOICE_AC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_VOICE_AC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_VOICE_AC: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_VOICE_AC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_VOICE_AC >> PCF_CS5_CCN_VOICE_AC >> PCFCheck_CS5_CCN_VOICE_AC >> delay_python_task_CS5_CCN_VOICE_AC >> Dedup_CS5_CCN_VOICE_AC >> Validation_End 
Branching_CS5_CCN_VOICE_AC >> Dedup2_CS5_CCN_VOICE_AC >> Validation_End
Branching_CS5_CCN_VOICE_AC >> faile_CS5_CCN_VOICE_AC
Branching_CS5_CCN_VOICE_AC >> Validation_End


feedname_CS5_CCN_VOICE_DA = 'CS5_CCN_VOICE_DA'.upper()
feedname2_CS5_CCN_VOICE_DA = 'CS5_CCN_VOICE_DA'.lower()

dedup_command_CS5_CCN_VOICE_DA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_VOICE_DA)
logging.info(dedup_command_CS5_CCN_VOICE_DA) 

pcf_command_CS5_CCN_VOICE_DA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_VOICE_DA)
logging.info(pcf_command_CS5_CCN_VOICE_DA)

PCFCheck_command_CS5_CCN_VOICE_DA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_VOICE_DA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_VOICE_DA)

branchScript_CS5_CCN_VOICE_DA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_VOICE_DA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_VOICE_DA)

def branch_CS5_CCN_VOICE_DA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_VOICE_DA,feedname2_CS5_CCN_VOICE_DA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_VOICE_DA)
    x = readcsv(filepath,feedname2_CS5_CCN_VOICE_DA)
    logging.info('branch_CS5_CCN_VOICE_DA' + x)
    return x

Branching_CS5_CCN_VOICE_DA = BranchPythonOperator(
    task_id='branchid_CS5_CCN_VOICE_DA',
    python_callable=branch_CS5_CCN_VOICE_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_CCN_VOICE_DA = BashOperator(
    task_id='PCF_CS5_CCN_VOICE_DA',
    bash_command= pcf_command_CS5_CCN_VOICE_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_CCN_VOICE_DA = BashOperator(
    task_id='Dedup_CS5_CCN_VOICE_DA',
    bash_command= dedup_command_CS5_CCN_VOICE_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_CCN_VOICE_DA = BashOperator(
    task_id='Dedup2_CS5_CCN_VOICE_DA',
    bash_command= dedup_command_CS5_CCN_VOICE_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_CCN_VOICE_DA = BashOperator(
    task_id='PCFCheck_CS5_CCN_VOICE_DA',
    bash_command=PCFCheck_command_CS5_CCN_VOICE_DA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_CCN_VOICE_DA = EmailOperator(
    task_id='faile_CS5_CCN_VOICE_DA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_VOICE_DA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_VOICE_DA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_VOICE_DA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_VOICE_DA >> PCF_CS5_CCN_VOICE_DA >> PCFCheck_CS5_CCN_VOICE_DA >> delay_python_task_CS5_CCN_VOICE_DA >> Dedup_CS5_CCN_VOICE_DA >> Validation_End 
Branching_CS5_CCN_VOICE_DA >> Dedup2_CS5_CCN_VOICE_DA >> Validation_End
Branching_CS5_CCN_VOICE_DA >> faile_CS5_CCN_VOICE_DA
Branching_CS5_CCN_VOICE_DA >> Validation_End


feedname_CS5_SDP_ACC_ADJ_AC = 'CS5_SDP_ACC_ADJ_AC'.upper()
feedname2_CS5_SDP_ACC_ADJ_AC = 'CS5_SDP_ACC_ADJ_AC'.lower()

dedup_command_CS5_SDP_ACC_ADJ_AC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_SDP_ACC_ADJ_AC)
logging.info(dedup_command_CS5_SDP_ACC_ADJ_AC) 

pcf_command_CS5_SDP_ACC_ADJ_AC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_SDP_ACC_ADJ_AC)
logging.info(pcf_command_CS5_SDP_ACC_ADJ_AC)

PCFCheck_command_CS5_SDP_ACC_ADJ_AC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_SDP_ACC_ADJ_AC,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_SDP_ACC_ADJ_AC)

branchScript_CS5_SDP_ACC_ADJ_AC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_SDP_ACC_ADJ_AC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_SDP_ACC_ADJ_AC)

def branch_CS5_SDP_ACC_ADJ_AC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_SDP_ACC_ADJ_AC,feedname2_CS5_SDP_ACC_ADJ_AC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_SDP_ACC_ADJ_AC)
    x = readcsv(filepath,feedname2_CS5_SDP_ACC_ADJ_AC)
    logging.info('branch_CS5_SDP_ACC_ADJ_AC' + x)
    return x

Branching_CS5_SDP_ACC_ADJ_AC = BranchPythonOperator(
    task_id='branchid_CS5_SDP_ACC_ADJ_AC',
    python_callable=branch_CS5_SDP_ACC_ADJ_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_SDP_ACC_ADJ_AC = BashOperator(
    task_id='PCF_CS5_SDP_ACC_ADJ_AC',
    bash_command= pcf_command_CS5_SDP_ACC_ADJ_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_SDP_ACC_ADJ_AC = BashOperator(
    task_id='Dedup_CS5_SDP_ACC_ADJ_AC',
    bash_command= dedup_command_CS5_SDP_ACC_ADJ_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_SDP_ACC_ADJ_AC = BashOperator(
    task_id='Dedup2_CS5_SDP_ACC_ADJ_AC',
    bash_command= dedup_command_CS5_SDP_ACC_ADJ_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_SDP_ACC_ADJ_AC = BashOperator(
    task_id='PCFCheck_CS5_SDP_ACC_ADJ_AC',
    bash_command=PCFCheck_command_CS5_SDP_ACC_ADJ_AC,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_SDP_ACC_ADJ_AC = EmailOperator(
    task_id='faile_CS5_SDP_ACC_ADJ_AC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_SDP_ACC_ADJ_AC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_SDP_ACC_ADJ_AC: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_SDP_ACC_ADJ_AC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_SDP_ACC_ADJ_AC >> PCF_CS5_SDP_ACC_ADJ_AC >> PCFCheck_CS5_SDP_ACC_ADJ_AC >> delay_python_task_CS5_SDP_ACC_ADJ_AC >> Dedup_CS5_SDP_ACC_ADJ_AC >> Validation_End 
Branching_CS5_SDP_ACC_ADJ_AC >> Dedup2_CS5_SDP_ACC_ADJ_AC >> Validation_End
Branching_CS5_SDP_ACC_ADJ_AC >> faile_CS5_SDP_ACC_ADJ_AC
Branching_CS5_SDP_ACC_ADJ_AC >> Validation_End


feedname_MSC_DAAS = 'MSC_DAAS'.upper()
feedname2_MSC_DAAS = 'MSC_DAAS'.lower()

dedup_command_MSC_DAAS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MSC_DAAS)
logging.info(dedup_command_MSC_DAAS) 

pcf_command_MSC_DAAS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_MSC_DAAS)
logging.info(pcf_command_MSC_DAAS)

PCFCheck_command_MSC_DAAS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSC_DAAS,run_date,sleeptime)
logging.info(PCFCheck_command_MSC_DAAS)

branchScript_MSC_DAAS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MSC_DAAS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MSC_DAAS)

def branch_MSC_DAAS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSC_DAAS,feedname2_MSC_DAAS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSC_DAAS)
    x = readcsv(filepath,feedname2_MSC_DAAS)
    logging.info('branch_MSC_DAAS' + x)
    return x

Branching_MSC_DAAS = BranchPythonOperator(
    task_id='branchid_MSC_DAAS',
    python_callable=branch_MSC_DAAS,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MSC_DAAS = BashOperator(
    task_id='PCF_MSC_DAAS',
    bash_command= pcf_command_MSC_DAAS,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MSC_DAAS = BashOperator(
    task_id='Dedup_MSC_DAAS',
    bash_command= dedup_command_MSC_DAAS,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MSC_DAAS = BashOperator(
    task_id='Dedup2_MSC_DAAS',
    bash_command= dedup_command_MSC_DAAS,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MSC_DAAS = BashOperator(
    task_id='PCFCheck_MSC_DAAS',
    bash_command=PCFCheck_command_MSC_DAAS,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_MSC_DAAS = EmailOperator(
    task_id='faile_MSC_DAAS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSC_DAAS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MSC_DAAS: PythonOperator = PythonOperator(task_id="waitForFlush_MSC_DAAS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MSC_DAAS >> PCF_MSC_DAAS >> PCFCheck_MSC_DAAS >> delay_python_task_MSC_DAAS >> Dedup_MSC_DAAS >> Validation_End 
Branching_MSC_DAAS >> Dedup2_MSC_DAAS >> Validation_End
Branching_MSC_DAAS >> faile_MSC_DAAS
Branching_MSC_DAAS >> Validation_End


feedname_CS5_SDP_PAM_ALL = 'CS5_SDP_PAM_ALL'.upper()
feedname2_CS5_SDP_PAM_ALL = 'CS5_SDP_PAM_ALL'.lower()

dedup_command_CS5_SDP_PAM_ALL="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_SDP_PAM_ALL)
logging.info(dedup_command_CS5_SDP_PAM_ALL) 

pcf_command_CS5_SDP_PAM_ALL = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_SDP_PAM_ALL)
logging.info(pcf_command_CS5_SDP_PAM_ALL)

PCFCheck_command_CS5_SDP_PAM_ALL = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_SDP_PAM_ALL,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_SDP_PAM_ALL)

branchScript_CS5_SDP_PAM_ALL = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_SDP_PAM_ALL,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_SDP_PAM_ALL)

def branch_CS5_SDP_PAM_ALL():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_SDP_PAM_ALL,feedname2_CS5_SDP_PAM_ALL)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_SDP_PAM_ALL)
    x = readcsv(filepath,feedname2_CS5_SDP_PAM_ALL)
    logging.info('branch_CS5_SDP_PAM_ALL' + x)
    return x

Branching_CS5_SDP_PAM_ALL = BranchPythonOperator(
    task_id='branchid_CS5_SDP_PAM_ALL',
    python_callable=branch_CS5_SDP_PAM_ALL,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_SDP_PAM_ALL = BashOperator(
    task_id='PCF_CS5_SDP_PAM_ALL',
    bash_command= pcf_command_CS5_SDP_PAM_ALL,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_SDP_PAM_ALL = BashOperator(
    task_id='Dedup_CS5_SDP_PAM_ALL',
    bash_command= dedup_command_CS5_SDP_PAM_ALL,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_SDP_PAM_ALL = BashOperator(
    task_id='Dedup2_CS5_SDP_PAM_ALL',
    bash_command= dedup_command_CS5_SDP_PAM_ALL,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_SDP_PAM_ALL = BashOperator(
    task_id='PCFCheck_CS5_SDP_PAM_ALL',
    bash_command=PCFCheck_command_CS5_SDP_PAM_ALL,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_SDP_PAM_ALL = EmailOperator(
    task_id='faile_CS5_SDP_PAM_ALL',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_SDP_PAM_ALL),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_SDP_PAM_ALL: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_SDP_PAM_ALL",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_SDP_PAM_ALL >> PCF_CS5_SDP_PAM_ALL >> PCFCheck_CS5_SDP_PAM_ALL >> delay_python_task_CS5_SDP_PAM_ALL >> Dedup_CS5_SDP_PAM_ALL >> Validation_End 
Branching_CS5_SDP_PAM_ALL >> Dedup2_CS5_SDP_PAM_ALL >> Validation_End
Branching_CS5_SDP_PAM_ALL >> faile_CS5_SDP_PAM_ALL
Branching_CS5_SDP_PAM_ALL >> Validation_End


feedname_USSDGW_CHARGING_CDR = 'USSDGW_CHARGING_CDR'.upper()
feedname2_USSDGW_CHARGING_CDR = 'USSDGW_CHARGING_CDR'.lower()

dedup_command_USSDGW_CHARGING_CDR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_USSDGW_CHARGING_CDR)
logging.info(dedup_command_USSDGW_CHARGING_CDR) 

pcf_command_USSDGW_CHARGING_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_USSDGW_CHARGING_CDR)
logging.info(pcf_command_USSDGW_CHARGING_CDR)

PCFCheck_command_USSDGW_CHARGING_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_USSDGW_CHARGING_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_USSDGW_CHARGING_CDR)

branchScript_USSDGW_CHARGING_CDR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_USSDGW_CHARGING_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_USSDGW_CHARGING_CDR)

def branch_USSDGW_CHARGING_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_USSDGW_CHARGING_CDR,feedname2_USSDGW_CHARGING_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_USSDGW_CHARGING_CDR)
    x = readcsv(filepath,feedname2_USSDGW_CHARGING_CDR)
    logging.info('branch_USSDGW_CHARGING_CDR' + x)
    return x

Branching_USSDGW_CHARGING_CDR = BranchPythonOperator(
    task_id='branchid_USSDGW_CHARGING_CDR',
    python_callable=branch_USSDGW_CHARGING_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_USSDGW_CHARGING_CDR = BashOperator(
    task_id='PCF_USSDGW_CHARGING_CDR',
    bash_command= pcf_command_USSDGW_CHARGING_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_USSDGW_CHARGING_CDR = BashOperator(
    task_id='Dedup_USSDGW_CHARGING_CDR',
    bash_command= dedup_command_USSDGW_CHARGING_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_USSDGW_CHARGING_CDR = BashOperator(
    task_id='Dedup2_USSDGW_CHARGING_CDR',
    bash_command= dedup_command_USSDGW_CHARGING_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_USSDGW_CHARGING_CDR = BashOperator(
    task_id='PCFCheck_USSDGW_CHARGING_CDR',
    bash_command=PCFCheck_command_USSDGW_CHARGING_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_USSDGW_CHARGING_CDR = EmailOperator(
    task_id='faile_USSDGW_CHARGING_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_USSDGW_CHARGING_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_USSDGW_CHARGING_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_USSDGW_CHARGING_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_USSDGW_CHARGING_CDR >> PCF_USSDGW_CHARGING_CDR >> PCFCheck_USSDGW_CHARGING_CDR >> delay_python_task_USSDGW_CHARGING_CDR >> Dedup_USSDGW_CHARGING_CDR >> Validation_End 
Branching_USSDGW_CHARGING_CDR >> Dedup2_USSDGW_CHARGING_CDR >> Validation_End
Branching_USSDGW_CHARGING_CDR >> faile_USSDGW_CHARGING_CDR
Branching_USSDGW_CHARGING_CDR >> Validation_End


feedname_CS5_SDP_ACC_ADJ_DA = 'CS5_SDP_ACC_ADJ_DA'.upper()
feedname2_CS5_SDP_ACC_ADJ_DA = 'CS5_SDP_ACC_ADJ_DA'.lower()

dedup_command_CS5_SDP_ACC_ADJ_DA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CS5_SDP_ACC_ADJ_DA)
logging.info(dedup_command_CS5_SDP_ACC_ADJ_DA) 

pcf_command_CS5_SDP_ACC_ADJ_DA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CS5_SDP_ACC_ADJ_DA)
logging.info(pcf_command_CS5_SDP_ACC_ADJ_DA)

PCFCheck_command_CS5_SDP_ACC_ADJ_DA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_SDP_ACC_ADJ_DA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_SDP_ACC_ADJ_DA)

branchScript_CS5_SDP_ACC_ADJ_DA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_SDP_ACC_ADJ_DA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_SDP_ACC_ADJ_DA)

def branch_CS5_SDP_ACC_ADJ_DA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_SDP_ACC_ADJ_DA,feedname2_CS5_SDP_ACC_ADJ_DA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_SDP_ACC_ADJ_DA)
    x = readcsv(filepath,feedname2_CS5_SDP_ACC_ADJ_DA)
    logging.info('branch_CS5_SDP_ACC_ADJ_DA' + x)
    return x

Branching_CS5_SDP_ACC_ADJ_DA = BranchPythonOperator(
    task_id='branchid_CS5_SDP_ACC_ADJ_DA',
    python_callable=branch_CS5_SDP_ACC_ADJ_DA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CS5_SDP_ACC_ADJ_DA = BashOperator(
    task_id='PCF_CS5_SDP_ACC_ADJ_DA',
    bash_command= pcf_command_CS5_SDP_ACC_ADJ_DA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CS5_SDP_ACC_ADJ_DA = BashOperator(
    task_id='Dedup_CS5_SDP_ACC_ADJ_DA',
    bash_command= dedup_command_CS5_SDP_ACC_ADJ_DA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CS5_SDP_ACC_ADJ_DA = BashOperator(
    task_id='Dedup2_CS5_SDP_ACC_ADJ_DA',
    bash_command= dedup_command_CS5_SDP_ACC_ADJ_DA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CS5_SDP_ACC_ADJ_DA = BashOperator(
    task_id='PCFCheck_CS5_SDP_ACC_ADJ_DA',
    bash_command=PCFCheck_command_CS5_SDP_ACC_ADJ_DA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CS5_SDP_ACC_ADJ_DA = EmailOperator(
    task_id='faile_CS5_SDP_ACC_ADJ_DA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_SDP_ACC_ADJ_DA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_SDP_ACC_ADJ_DA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_SDP_ACC_ADJ_DA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_SDP_ACC_ADJ_DA >> PCF_CS5_SDP_ACC_ADJ_DA >> PCFCheck_CS5_SDP_ACC_ADJ_DA >> delay_python_task_CS5_SDP_ACC_ADJ_DA >> Dedup_CS5_SDP_ACC_ADJ_DA >> Validation_End 
Branching_CS5_SDP_ACC_ADJ_DA >> Dedup2_CS5_SDP_ACC_ADJ_DA >> Validation_End
Branching_CS5_SDP_ACC_ADJ_DA >> faile_CS5_SDP_ACC_ADJ_DA
Branching_CS5_SDP_ACC_ADJ_DA >> Validation_End


feedname_BILL_RUN_STATISTICS_TAB = 'BILL_RUN_STATISTICS_TAB'.upper()
feedname2_BILL_RUN_STATISTICS_TAB = 'BILL_RUN_STATISTICS_TAB'.lower()

dedup_command_BILL_RUN_STATISTICS_TAB="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_BILL_RUN_STATISTICS_TAB)
logging.info(dedup_command_BILL_RUN_STATISTICS_TAB) 

pcf_command_BILL_RUN_STATISTICS_TAB = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_BILL_RUN_STATISTICS_TAB)
logging.info(pcf_command_BILL_RUN_STATISTICS_TAB)

PCFCheck_command_BILL_RUN_STATISTICS_TAB = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BILL_RUN_STATISTICS_TAB,run_date,sleeptime)
logging.info(PCFCheck_command_BILL_RUN_STATISTICS_TAB)

branchScript_BILL_RUN_STATISTICS_TAB = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BILL_RUN_STATISTICS_TAB,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BILL_RUN_STATISTICS_TAB)

def branch_BILL_RUN_STATISTICS_TAB():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BILL_RUN_STATISTICS_TAB,feedname2_BILL_RUN_STATISTICS_TAB)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BILL_RUN_STATISTICS_TAB)
    x = readcsv(filepath,feedname2_BILL_RUN_STATISTICS_TAB)
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
    priority_weight=1
)

Dedup_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='Dedup_BILL_RUN_STATISTICS_TAB',
    bash_command= dedup_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='Dedup2_BILL_RUN_STATISTICS_TAB',
    bash_command= dedup_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_BILL_RUN_STATISTICS_TAB = BashOperator(
    task_id='PCFCheck_BILL_RUN_STATISTICS_TAB',
    bash_command=PCFCheck_command_BILL_RUN_STATISTICS_TAB,
    dag=dag,
    run_as_user='daasuser',
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

delay_python_task_BILL_RUN_STATISTICS_TAB: PythonOperator = PythonOperator(task_id="waitForFlush_BILL_RUN_STATISTICS_TAB",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BILL_RUN_STATISTICS_TAB >> PCF_BILL_RUN_STATISTICS_TAB >> PCFCheck_BILL_RUN_STATISTICS_TAB >> delay_python_task_BILL_RUN_STATISTICS_TAB >> Dedup_BILL_RUN_STATISTICS_TAB >> Validation_End 
Branching_BILL_RUN_STATISTICS_TAB >> Dedup2_BILL_RUN_STATISTICS_TAB >> Validation_End
Branching_BILL_RUN_STATISTICS_TAB >> faile_BILL_RUN_STATISTICS_TAB
Branching_BILL_RUN_STATISTICS_TAB >> Validation_End


feedname_AUTO_TOPUP = 'AUTO_TOPUP'.upper()
feedname2_AUTO_TOPUP = 'AUTO_TOPUP'.lower()

dedup_command_AUTO_TOPUP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AUTO_TOPUP)
logging.info(dedup_command_AUTO_TOPUP) 

pcf_command_AUTO_TOPUP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AUTO_TOPUP)
logging.info(pcf_command_AUTO_TOPUP)

PCFCheck_command_AUTO_TOPUP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AUTO_TOPUP,run_date,sleeptime)
logging.info(PCFCheck_command_AUTO_TOPUP)

branchScript_AUTO_TOPUP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AUTO_TOPUP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AUTO_TOPUP)

def branch_AUTO_TOPUP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AUTO_TOPUP,feedname2_AUTO_TOPUP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AUTO_TOPUP)
    x = readcsv(filepath,feedname2_AUTO_TOPUP)
    logging.info('branch_AUTO_TOPUP' + x)
    return x

Branching_AUTO_TOPUP = BranchPythonOperator(
    task_id='branchid_AUTO_TOPUP',
    python_callable=branch_AUTO_TOPUP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AUTO_TOPUP = BashOperator(
    task_id='PCF_AUTO_TOPUP',
    bash_command= pcf_command_AUTO_TOPUP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AUTO_TOPUP = BashOperator(
    task_id='Dedup_AUTO_TOPUP',
    bash_command= dedup_command_AUTO_TOPUP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AUTO_TOPUP = BashOperator(
    task_id='Dedup2_AUTO_TOPUP',
    bash_command= dedup_command_AUTO_TOPUP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AUTO_TOPUP = BashOperator(
    task_id='PCFCheck_AUTO_TOPUP',
    bash_command=PCFCheck_command_AUTO_TOPUP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AUTO_TOPUP = EmailOperator(
    task_id='faile_AUTO_TOPUP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AUTO_TOPUP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AUTO_TOPUP: PythonOperator = PythonOperator(task_id="waitForFlush_AUTO_TOPUP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AUTO_TOPUP >> PCF_AUTO_TOPUP >> PCFCheck_AUTO_TOPUP >> delay_python_task_AUTO_TOPUP >> Dedup_AUTO_TOPUP >> Validation_End 
Branching_AUTO_TOPUP >> Dedup2_AUTO_TOPUP >> Validation_End
Branching_AUTO_TOPUP >> faile_AUTO_TOPUP
Branching_AUTO_TOPUP >> Validation_End
