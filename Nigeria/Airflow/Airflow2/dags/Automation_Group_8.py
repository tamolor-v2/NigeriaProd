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
d_1 = Variable.get("rerunDate8", deserialize_json=True)
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

dag = DAG('New_Automation_Group_8', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group8 -c config.json' %(d_1)
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


feedname_TAPIN_VOICE = 'TAPIN_VOICE'.upper()
feedname2_TAPIN_VOICE = 'TAPIN_VOICE'.lower()

dedup_command_TAPIN_VOICE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAPIN_VOICE)
logging.info(dedup_command_TAPIN_VOICE) 

pcf_command_TAPIN_VOICE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAPIN_VOICE)
logging.info(pcf_command_TAPIN_VOICE)

PCFCheck_command_TAPIN_VOICE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAPIN_VOICE,run_date,sleeptime)
logging.info(PCFCheck_command_TAPIN_VOICE)

branchScript_TAPIN_VOICE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAPIN_VOICE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAPIN_VOICE)

def branch_TAPIN_VOICE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAPIN_VOICE,feedname2_TAPIN_VOICE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAPIN_VOICE)
    x = readcsv(filepath,feedname2_TAPIN_VOICE)
    logging.info('branch_TAPIN_VOICE' + x)
    return x

Branching_TAPIN_VOICE = BranchPythonOperator(
    task_id='branchid_TAPIN_VOICE',
    python_callable=branch_TAPIN_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAPIN_VOICE = BashOperator(
    task_id='PCF_TAPIN_VOICE',
    bash_command= pcf_command_TAPIN_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAPIN_VOICE = BashOperator(
    task_id='Dedup_TAPIN_VOICE',
    bash_command= dedup_command_TAPIN_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAPIN_VOICE = BashOperator(
    task_id='Dedup2_TAPIN_VOICE',
    bash_command= dedup_command_TAPIN_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAPIN_VOICE = BashOperator(
    task_id='PCFCheck_TAPIN_VOICE',
    bash_command=PCFCheck_command_TAPIN_VOICE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAPIN_VOICE = EmailOperator(
    task_id='faile_TAPIN_VOICE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAPIN_VOICE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAPIN_VOICE: PythonOperator = PythonOperator(task_id="waitForFlush_TAPIN_VOICE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAPIN_VOICE >> PCF_TAPIN_VOICE >> PCFCheck_TAPIN_VOICE >> delay_python_task_TAPIN_VOICE >> Dedup_TAPIN_VOICE >> Validation_End 
Branching_TAPIN_VOICE >> Dedup2_TAPIN_VOICE >> Validation_End
Branching_TAPIN_VOICE >> faile_TAPIN_VOICE
Branching_TAPIN_VOICE >> Validation_End


feedname_TAPIN_GPRS = 'TAPIN_GPRS'.upper()
feedname2_TAPIN_GPRS = 'TAPIN_GPRS'.lower()

dedup_command_TAPIN_GPRS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAPIN_GPRS)
logging.info(dedup_command_TAPIN_GPRS) 

pcf_command_TAPIN_GPRS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAPIN_GPRS)
logging.info(pcf_command_TAPIN_GPRS)

PCFCheck_command_TAPIN_GPRS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAPIN_GPRS,run_date,sleeptime)
logging.info(PCFCheck_command_TAPIN_GPRS)

branchScript_TAPIN_GPRS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAPIN_GPRS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAPIN_GPRS)

def branch_TAPIN_GPRS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAPIN_GPRS,feedname2_TAPIN_GPRS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAPIN_GPRS)
    x = readcsv(filepath,feedname2_TAPIN_GPRS)
    logging.info('branch_TAPIN_GPRS' + x)
    return x

Branching_TAPIN_GPRS = BranchPythonOperator(
    task_id='branchid_TAPIN_GPRS',
    python_callable=branch_TAPIN_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAPIN_GPRS = BashOperator(
    task_id='PCF_TAPIN_GPRS',
    bash_command= pcf_command_TAPIN_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAPIN_GPRS = BashOperator(
    task_id='Dedup_TAPIN_GPRS',
    bash_command= dedup_command_TAPIN_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAPIN_GPRS = BashOperator(
    task_id='Dedup2_TAPIN_GPRS',
    bash_command= dedup_command_TAPIN_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAPIN_GPRS = BashOperator(
    task_id='PCFCheck_TAPIN_GPRS',
    bash_command=PCFCheck_command_TAPIN_GPRS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAPIN_GPRS = EmailOperator(
    task_id='faile_TAPIN_GPRS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAPIN_GPRS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAPIN_GPRS: PythonOperator = PythonOperator(task_id="waitForFlush_TAPIN_GPRS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAPIN_GPRS >> PCF_TAPIN_GPRS >> PCFCheck_TAPIN_GPRS >> delay_python_task_TAPIN_GPRS >> Dedup_TAPIN_GPRS >> Validation_End 
Branching_TAPIN_GPRS >> Dedup2_TAPIN_GPRS >> Validation_End
Branching_TAPIN_GPRS >> faile_TAPIN_GPRS
Branching_TAPIN_GPRS >> Validation_End


feedname_MAPS2G = 'MAPS2G'.upper()
feedname2_MAPS2G = 'MAPS2G'.lower()

dedup_command_MAPS2G="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MAPS2G)
logging.info(dedup_command_MAPS2G) 

pcf_command_MAPS2G = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MAPS2G)
logging.info(pcf_command_MAPS2G)

PCFCheck_command_MAPS2G = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MAPS2G,run_date,sleeptime)
logging.info(PCFCheck_command_MAPS2G)

branchScript_MAPS2G = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MAPS2G,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MAPS2G)

def branch_MAPS2G():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MAPS2G,feedname2_MAPS2G)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MAPS2G)
    x = readcsv(filepath,feedname2_MAPS2G)
    logging.info('branch_MAPS2G' + x)
    return x

Branching_MAPS2G = BranchPythonOperator(
    task_id='branchid_MAPS2G',
    python_callable=branch_MAPS2G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MAPS2G = BashOperator(
    task_id='PCF_MAPS2G',
    bash_command= pcf_command_MAPS2G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MAPS2G = BashOperator(
    task_id='Dedup_MAPS2G',
    bash_command= dedup_command_MAPS2G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MAPS2G = BashOperator(
    task_id='Dedup2_MAPS2G',
    bash_command= dedup_command_MAPS2G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MAPS2G = BashOperator(
    task_id='PCFCheck_MAPS2G',
    bash_command=PCFCheck_command_MAPS2G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MAPS2G = EmailOperator(
    task_id='faile_MAPS2G',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS2G),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MAPS2G: PythonOperator = PythonOperator(task_id="waitForFlush_MAPS2G",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MAPS2G >> PCF_MAPS2G >> PCFCheck_MAPS2G >> delay_python_task_MAPS2G >> Dedup_MAPS2G >> Validation_End 
Branching_MAPS2G >> Dedup2_MAPS2G >> Validation_End
Branching_MAPS2G >> faile_MAPS2G
Branching_MAPS2G >> Validation_End


feedname_MAPS3G = 'MAPS3G'.upper()
feedname2_MAPS3G = 'MAPS3G'.lower()

dedup_command_MAPS3G="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MAPS3G)
logging.info(dedup_command_MAPS3G) 

pcf_command_MAPS3G = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MAPS3G)
logging.info(pcf_command_MAPS3G)

PCFCheck_command_MAPS3G = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MAPS3G,run_date,sleeptime)
logging.info(PCFCheck_command_MAPS3G)

branchScript_MAPS3G = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MAPS3G,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MAPS3G)

def branch_MAPS3G():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MAPS3G,feedname2_MAPS3G)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MAPS3G)
    x = readcsv(filepath,feedname2_MAPS3G)
    logging.info('branch_MAPS3G' + x)
    return x

Branching_MAPS3G = BranchPythonOperator(
    task_id='branchid_MAPS3G',
    python_callable=branch_MAPS3G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MAPS3G = BashOperator(
    task_id='PCF_MAPS3G',
    bash_command= pcf_command_MAPS3G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MAPS3G = BashOperator(
    task_id='Dedup_MAPS3G',
    bash_command= dedup_command_MAPS3G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MAPS3G = BashOperator(
    task_id='Dedup2_MAPS3G',
    bash_command= dedup_command_MAPS3G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MAPS3G = BashOperator(
    task_id='PCFCheck_MAPS3G',
    bash_command=PCFCheck_command_MAPS3G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MAPS3G = EmailOperator(
    task_id='faile_MAPS3G',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS3G),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MAPS3G: PythonOperator = PythonOperator(task_id="waitForFlush_MAPS3G",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MAPS3G >> PCF_MAPS3G >> PCFCheck_MAPS3G >> delay_python_task_MAPS3G >> Dedup_MAPS3G >> Validation_End 
Branching_MAPS3G >> Dedup2_MAPS3G >> Validation_End
Branching_MAPS3G >> faile_MAPS3G
Branching_MAPS3G >> Validation_End


feedname_MAPS4G = 'MAPS4G'.upper()
feedname2_MAPS4G = 'MAPS4G'.lower()

dedup_command_MAPS4G="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MAPS4G)
logging.info(dedup_command_MAPS4G) 

pcf_command_MAPS4G = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MAPS4G)
logging.info(pcf_command_MAPS4G)

PCFCheck_command_MAPS4G = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MAPS4G,run_date,sleeptime)
logging.info(PCFCheck_command_MAPS4G)

branchScript_MAPS4G = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MAPS4G,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MAPS4G)

def branch_MAPS4G():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MAPS4G,feedname2_MAPS4G)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MAPS4G)
    x = readcsv(filepath,feedname2_MAPS4G)
    logging.info('branch_MAPS4G' + x)
    return x

Branching_MAPS4G = BranchPythonOperator(
    task_id='branchid_MAPS4G',
    python_callable=branch_MAPS4G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MAPS4G = BashOperator(
    task_id='PCF_MAPS4G',
    bash_command= pcf_command_MAPS4G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MAPS4G = BashOperator(
    task_id='Dedup_MAPS4G',
    bash_command= dedup_command_MAPS4G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MAPS4G = BashOperator(
    task_id='Dedup2_MAPS4G',
    bash_command= dedup_command_MAPS4G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MAPS4G = BashOperator(
    task_id='PCFCheck_MAPS4G',
    bash_command=PCFCheck_command_MAPS4G,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MAPS4G = EmailOperator(
    task_id='faile_MAPS4G',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS4G),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MAPS4G: PythonOperator = PythonOperator(task_id="waitForFlush_MAPS4G",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MAPS4G >> PCF_MAPS4G >> PCFCheck_MAPS4G >> delay_python_task_MAPS4G >> Dedup_MAPS4G >> Validation_End 
Branching_MAPS4G >> Dedup2_MAPS4G >> Validation_End
Branching_MAPS4G >> faile_MAPS4G
Branching_MAPS4G >> Validation_End


feedname_MAPS_4G_BOARD_D = 'MAPS_4G_BOARD_D'.upper()
feedname2_MAPS_4G_BOARD_D = 'MAPS_4G_BOARD_D'.lower()

dedup_command_MAPS_4G_BOARD_D="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MAPS_4G_BOARD_D)
logging.info(dedup_command_MAPS_4G_BOARD_D) 

pcf_command_MAPS_4G_BOARD_D = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MAPS_4G_BOARD_D)
logging.info(pcf_command_MAPS_4G_BOARD_D)

PCFCheck_command_MAPS_4G_BOARD_D = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MAPS_4G_BOARD_D,run_date,sleeptime)
logging.info(PCFCheck_command_MAPS_4G_BOARD_D)

branchScript_MAPS_4G_BOARD_D = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MAPS_4G_BOARD_D,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MAPS_4G_BOARD_D)

def branch_MAPS_4G_BOARD_D():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MAPS_4G_BOARD_D,feedname2_MAPS_4G_BOARD_D)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MAPS_4G_BOARD_D)
    x = readcsv(filepath,feedname2_MAPS_4G_BOARD_D)
    logging.info('branch_MAPS_4G_BOARD_D' + x)
    return x

Branching_MAPS_4G_BOARD_D = BranchPythonOperator(
    task_id='branchid_MAPS_4G_BOARD_D',
    python_callable=branch_MAPS_4G_BOARD_D,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MAPS_4G_BOARD_D = BashOperator(
    task_id='PCF_MAPS_4G_BOARD_D',
    bash_command= pcf_command_MAPS_4G_BOARD_D,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MAPS_4G_BOARD_D = BashOperator(
    task_id='Dedup_MAPS_4G_BOARD_D',
    bash_command= dedup_command_MAPS_4G_BOARD_D,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MAPS_4G_BOARD_D = BashOperator(
    task_id='Dedup2_MAPS_4G_BOARD_D',
    bash_command= dedup_command_MAPS_4G_BOARD_D,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MAPS_4G_BOARD_D = BashOperator(
    task_id='PCFCheck_MAPS_4G_BOARD_D',
    bash_command=PCFCheck_command_MAPS_4G_BOARD_D,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MAPS_4G_BOARD_D = EmailOperator(
    task_id='faile_MAPS_4G_BOARD_D',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS_4G_BOARD_D),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MAPS_4G_BOARD_D: PythonOperator = PythonOperator(task_id="waitForFlush_MAPS_4G_BOARD_D",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MAPS_4G_BOARD_D >> PCF_MAPS_4G_BOARD_D >> PCFCheck_MAPS_4G_BOARD_D >> delay_python_task_MAPS_4G_BOARD_D >> Dedup_MAPS_4G_BOARD_D >> Validation_End 
Branching_MAPS_4G_BOARD_D >> Dedup2_MAPS_4G_BOARD_D >> Validation_End
Branching_MAPS_4G_BOARD_D >> faile_MAPS_4G_BOARD_D
Branching_MAPS_4G_BOARD_D >> Validation_End


feedname_MAPS_2G_CN_D_BH = 'MAPS_2G_CN_D_BH'.upper()
feedname2_MAPS_2G_CN_D_BH = 'MAPS_2G_CN_D_BH'.lower()

dedup_command_MAPS_2G_CN_D_BH="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MAPS_2G_CN_D_BH)
logging.info(dedup_command_MAPS_2G_CN_D_BH) 

pcf_command_MAPS_2G_CN_D_BH = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MAPS_2G_CN_D_BH)
logging.info(pcf_command_MAPS_2G_CN_D_BH)

PCFCheck_command_MAPS_2G_CN_D_BH = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MAPS_2G_CN_D_BH,run_date,sleeptime)
logging.info(PCFCheck_command_MAPS_2G_CN_D_BH)

branchScript_MAPS_2G_CN_D_BH = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MAPS_2G_CN_D_BH,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MAPS_2G_CN_D_BH)

def branch_MAPS_2G_CN_D_BH():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MAPS_2G_CN_D_BH,feedname2_MAPS_2G_CN_D_BH)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MAPS_2G_CN_D_BH)
    x = readcsv(filepath,feedname2_MAPS_2G_CN_D_BH)
    logging.info('branch_MAPS_2G_CN_D_BH' + x)
    return x

Branching_MAPS_2G_CN_D_BH = BranchPythonOperator(
    task_id='branchid_MAPS_2G_CN_D_BH',
    python_callable=branch_MAPS_2G_CN_D_BH,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MAPS_2G_CN_D_BH = BashOperator(
    task_id='PCF_MAPS_2G_CN_D_BH',
    bash_command= pcf_command_MAPS_2G_CN_D_BH,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MAPS_2G_CN_D_BH = BashOperator(
    task_id='Dedup_MAPS_2G_CN_D_BH',
    bash_command= dedup_command_MAPS_2G_CN_D_BH,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MAPS_2G_CN_D_BH = BashOperator(
    task_id='Dedup2_MAPS_2G_CN_D_BH',
    bash_command= dedup_command_MAPS_2G_CN_D_BH,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MAPS_2G_CN_D_BH = BashOperator(
    task_id='PCFCheck_MAPS_2G_CN_D_BH',
    bash_command=PCFCheck_command_MAPS_2G_CN_D_BH,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MAPS_2G_CN_D_BH = EmailOperator(
    task_id='faile_MAPS_2G_CN_D_BH',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS_2G_CN_D_BH),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MAPS_2G_CN_D_BH: PythonOperator = PythonOperator(task_id="waitForFlush_MAPS_2G_CN_D_BH",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MAPS_2G_CN_D_BH >> PCF_MAPS_2G_CN_D_BH >> PCFCheck_MAPS_2G_CN_D_BH >> delay_python_task_MAPS_2G_CN_D_BH >> Dedup_MAPS_2G_CN_D_BH >> Validation_End 
Branching_MAPS_2G_CN_D_BH >> Dedup2_MAPS_2G_CN_D_BH >> Validation_End
Branching_MAPS_2G_CN_D_BH >> faile_MAPS_2G_CN_D_BH
Branching_MAPS_2G_CN_D_BH >> Validation_End


feedname_TAS_DSR_ADHERENCE = 'TAS_DSR_ADHERENCE'.upper()
feedname2_TAS_DSR_ADHERENCE = 'TAS_DSR_ADHERENCE'.lower()

dedup_command_TAS_DSR_ADHERENCE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_DSR_ADHERENCE)
logging.info(dedup_command_TAS_DSR_ADHERENCE) 

pcf_command_TAS_DSR_ADHERENCE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_DSR_ADHERENCE)
logging.info(pcf_command_TAS_DSR_ADHERENCE)

PCFCheck_command_TAS_DSR_ADHERENCE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_DSR_ADHERENCE,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_DSR_ADHERENCE)

branchScript_TAS_DSR_ADHERENCE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_DSR_ADHERENCE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_DSR_ADHERENCE)

def branch_TAS_DSR_ADHERENCE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_DSR_ADHERENCE,feedname2_TAS_DSR_ADHERENCE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_DSR_ADHERENCE)
    x = readcsv(filepath,feedname2_TAS_DSR_ADHERENCE)
    logging.info('branch_TAS_DSR_ADHERENCE' + x)
    return x

Branching_TAS_DSR_ADHERENCE = BranchPythonOperator(
    task_id='branchid_TAS_DSR_ADHERENCE',
    python_callable=branch_TAS_DSR_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_DSR_ADHERENCE = BashOperator(
    task_id='PCF_TAS_DSR_ADHERENCE',
    bash_command= pcf_command_TAS_DSR_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_DSR_ADHERENCE = BashOperator(
    task_id='Dedup_TAS_DSR_ADHERENCE',
    bash_command= dedup_command_TAS_DSR_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_DSR_ADHERENCE = BashOperator(
    task_id='Dedup2_TAS_DSR_ADHERENCE',
    bash_command= dedup_command_TAS_DSR_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_DSR_ADHERENCE = BashOperator(
    task_id='PCFCheck_TAS_DSR_ADHERENCE',
    bash_command=PCFCheck_command_TAS_DSR_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_DSR_ADHERENCE = EmailOperator(
    task_id='faile_TAS_DSR_ADHERENCE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_DSR_ADHERENCE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_DSR_ADHERENCE: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_DSR_ADHERENCE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_DSR_ADHERENCE >> PCF_TAS_DSR_ADHERENCE >> PCFCheck_TAS_DSR_ADHERENCE >> delay_python_task_TAS_DSR_ADHERENCE >> Dedup_TAS_DSR_ADHERENCE >> Validation_End 
Branching_TAS_DSR_ADHERENCE >> Dedup2_TAS_DSR_ADHERENCE >> Validation_End
Branching_TAS_DSR_ADHERENCE >> faile_TAS_DSR_ADHERENCE
Branching_TAS_DSR_ADHERENCE >> Validation_End


feedname_TAS_KPI_TARGET_VS_ACHIEVEMENT = 'TAS_KPI_TARGET_VS_ACHIEVEMENT'.upper()
feedname2_TAS_KPI_TARGET_VS_ACHIEVEMENT = 'TAS_KPI_TARGET_VS_ACHIEVEMENT'.lower()

dedup_command_TAS_KPI_TARGET_VS_ACHIEVEMENT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_KPI_TARGET_VS_ACHIEVEMENT)
logging.info(dedup_command_TAS_KPI_TARGET_VS_ACHIEVEMENT) 

pcf_command_TAS_KPI_TARGET_VS_ACHIEVEMENT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_KPI_TARGET_VS_ACHIEVEMENT)
logging.info(pcf_command_TAS_KPI_TARGET_VS_ACHIEVEMENT)

PCFCheck_command_TAS_KPI_TARGET_VS_ACHIEVEMENT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_KPI_TARGET_VS_ACHIEVEMENT,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_KPI_TARGET_VS_ACHIEVEMENT)

branchScript_TAS_KPI_TARGET_VS_ACHIEVEMENT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_KPI_TARGET_VS_ACHIEVEMENT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_KPI_TARGET_VS_ACHIEVEMENT)

def branch_TAS_KPI_TARGET_VS_ACHIEVEMENT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_KPI_TARGET_VS_ACHIEVEMENT,feedname2_TAS_KPI_TARGET_VS_ACHIEVEMENT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_KPI_TARGET_VS_ACHIEVEMENT)
    x = readcsv(filepath,feedname2_TAS_KPI_TARGET_VS_ACHIEVEMENT)
    logging.info('branch_TAS_KPI_TARGET_VS_ACHIEVEMENT' + x)
    return x

Branching_TAS_KPI_TARGET_VS_ACHIEVEMENT = BranchPythonOperator(
    task_id='branchid_TAS_KPI_TARGET_VS_ACHIEVEMENT',
    python_callable=branch_TAS_KPI_TARGET_VS_ACHIEVEMENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_KPI_TARGET_VS_ACHIEVEMENT = BashOperator(
    task_id='PCF_TAS_KPI_TARGET_VS_ACHIEVEMENT',
    bash_command= pcf_command_TAS_KPI_TARGET_VS_ACHIEVEMENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_KPI_TARGET_VS_ACHIEVEMENT = BashOperator(
    task_id='Dedup_TAS_KPI_TARGET_VS_ACHIEVEMENT',
    bash_command= dedup_command_TAS_KPI_TARGET_VS_ACHIEVEMENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_KPI_TARGET_VS_ACHIEVEMENT = BashOperator(
    task_id='Dedup2_TAS_KPI_TARGET_VS_ACHIEVEMENT',
    bash_command= dedup_command_TAS_KPI_TARGET_VS_ACHIEVEMENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_KPI_TARGET_VS_ACHIEVEMENT = BashOperator(
    task_id='PCFCheck_TAS_KPI_TARGET_VS_ACHIEVEMENT',
    bash_command=PCFCheck_command_TAS_KPI_TARGET_VS_ACHIEVEMENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_KPI_TARGET_VS_ACHIEVEMENT = EmailOperator(
    task_id='faile_TAS_KPI_TARGET_VS_ACHIEVEMENT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_KPI_TARGET_VS_ACHIEVEMENT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_KPI_TARGET_VS_ACHIEVEMENT: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_KPI_TARGET_VS_ACHIEVEMENT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_KPI_TARGET_VS_ACHIEVEMENT >> PCF_TAS_KPI_TARGET_VS_ACHIEVEMENT >> PCFCheck_TAS_KPI_TARGET_VS_ACHIEVEMENT >> delay_python_task_TAS_KPI_TARGET_VS_ACHIEVEMENT >> Dedup_TAS_KPI_TARGET_VS_ACHIEVEMENT >> Validation_End 
Branching_TAS_KPI_TARGET_VS_ACHIEVEMENT >> Dedup2_TAS_KPI_TARGET_VS_ACHIEVEMENT >> Validation_End
Branching_TAS_KPI_TARGET_VS_ACHIEVEMENT >> faile_TAS_KPI_TARGET_VS_ACHIEVEMENT
Branching_TAS_KPI_TARGET_VS_ACHIEVEMENT >> Validation_End


feedname_TAS_PJP_ADHERENCE = 'TAS_PJP_ADHERENCE'.upper()
feedname2_TAS_PJP_ADHERENCE = 'TAS_PJP_ADHERENCE'.lower()

dedup_command_TAS_PJP_ADHERENCE="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_PJP_ADHERENCE)
logging.info(dedup_command_TAS_PJP_ADHERENCE) 

pcf_command_TAS_PJP_ADHERENCE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_PJP_ADHERENCE)
logging.info(pcf_command_TAS_PJP_ADHERENCE)

PCFCheck_command_TAS_PJP_ADHERENCE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_PJP_ADHERENCE,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_PJP_ADHERENCE)

branchScript_TAS_PJP_ADHERENCE = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_PJP_ADHERENCE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_PJP_ADHERENCE)

def branch_TAS_PJP_ADHERENCE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_PJP_ADHERENCE,feedname2_TAS_PJP_ADHERENCE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_PJP_ADHERENCE)
    x = readcsv(filepath,feedname2_TAS_PJP_ADHERENCE)
    logging.info('branch_TAS_PJP_ADHERENCE' + x)
    return x

Branching_TAS_PJP_ADHERENCE = BranchPythonOperator(
    task_id='branchid_TAS_PJP_ADHERENCE',
    python_callable=branch_TAS_PJP_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_PJP_ADHERENCE = BashOperator(
    task_id='PCF_TAS_PJP_ADHERENCE',
    bash_command= pcf_command_TAS_PJP_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_PJP_ADHERENCE = BashOperator(
    task_id='Dedup_TAS_PJP_ADHERENCE',
    bash_command= dedup_command_TAS_PJP_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_PJP_ADHERENCE = BashOperator(
    task_id='Dedup2_TAS_PJP_ADHERENCE',
    bash_command= dedup_command_TAS_PJP_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_PJP_ADHERENCE = BashOperator(
    task_id='PCFCheck_TAS_PJP_ADHERENCE',
    bash_command=PCFCheck_command_TAS_PJP_ADHERENCE,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_PJP_ADHERENCE = EmailOperator(
    task_id='faile_TAS_PJP_ADHERENCE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_PJP_ADHERENCE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_PJP_ADHERENCE: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_PJP_ADHERENCE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_PJP_ADHERENCE >> PCF_TAS_PJP_ADHERENCE >> PCFCheck_TAS_PJP_ADHERENCE >> delay_python_task_TAS_PJP_ADHERENCE >> Dedup_TAS_PJP_ADHERENCE >> Validation_End 
Branching_TAS_PJP_ADHERENCE >> Dedup2_TAS_PJP_ADHERENCE >> Validation_End
Branching_TAS_PJP_ADHERENCE >> faile_TAS_PJP_ADHERENCE
Branching_TAS_PJP_ADHERENCE >> Validation_End


feedname_TAS_DISTRIBUTOR_MASTER = 'TAS_DISTRIBUTOR_MASTER'.upper()
feedname2_TAS_DISTRIBUTOR_MASTER = 'TAS_DISTRIBUTOR_MASTER'.lower()

dedup_command_TAS_DISTRIBUTOR_MASTER="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TAS_DISTRIBUTOR_MASTER)
logging.info(dedup_command_TAS_DISTRIBUTOR_MASTER) 

pcf_command_TAS_DISTRIBUTOR_MASTER = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TAS_DISTRIBUTOR_MASTER)
logging.info(pcf_command_TAS_DISTRIBUTOR_MASTER)

PCFCheck_command_TAS_DISTRIBUTOR_MASTER = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TAS_DISTRIBUTOR_MASTER,run_date,sleeptime)
logging.info(PCFCheck_command_TAS_DISTRIBUTOR_MASTER)

branchScript_TAS_DISTRIBUTOR_MASTER = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TAS_DISTRIBUTOR_MASTER,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TAS_DISTRIBUTOR_MASTER)

def branch_TAS_DISTRIBUTOR_MASTER():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TAS_DISTRIBUTOR_MASTER,feedname2_TAS_DISTRIBUTOR_MASTER)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TAS_DISTRIBUTOR_MASTER)
    x = readcsv(filepath,feedname2_TAS_DISTRIBUTOR_MASTER)
    logging.info('branch_TAS_DISTRIBUTOR_MASTER' + x)
    return x

Branching_TAS_DISTRIBUTOR_MASTER = BranchPythonOperator(
    task_id='branchid_TAS_DISTRIBUTOR_MASTER',
    python_callable=branch_TAS_DISTRIBUTOR_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TAS_DISTRIBUTOR_MASTER = BashOperator(
    task_id='PCF_TAS_DISTRIBUTOR_MASTER',
    bash_command= pcf_command_TAS_DISTRIBUTOR_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TAS_DISTRIBUTOR_MASTER = BashOperator(
    task_id='Dedup_TAS_DISTRIBUTOR_MASTER',
    bash_command= dedup_command_TAS_DISTRIBUTOR_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TAS_DISTRIBUTOR_MASTER = BashOperator(
    task_id='Dedup2_TAS_DISTRIBUTOR_MASTER',
    bash_command= dedup_command_TAS_DISTRIBUTOR_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TAS_DISTRIBUTOR_MASTER = BashOperator(
    task_id='PCFCheck_TAS_DISTRIBUTOR_MASTER',
    bash_command=PCFCheck_command_TAS_DISTRIBUTOR_MASTER,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TAS_DISTRIBUTOR_MASTER = EmailOperator(
    task_id='faile_TAS_DISTRIBUTOR_MASTER',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TAS_DISTRIBUTOR_MASTER),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TAS_DISTRIBUTOR_MASTER: PythonOperator = PythonOperator(task_id="waitForFlush_TAS_DISTRIBUTOR_MASTER",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TAS_DISTRIBUTOR_MASTER >> PCF_TAS_DISTRIBUTOR_MASTER >> PCFCheck_TAS_DISTRIBUTOR_MASTER >> delay_python_task_TAS_DISTRIBUTOR_MASTER >> Dedup_TAS_DISTRIBUTOR_MASTER >> Validation_End 
Branching_TAS_DISTRIBUTOR_MASTER >> Dedup2_TAS_DISTRIBUTOR_MASTER >> Validation_End
Branching_TAS_DISTRIBUTOR_MASTER >> faile_TAS_DISTRIBUTOR_MASTER
Branching_TAS_DISTRIBUTOR_MASTER >> Validation_End


feedname_DND_CDR = 'DND_CDR'.upper()
feedname2_DND_CDR = 'DND_CDR'.lower()

dedup_command_DND_CDR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_DND_CDR)
logging.info(dedup_command_DND_CDR) 

pcf_command_DND_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_DND_CDR)
logging.info(pcf_command_DND_CDR)

PCFCheck_command_DND_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_DND_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_DND_CDR)

branchScript_DND_CDR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_DND_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_DND_CDR)

def branch_DND_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_DND_CDR,feedname2_DND_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_DND_CDR)
    x = readcsv(filepath,feedname2_DND_CDR)
    logging.info('branch_DND_CDR' + x)
    return x

Branching_DND_CDR = BranchPythonOperator(
    task_id='branchid_DND_CDR',
    python_callable=branch_DND_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_DND_CDR = BashOperator(
    task_id='PCF_DND_CDR',
    bash_command= pcf_command_DND_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_DND_CDR = BashOperator(
    task_id='Dedup_DND_CDR',
    bash_command= dedup_command_DND_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_DND_CDR = BashOperator(
    task_id='Dedup2_DND_CDR',
    bash_command= dedup_command_DND_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_DND_CDR = BashOperator(
    task_id='PCFCheck_DND_CDR',
    bash_command=PCFCheck_command_DND_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_DND_CDR = EmailOperator(
    task_id='faile_DND_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_DND_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_DND_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_DND_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_DND_CDR >> PCF_DND_CDR >> PCFCheck_DND_CDR >> delay_python_task_DND_CDR >> Dedup_DND_CDR >> Validation_End 
Branching_DND_CDR >> Dedup2_DND_CDR >> Validation_End
Branching_DND_CDR >> faile_DND_CDR
Branching_DND_CDR >> Validation_End


feedname_ECW_TRANSACTION = 'ECW_TRANSACTION'.upper()
feedname2_ECW_TRANSACTION = 'ECW_TRANSACTION'.lower()

dedup_command_ECW_TRANSACTION="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ECW_TRANSACTION)
logging.info(dedup_command_ECW_TRANSACTION) 

pcf_command_ECW_TRANSACTION = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ECW_TRANSACTION)
logging.info(pcf_command_ECW_TRANSACTION)

PCFCheck_command_ECW_TRANSACTION = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ECW_TRANSACTION,run_date,sleeptime)
logging.info(PCFCheck_command_ECW_TRANSACTION)

branchScript_ECW_TRANSACTION = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ECW_TRANSACTION,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ECW_TRANSACTION)

def branch_ECW_TRANSACTION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ECW_TRANSACTION,feedname2_ECW_TRANSACTION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ECW_TRANSACTION)
    x = readcsv(filepath,feedname2_ECW_TRANSACTION)
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
    priority_weight=1
)

Dedup_ECW_TRANSACTION = BashOperator(
    task_id='Dedup_ECW_TRANSACTION',
    bash_command= dedup_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ECW_TRANSACTION = BashOperator(
    task_id='Dedup2_ECW_TRANSACTION',
    bash_command= dedup_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ECW_TRANSACTION = BashOperator(
    task_id='PCFCheck_ECW_TRANSACTION',
    bash_command=PCFCheck_command_ECW_TRANSACTION,
    dag=dag,
    run_as_user='daasuser',
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

delay_python_task_ECW_TRANSACTION: PythonOperator = PythonOperator(task_id="waitForFlush_ECW_TRANSACTION",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ECW_TRANSACTION >> PCF_ECW_TRANSACTION >> PCFCheck_ECW_TRANSACTION >> delay_python_task_ECW_TRANSACTION >> Dedup_ECW_TRANSACTION >> Validation_End 
Branching_ECW_TRANSACTION >> Dedup2_ECW_TRANSACTION >> Validation_End
Branching_ECW_TRANSACTION >> faile_ECW_TRANSACTION
Branching_ECW_TRANSACTION >> Validation_End


feedname_PORT_IN_OUT = 'PORT_IN_OUT'.upper()
feedname2_PORT_IN_OUT = 'PORT_IN_OUT'.lower()

dedup_command_PORT_IN_OUT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_PORT_IN_OUT)
logging.info(dedup_command_PORT_IN_OUT) 

pcf_command_PORT_IN_OUT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_PORT_IN_OUT)
logging.info(pcf_command_PORT_IN_OUT)

PCFCheck_command_PORT_IN_OUT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_PORT_IN_OUT,run_date,sleeptime)
logging.info(PCFCheck_command_PORT_IN_OUT)

branchScript_PORT_IN_OUT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_PORT_IN_OUT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_PORT_IN_OUT)

def branch_PORT_IN_OUT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_PORT_IN_OUT,feedname2_PORT_IN_OUT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_PORT_IN_OUT)
    x = readcsv(filepath,feedname2_PORT_IN_OUT)
    logging.info('branch_PORT_IN_OUT' + x)
    return x

Branching_PORT_IN_OUT = BranchPythonOperator(
    task_id='branchid_PORT_IN_OUT',
    python_callable=branch_PORT_IN_OUT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_PORT_IN_OUT = BashOperator(
    task_id='PCF_PORT_IN_OUT',
    bash_command= pcf_command_PORT_IN_OUT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_PORT_IN_OUT = BashOperator(
    task_id='Dedup_PORT_IN_OUT',
    bash_command= dedup_command_PORT_IN_OUT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_PORT_IN_OUT = BashOperator(
    task_id='Dedup2_PORT_IN_OUT',
    bash_command= dedup_command_PORT_IN_OUT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_PORT_IN_OUT = BashOperator(
    task_id='PCFCheck_PORT_IN_OUT',
    bash_command=PCFCheck_command_PORT_IN_OUT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_PORT_IN_OUT = EmailOperator(
    task_id='faile_PORT_IN_OUT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_PORT_IN_OUT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_PORT_IN_OUT: PythonOperator = PythonOperator(task_id="waitForFlush_PORT_IN_OUT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_PORT_IN_OUT >> PCF_PORT_IN_OUT >> PCFCheck_PORT_IN_OUT >> delay_python_task_PORT_IN_OUT >> Dedup_PORT_IN_OUT >> Validation_End 
Branching_PORT_IN_OUT >> Dedup2_PORT_IN_OUT >> Validation_End
Branching_PORT_IN_OUT >> faile_PORT_IN_OUT
Branching_PORT_IN_OUT >> Validation_End


feedname_FLYTXT_LATCH_DUMP = 'FLYTXT_LATCH_DUMP'.upper()
feedname2_FLYTXT_LATCH_DUMP = 'FLYTXT_LATCH_DUMP'.lower()

dedup_command_FLYTXT_LATCH_DUMP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_FLYTXT_LATCH_DUMP)
logging.info(dedup_command_FLYTXT_LATCH_DUMP) 

pcf_command_FLYTXT_LATCH_DUMP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_FLYTXT_LATCH_DUMP)
logging.info(pcf_command_FLYTXT_LATCH_DUMP)

PCFCheck_command_FLYTXT_LATCH_DUMP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_FLYTXT_LATCH_DUMP,run_date,sleeptime)
logging.info(PCFCheck_command_FLYTXT_LATCH_DUMP)

branchScript_FLYTXT_LATCH_DUMP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_FLYTXT_LATCH_DUMP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_FLYTXT_LATCH_DUMP)

def branch_FLYTXT_LATCH_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_FLYTXT_LATCH_DUMP,feedname2_FLYTXT_LATCH_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_FLYTXT_LATCH_DUMP)
    x = readcsv(filepath,feedname2_FLYTXT_LATCH_DUMP)
    logging.info('branch_FLYTXT_LATCH_DUMP' + x)
    return x

Branching_FLYTXT_LATCH_DUMP = BranchPythonOperator(
    task_id='branchid_FLYTXT_LATCH_DUMP',
    python_callable=branch_FLYTXT_LATCH_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='PCF_FLYTXT_LATCH_DUMP',
    bash_command= pcf_command_FLYTXT_LATCH_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='Dedup_FLYTXT_LATCH_DUMP',
    bash_command= dedup_command_FLYTXT_LATCH_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='Dedup2_FLYTXT_LATCH_DUMP',
    bash_command= dedup_command_FLYTXT_LATCH_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='PCFCheck_FLYTXT_LATCH_DUMP',
    bash_command=PCFCheck_command_FLYTXT_LATCH_DUMP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_FLYTXT_LATCH_DUMP = EmailOperator(
    task_id='faile_FLYTXT_LATCH_DUMP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_FLYTXT_LATCH_DUMP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_FLYTXT_LATCH_DUMP: PythonOperator = PythonOperator(task_id="waitForFlush_FLYTXT_LATCH_DUMP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_FLYTXT_LATCH_DUMP >> PCF_FLYTXT_LATCH_DUMP >> PCFCheck_FLYTXT_LATCH_DUMP >> delay_python_task_FLYTXT_LATCH_DUMP >> Dedup_FLYTXT_LATCH_DUMP >> Validation_End 
Branching_FLYTXT_LATCH_DUMP >> Dedup2_FLYTXT_LATCH_DUMP >> Validation_End
Branching_FLYTXT_LATCH_DUMP >> faile_FLYTXT_LATCH_DUMP
Branching_FLYTXT_LATCH_DUMP >> Validation_End


feedname_RECON = 'RECON'.upper()
feedname2_RECON = 'RECON'.lower()

dedup_command_RECON="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RECON)
logging.info(dedup_command_RECON) 

pcf_command_RECON = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RECON)
logging.info(pcf_command_RECON)

PCFCheck_command_RECON = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RECON,run_date,sleeptime)
logging.info(PCFCheck_command_RECON)

branchScript_RECON = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RECON,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RECON)

def branch_RECON():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RECON,feedname2_RECON)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RECON)
    x = readcsv(filepath,feedname2_RECON)
    logging.info('branch_RECON' + x)
    return x

Branching_RECON = BranchPythonOperator(
    task_id='branchid_RECON',
    python_callable=branch_RECON,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RECON = BashOperator(
    task_id='PCF_RECON',
    bash_command= pcf_command_RECON,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RECON = BashOperator(
    task_id='Dedup_RECON',
    bash_command= dedup_command_RECON,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RECON = BashOperator(
    task_id='Dedup2_RECON',
    bash_command= dedup_command_RECON,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RECON = BashOperator(
    task_id='PCFCheck_RECON',
    bash_command=PCFCheck_command_RECON,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RECON = EmailOperator(
    task_id='faile_RECON',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RECON),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RECON: PythonOperator = PythonOperator(task_id="waitForFlush_RECON",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RECON >> PCF_RECON >> PCFCheck_RECON >> delay_python_task_RECON >> Dedup_RECON >> Validation_End 
Branching_RECON >> Dedup2_RECON >> Validation_End
Branching_RECON >> faile_RECON
Branching_RECON >> Validation_End


feedname_FLYTXT_INBOUND_EVENTS_DATA = 'FLYTXT_INBOUND_EVENTS_DATA'.upper()
feedname2_FLYTXT_INBOUND_EVENTS_DATA = 'FLYTXT_INBOUND_EVENTS_DATA'.lower()

dedup_command_FLYTXT_INBOUND_EVENTS_DATA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_FLYTXT_INBOUND_EVENTS_DATA)
logging.info(dedup_command_FLYTXT_INBOUND_EVENTS_DATA) 

pcf_command_FLYTXT_INBOUND_EVENTS_DATA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_FLYTXT_INBOUND_EVENTS_DATA)
logging.info(pcf_command_FLYTXT_INBOUND_EVENTS_DATA)

PCFCheck_command_FLYTXT_INBOUND_EVENTS_DATA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_FLYTXT_INBOUND_EVENTS_DATA,run_date,sleeptime)
logging.info(PCFCheck_command_FLYTXT_INBOUND_EVENTS_DATA)

branchScript_FLYTXT_INBOUND_EVENTS_DATA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_FLYTXT_INBOUND_EVENTS_DATA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_FLYTXT_INBOUND_EVENTS_DATA)

def branch_FLYTXT_INBOUND_EVENTS_DATA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_FLYTXT_INBOUND_EVENTS_DATA,feedname2_FLYTXT_INBOUND_EVENTS_DATA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_FLYTXT_INBOUND_EVENTS_DATA)
    x = readcsv(filepath,feedname2_FLYTXT_INBOUND_EVENTS_DATA)
    logging.info('branch_FLYTXT_INBOUND_EVENTS_DATA' + x)
    return x

Branching_FLYTXT_INBOUND_EVENTS_DATA = BranchPythonOperator(
    task_id='branchid_FLYTXT_INBOUND_EVENTS_DATA',
    python_callable=branch_FLYTXT_INBOUND_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_FLYTXT_INBOUND_EVENTS_DATA = BashOperator(
    task_id='PCF_FLYTXT_INBOUND_EVENTS_DATA',
    bash_command= pcf_command_FLYTXT_INBOUND_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_FLYTXT_INBOUND_EVENTS_DATA = BashOperator(
    task_id='Dedup_FLYTXT_INBOUND_EVENTS_DATA',
    bash_command= dedup_command_FLYTXT_INBOUND_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_FLYTXT_INBOUND_EVENTS_DATA = BashOperator(
    task_id='Dedup2_FLYTXT_INBOUND_EVENTS_DATA',
    bash_command= dedup_command_FLYTXT_INBOUND_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_FLYTXT_INBOUND_EVENTS_DATA = BashOperator(
    task_id='PCFCheck_FLYTXT_INBOUND_EVENTS_DATA',
    bash_command=PCFCheck_command_FLYTXT_INBOUND_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_FLYTXT_INBOUND_EVENTS_DATA = EmailOperator(
    task_id='faile_FLYTXT_INBOUND_EVENTS_DATA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_FLYTXT_INBOUND_EVENTS_DATA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_FLYTXT_INBOUND_EVENTS_DATA: PythonOperator = PythonOperator(task_id="waitForFlush_FLYTXT_INBOUND_EVENTS_DATA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_FLYTXT_INBOUND_EVENTS_DATA >> PCF_FLYTXT_INBOUND_EVENTS_DATA >> PCFCheck_FLYTXT_INBOUND_EVENTS_DATA >> delay_python_task_FLYTXT_INBOUND_EVENTS_DATA >> Dedup_FLYTXT_INBOUND_EVENTS_DATA >> Validation_End 
Branching_FLYTXT_INBOUND_EVENTS_DATA >> Dedup2_FLYTXT_INBOUND_EVENTS_DATA >> Validation_End
Branching_FLYTXT_INBOUND_EVENTS_DATA >> faile_FLYTXT_INBOUND_EVENTS_DATA
Branching_FLYTXT_INBOUND_EVENTS_DATA >> Validation_End


feedname_APLIMAN_OBD = 'APLIMAN_OBD'.upper()
feedname2_APLIMAN_OBD = 'APLIMAN_OBD'.lower()

dedup_command_APLIMAN_OBD="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_APLIMAN_OBD)
logging.info(dedup_command_APLIMAN_OBD) 

pcf_command_APLIMAN_OBD = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_APLIMAN_OBD)
logging.info(pcf_command_APLIMAN_OBD)

PCFCheck_command_APLIMAN_OBD = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_APLIMAN_OBD,run_date,sleeptime)
logging.info(PCFCheck_command_APLIMAN_OBD)

branchScript_APLIMAN_OBD = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_APLIMAN_OBD,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_APLIMAN_OBD)

def branch_APLIMAN_OBD():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_APLIMAN_OBD,feedname2_APLIMAN_OBD)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_APLIMAN_OBD)
    x = readcsv(filepath,feedname2_APLIMAN_OBD)
    logging.info('branch_APLIMAN_OBD' + x)
    return x

Branching_APLIMAN_OBD = BranchPythonOperator(
    task_id='branchid_APLIMAN_OBD',
    python_callable=branch_APLIMAN_OBD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_APLIMAN_OBD = BashOperator(
    task_id='PCF_APLIMAN_OBD',
    bash_command= pcf_command_APLIMAN_OBD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_APLIMAN_OBD = BashOperator(
    task_id='Dedup_APLIMAN_OBD',
    bash_command= dedup_command_APLIMAN_OBD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_APLIMAN_OBD = BashOperator(
    task_id='Dedup2_APLIMAN_OBD',
    bash_command= dedup_command_APLIMAN_OBD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_APLIMAN_OBD = BashOperator(
    task_id='PCFCheck_APLIMAN_OBD',
    bash_command=PCFCheck_command_APLIMAN_OBD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_APLIMAN_OBD = EmailOperator(
    task_id='faile_APLIMAN_OBD',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_APLIMAN_OBD),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_APLIMAN_OBD: PythonOperator = PythonOperator(task_id="waitForFlush_APLIMAN_OBD",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_APLIMAN_OBD >> PCF_APLIMAN_OBD >> PCFCheck_APLIMAN_OBD >> delay_python_task_APLIMAN_OBD >> Dedup_APLIMAN_OBD >> Validation_End 
Branching_APLIMAN_OBD >> Dedup2_APLIMAN_OBD >> Validation_End
Branching_APLIMAN_OBD >> faile_APLIMAN_OBD
Branching_APLIMAN_OBD >> Validation_End


feedname_CALL_REASON = 'CALL_REASON'.upper()
feedname2_CALL_REASON = 'CALL_REASON'.lower()

dedup_command_CALL_REASON="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CALL_REASON)
logging.info(dedup_command_CALL_REASON) 

pcf_command_CALL_REASON = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CALL_REASON)
logging.info(pcf_command_CALL_REASON)

PCFCheck_command_CALL_REASON = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CALL_REASON,run_date,sleeptime)
logging.info(PCFCheck_command_CALL_REASON)

branchScript_CALL_REASON = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CALL_REASON,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CALL_REASON)

def branch_CALL_REASON():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CALL_REASON,feedname2_CALL_REASON)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CALL_REASON)
    x = readcsv(filepath,feedname2_CALL_REASON)
    logging.info('branch_CALL_REASON' + x)
    return x

Branching_CALL_REASON = BranchPythonOperator(
    task_id='branchid_CALL_REASON',
    python_callable=branch_CALL_REASON,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CALL_REASON = BashOperator(
    task_id='PCF_CALL_REASON',
    bash_command= pcf_command_CALL_REASON,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CALL_REASON = BashOperator(
    task_id='Dedup_CALL_REASON',
    bash_command= dedup_command_CALL_REASON,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CALL_REASON = BashOperator(
    task_id='Dedup2_CALL_REASON',
    bash_command= dedup_command_CALL_REASON,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CALL_REASON = BashOperator(
    task_id='PCFCheck_CALL_REASON',
    bash_command=PCFCheck_command_CALL_REASON,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_CALL_REASON = EmailOperator(
    task_id='faile_CALL_REASON',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CALL_REASON),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CALL_REASON: PythonOperator = PythonOperator(task_id="waitForFlush_CALL_REASON",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CALL_REASON >> PCF_CALL_REASON >> PCFCheck_CALL_REASON >> delay_python_task_CALL_REASON >> Dedup_CALL_REASON >> Validation_End 
Branching_CALL_REASON >> Dedup2_CALL_REASON >> Validation_End
Branching_CALL_REASON >> faile_CALL_REASON
Branching_CALL_REASON >> Validation_End


feedname_SMART_APP_DOWNLOAD = 'SMART_APP_DOWNLOAD'.upper()
feedname2_SMART_APP_DOWNLOAD = 'SMART_APP_DOWNLOAD'.lower()

dedup_command_SMART_APP_DOWNLOAD="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SMART_APP_DOWNLOAD)
logging.info(dedup_command_SMART_APP_DOWNLOAD) 

pcf_command_SMART_APP_DOWNLOAD = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SMART_APP_DOWNLOAD)
logging.info(pcf_command_SMART_APP_DOWNLOAD)

PCFCheck_command_SMART_APP_DOWNLOAD = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMART_APP_DOWNLOAD,run_date,sleeptime)
logging.info(PCFCheck_command_SMART_APP_DOWNLOAD)

branchScript_SMART_APP_DOWNLOAD = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SMART_APP_DOWNLOAD,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SMART_APP_DOWNLOAD)

def branch_SMART_APP_DOWNLOAD():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMART_APP_DOWNLOAD,feedname2_SMART_APP_DOWNLOAD)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMART_APP_DOWNLOAD)
    x = readcsv(filepath,feedname2_SMART_APP_DOWNLOAD)
    logging.info('branch_SMART_APP_DOWNLOAD' + x)
    return x

Branching_SMART_APP_DOWNLOAD = BranchPythonOperator(
    task_id='branchid_SMART_APP_DOWNLOAD',
    python_callable=branch_SMART_APP_DOWNLOAD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMART_APP_DOWNLOAD = BashOperator(
    task_id='PCF_SMART_APP_DOWNLOAD',
    bash_command= pcf_command_SMART_APP_DOWNLOAD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SMART_APP_DOWNLOAD = BashOperator(
    task_id='Dedup_SMART_APP_DOWNLOAD',
    bash_command= dedup_command_SMART_APP_DOWNLOAD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SMART_APP_DOWNLOAD = BashOperator(
    task_id='Dedup2_SMART_APP_DOWNLOAD',
    bash_command= dedup_command_SMART_APP_DOWNLOAD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SMART_APP_DOWNLOAD = BashOperator(
    task_id='PCFCheck_SMART_APP_DOWNLOAD',
    bash_command=PCFCheck_command_SMART_APP_DOWNLOAD,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_SMART_APP_DOWNLOAD = EmailOperator(
    task_id='faile_SMART_APP_DOWNLOAD',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMART_APP_DOWNLOAD),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SMART_APP_DOWNLOAD: PythonOperator = PythonOperator(task_id="waitForFlush_SMART_APP_DOWNLOAD",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SMART_APP_DOWNLOAD >> PCF_SMART_APP_DOWNLOAD >> PCFCheck_SMART_APP_DOWNLOAD >> delay_python_task_SMART_APP_DOWNLOAD >> Dedup_SMART_APP_DOWNLOAD >> Validation_End 
Branching_SMART_APP_DOWNLOAD >> Dedup2_SMART_APP_DOWNLOAD >> Validation_End
Branching_SMART_APP_DOWNLOAD >> faile_SMART_APP_DOWNLOAD
Branching_SMART_APP_DOWNLOAD >> Validation_End
