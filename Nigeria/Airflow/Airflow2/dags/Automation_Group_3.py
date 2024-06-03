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
d_1 = Variable.get("rerunDate3", deserialize_json=True)
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

dag = DAG('New_Automation_Group_3', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group3 -c config.json' %(d_1)
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


feedname_SIS_TOKEN = 'SIS_TOKEN'.upper()
feedname2_SIS_TOKEN = 'SIS_TOKEN'.lower()

dedup_command_SIS_TOKEN="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SIS_TOKEN)
logging.info(dedup_command_SIS_TOKEN) 

pcf_command_SIS_TOKEN = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SIS_TOKEN)
logging.info(pcf_command_SIS_TOKEN)

PCFCheck_command_SIS_TOKEN = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SIS_TOKEN,run_date,sleeptime)
logging.info(PCFCheck_command_SIS_TOKEN)

branchScript_SIS_TOKEN = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SIS_TOKEN,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SIS_TOKEN)

def branch_SIS_TOKEN():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SIS_TOKEN,feedname2_SIS_TOKEN)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SIS_TOKEN)
    x = readcsv(filepath,feedname2_SIS_TOKEN)
    logging.info('branch_SIS_TOKEN' + x)
    return x

Branching_SIS_TOKEN = BranchPythonOperator(
    task_id='branchid_SIS_TOKEN',
    python_callable=branch_SIS_TOKEN,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SIS_TOKEN = BashOperator(
    task_id='PCF_SIS_TOKEN',
    bash_command= pcf_command_SIS_TOKEN,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SIS_TOKEN = BashOperator(
    task_id='Dedup_SIS_TOKEN',
    bash_command= dedup_command_SIS_TOKEN,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SIS_TOKEN = BashOperator(
    task_id='Dedup2_SIS_TOKEN',
    bash_command= dedup_command_SIS_TOKEN,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SIS_TOKEN = BashOperator(
    task_id='PCFCheck_SIS_TOKEN',
    bash_command=PCFCheck_command_SIS_TOKEN,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_SIS_TOKEN = EmailOperator(
    task_id='faile_SIS_TOKEN',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SIS_TOKEN),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SIS_TOKEN: PythonOperator = PythonOperator(task_id="waitForFlush_SIS_TOKEN",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SIS_TOKEN >> PCF_SIS_TOKEN >> PCFCheck_SIS_TOKEN >> delay_python_task_SIS_TOKEN >> Dedup_SIS_TOKEN >> Validation_End 
Branching_SIS_TOKEN >> Dedup2_SIS_TOKEN >> Validation_End
Branching_SIS_TOKEN >> faile_SIS_TOKEN
Branching_SIS_TOKEN >> Validation_End


feedname_CHATBOT = 'CHATBOT'.upper()
feedname2_CHATBOT = 'CHATBOT'.lower()

dedup_command_CHATBOT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CHATBOT)
logging.info(dedup_command_CHATBOT) 

pcf_command_CHATBOT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CHATBOT)
logging.info(pcf_command_CHATBOT)

PCFCheck_command_CHATBOT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CHATBOT,run_date,sleeptime)
logging.info(PCFCheck_command_CHATBOT)

branchScript_CHATBOT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CHATBOT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CHATBOT)

def branch_CHATBOT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CHATBOT,feedname2_CHATBOT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CHATBOT)
    x = readcsv(filepath,feedname2_CHATBOT)
    logging.info('branch_CHATBOT' + x)
    return x

Branching_CHATBOT = BranchPythonOperator(
    task_id='branchid_CHATBOT',
    python_callable=branch_CHATBOT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CHATBOT = BashOperator(
    task_id='PCF_CHATBOT',
    bash_command= pcf_command_CHATBOT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CHATBOT = BashOperator(
    task_id='Dedup_CHATBOT',
    bash_command= dedup_command_CHATBOT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CHATBOT = BashOperator(
    task_id='Dedup2_CHATBOT',
    bash_command= dedup_command_CHATBOT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CHATBOT = BashOperator(
    task_id='PCFCheck_CHATBOT',
    bash_command=PCFCheck_command_CHATBOT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CHATBOT = EmailOperator(
    task_id='faile_CHATBOT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CHATBOT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CHATBOT: PythonOperator = PythonOperator(task_id="waitForFlush_CHATBOT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CHATBOT >> PCF_CHATBOT >> PCFCheck_CHATBOT >> delay_python_task_CHATBOT >> Dedup_CHATBOT >> Validation_End 
Branching_CHATBOT >> Dedup2_CHATBOT >> Validation_End
Branching_CHATBOT >> faile_CHATBOT
Branching_CHATBOT >> Validation_End


feedname_AGL_CRM_COUNTRY_MAP = 'AGL_CRM_COUNTRY_MAP'.upper()
feedname2_AGL_CRM_COUNTRY_MAP = 'AGL_CRM_COUNTRY_MAP'.lower()

dedup_command_AGL_CRM_COUNTRY_MAP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AGL_CRM_COUNTRY_MAP)
logging.info(dedup_command_AGL_CRM_COUNTRY_MAP) 

pcf_command_AGL_CRM_COUNTRY_MAP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AGL_CRM_COUNTRY_MAP)
logging.info(pcf_command_AGL_CRM_COUNTRY_MAP)

PCFCheck_command_AGL_CRM_COUNTRY_MAP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AGL_CRM_COUNTRY_MAP,run_date,sleeptime)
logging.info(PCFCheck_command_AGL_CRM_COUNTRY_MAP)

branchScript_AGL_CRM_COUNTRY_MAP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AGL_CRM_COUNTRY_MAP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AGL_CRM_COUNTRY_MAP)

def branch_AGL_CRM_COUNTRY_MAP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AGL_CRM_COUNTRY_MAP,feedname2_AGL_CRM_COUNTRY_MAP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AGL_CRM_COUNTRY_MAP)
    x = readcsv(filepath,feedname2_AGL_CRM_COUNTRY_MAP)
    logging.info('branch_AGL_CRM_COUNTRY_MAP' + x)
    return x

Branching_AGL_CRM_COUNTRY_MAP = BranchPythonOperator(
    task_id='branchid_AGL_CRM_COUNTRY_MAP',
    python_callable=branch_AGL_CRM_COUNTRY_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AGL_CRM_COUNTRY_MAP = BashOperator(
    task_id='PCF_AGL_CRM_COUNTRY_MAP',
    bash_command= pcf_command_AGL_CRM_COUNTRY_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AGL_CRM_COUNTRY_MAP = BashOperator(
    task_id='Dedup_AGL_CRM_COUNTRY_MAP',
    bash_command= dedup_command_AGL_CRM_COUNTRY_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AGL_CRM_COUNTRY_MAP = BashOperator(
    task_id='Dedup2_AGL_CRM_COUNTRY_MAP',
    bash_command= dedup_command_AGL_CRM_COUNTRY_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AGL_CRM_COUNTRY_MAP = BashOperator(
    task_id='PCFCheck_AGL_CRM_COUNTRY_MAP',
    bash_command=PCFCheck_command_AGL_CRM_COUNTRY_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AGL_CRM_COUNTRY_MAP = EmailOperator(
    task_id='faile_AGL_CRM_COUNTRY_MAP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AGL_CRM_COUNTRY_MAP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AGL_CRM_COUNTRY_MAP: PythonOperator = PythonOperator(task_id="waitForFlush_AGL_CRM_COUNTRY_MAP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AGL_CRM_COUNTRY_MAP >> PCF_AGL_CRM_COUNTRY_MAP >> PCFCheck_AGL_CRM_COUNTRY_MAP >> delay_python_task_AGL_CRM_COUNTRY_MAP >> Dedup_AGL_CRM_COUNTRY_MAP >> Validation_End 
Branching_AGL_CRM_COUNTRY_MAP >> Dedup2_AGL_CRM_COUNTRY_MAP >> Validation_End
Branching_AGL_CRM_COUNTRY_MAP >> faile_AGL_CRM_COUNTRY_MAP
Branching_AGL_CRM_COUNTRY_MAP >> Validation_End


feedname_AGL_CRM_LGA_MAP = 'AGL_CRM_LGA_MAP'.upper()
feedname2_AGL_CRM_LGA_MAP = 'AGL_CRM_LGA_MAP'.lower()

dedup_command_AGL_CRM_LGA_MAP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AGL_CRM_LGA_MAP)
logging.info(dedup_command_AGL_CRM_LGA_MAP) 

pcf_command_AGL_CRM_LGA_MAP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AGL_CRM_LGA_MAP)
logging.info(pcf_command_AGL_CRM_LGA_MAP)

PCFCheck_command_AGL_CRM_LGA_MAP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AGL_CRM_LGA_MAP,run_date,sleeptime)
logging.info(PCFCheck_command_AGL_CRM_LGA_MAP)

branchScript_AGL_CRM_LGA_MAP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AGL_CRM_LGA_MAP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AGL_CRM_LGA_MAP)

def branch_AGL_CRM_LGA_MAP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AGL_CRM_LGA_MAP,feedname2_AGL_CRM_LGA_MAP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AGL_CRM_LGA_MAP)
    x = readcsv(filepath,feedname2_AGL_CRM_LGA_MAP)
    logging.info('branch_AGL_CRM_LGA_MAP' + x)
    return x

Branching_AGL_CRM_LGA_MAP = BranchPythonOperator(
    task_id='branchid_AGL_CRM_LGA_MAP',
    python_callable=branch_AGL_CRM_LGA_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AGL_CRM_LGA_MAP = BashOperator(
    task_id='PCF_AGL_CRM_LGA_MAP',
    bash_command= pcf_command_AGL_CRM_LGA_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AGL_CRM_LGA_MAP = BashOperator(
    task_id='Dedup_AGL_CRM_LGA_MAP',
    bash_command= dedup_command_AGL_CRM_LGA_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AGL_CRM_LGA_MAP = BashOperator(
    task_id='Dedup2_AGL_CRM_LGA_MAP',
    bash_command= dedup_command_AGL_CRM_LGA_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AGL_CRM_LGA_MAP = BashOperator(
    task_id='PCFCheck_AGL_CRM_LGA_MAP',
    bash_command=PCFCheck_command_AGL_CRM_LGA_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AGL_CRM_LGA_MAP = EmailOperator(
    task_id='faile_AGL_CRM_LGA_MAP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AGL_CRM_LGA_MAP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AGL_CRM_LGA_MAP: PythonOperator = PythonOperator(task_id="waitForFlush_AGL_CRM_LGA_MAP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AGL_CRM_LGA_MAP >> PCF_AGL_CRM_LGA_MAP >> PCFCheck_AGL_CRM_LGA_MAP >> delay_python_task_AGL_CRM_LGA_MAP >> Dedup_AGL_CRM_LGA_MAP >> Validation_End 
Branching_AGL_CRM_LGA_MAP >> Dedup2_AGL_CRM_LGA_MAP >> Validation_End
Branching_AGL_CRM_LGA_MAP >> faile_AGL_CRM_LGA_MAP
Branching_AGL_CRM_LGA_MAP >> Validation_End


feedname_AGL_CRM_STATE_MAP = 'AGL_CRM_STATE_MAP'.upper()
feedname2_AGL_CRM_STATE_MAP = 'AGL_CRM_STATE_MAP'.lower()

dedup_command_AGL_CRM_STATE_MAP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AGL_CRM_STATE_MAP)
logging.info(dedup_command_AGL_CRM_STATE_MAP) 

pcf_command_AGL_CRM_STATE_MAP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AGL_CRM_STATE_MAP)
logging.info(pcf_command_AGL_CRM_STATE_MAP)

PCFCheck_command_AGL_CRM_STATE_MAP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AGL_CRM_STATE_MAP,run_date,sleeptime)
logging.info(PCFCheck_command_AGL_CRM_STATE_MAP)

branchScript_AGL_CRM_STATE_MAP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AGL_CRM_STATE_MAP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AGL_CRM_STATE_MAP)

def branch_AGL_CRM_STATE_MAP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AGL_CRM_STATE_MAP,feedname2_AGL_CRM_STATE_MAP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AGL_CRM_STATE_MAP)
    x = readcsv(filepath,feedname2_AGL_CRM_STATE_MAP)
    logging.info('branch_AGL_CRM_STATE_MAP' + x)
    return x

Branching_AGL_CRM_STATE_MAP = BranchPythonOperator(
    task_id='branchid_AGL_CRM_STATE_MAP',
    python_callable=branch_AGL_CRM_STATE_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AGL_CRM_STATE_MAP = BashOperator(
    task_id='PCF_AGL_CRM_STATE_MAP',
    bash_command= pcf_command_AGL_CRM_STATE_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AGL_CRM_STATE_MAP = BashOperator(
    task_id='Dedup_AGL_CRM_STATE_MAP',
    bash_command= dedup_command_AGL_CRM_STATE_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AGL_CRM_STATE_MAP = BashOperator(
    task_id='Dedup2_AGL_CRM_STATE_MAP',
    bash_command= dedup_command_AGL_CRM_STATE_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AGL_CRM_STATE_MAP = BashOperator(
    task_id='PCFCheck_AGL_CRM_STATE_MAP',
    bash_command=PCFCheck_command_AGL_CRM_STATE_MAP,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AGL_CRM_STATE_MAP = EmailOperator(
    task_id='faile_AGL_CRM_STATE_MAP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AGL_CRM_STATE_MAP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AGL_CRM_STATE_MAP: PythonOperator = PythonOperator(task_id="waitForFlush_AGL_CRM_STATE_MAP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AGL_CRM_STATE_MAP >> PCF_AGL_CRM_STATE_MAP >> PCFCheck_AGL_CRM_STATE_MAP >> delay_python_task_AGL_CRM_STATE_MAP >> Dedup_AGL_CRM_STATE_MAP >> Validation_End 
Branching_AGL_CRM_STATE_MAP >> Dedup2_AGL_CRM_STATE_MAP >> Validation_End
Branching_AGL_CRM_STATE_MAP >> faile_AGL_CRM_STATE_MAP
Branching_AGL_CRM_STATE_MAP >> Validation_End


feedname_QMATIC = 'QMATIC'.upper()
feedname2_QMATIC = 'QMATIC'.lower()

dedup_command_QMATIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_QMATIC)
logging.info(dedup_command_QMATIC) 

pcf_command_QMATIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_QMATIC)
logging.info(pcf_command_QMATIC)

PCFCheck_command_QMATIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_QMATIC,run_date,sleeptime)
logging.info(PCFCheck_command_QMATIC)

branchScript_QMATIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_QMATIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_QMATIC)

def branch_QMATIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_QMATIC,feedname2_QMATIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_QMATIC)
    x = readcsv(filepath,feedname2_QMATIC)
    logging.info('branch_QMATIC' + x)
    return x

Branching_QMATIC = BranchPythonOperator(
    task_id='branchid_QMATIC',
    python_callable=branch_QMATIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_QMATIC = BashOperator(
    task_id='PCF_QMATIC',
    bash_command= pcf_command_QMATIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_QMATIC = BashOperator(
    task_id='Dedup_QMATIC',
    bash_command= dedup_command_QMATIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_QMATIC = BashOperator(
    task_id='Dedup2_QMATIC',
    bash_command= dedup_command_QMATIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_QMATIC = BashOperator(
    task_id='PCFCheck_QMATIC',
    bash_command=PCFCheck_command_QMATIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_QMATIC = EmailOperator(
    task_id='faile_QMATIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_QMATIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_QMATIC: PythonOperator = PythonOperator(task_id="waitForFlush_QMATIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_QMATIC >> PCF_QMATIC >> PCFCheck_QMATIC >> delay_python_task_QMATIC >> Dedup_QMATIC >> Validation_End 
Branching_QMATIC >> Dedup2_QMATIC >> Validation_End
Branching_QMATIC >> faile_QMATIC
Branching_QMATIC >> Validation_End


feedname_CB_SCHEDULES = 'CB_SCHEDULES'.upper()
feedname2_CB_SCHEDULES = 'CB_SCHEDULES'.lower()

dedup_command_CB_SCHEDULES="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CB_SCHEDULES)
logging.info(dedup_command_CB_SCHEDULES) 

pcf_command_CB_SCHEDULES = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CB_SCHEDULES)
logging.info(pcf_command_CB_SCHEDULES)

PCFCheck_command_CB_SCHEDULES = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_SCHEDULES,run_date,sleeptime)
logging.info(PCFCheck_command_CB_SCHEDULES)

branchScript_CB_SCHEDULES = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_SCHEDULES,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_SCHEDULES)

def branch_CB_SCHEDULES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_SCHEDULES,feedname2_CB_SCHEDULES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_SCHEDULES)
    x = readcsv(filepath,feedname2_CB_SCHEDULES)
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
    priority_weight=1
)

Dedup_CB_SCHEDULES = BashOperator(
    task_id='Dedup_CB_SCHEDULES',
    bash_command= dedup_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CB_SCHEDULES = BashOperator(
    task_id='Dedup2_CB_SCHEDULES',
    bash_command= dedup_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CB_SCHEDULES = BashOperator(
    task_id='PCFCheck_CB_SCHEDULES',
    bash_command=PCFCheck_command_CB_SCHEDULES,
    dag=dag,
    run_as_user='daasuser',
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

delay_python_task_CB_SCHEDULES: PythonOperator = PythonOperator(task_id="waitForFlush_CB_SCHEDULES",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_SCHEDULES >> PCF_CB_SCHEDULES >> PCFCheck_CB_SCHEDULES >> delay_python_task_CB_SCHEDULES >> Dedup_CB_SCHEDULES >> Validation_End 
Branching_CB_SCHEDULES >> Dedup2_CB_SCHEDULES >> Validation_End
Branching_CB_SCHEDULES >> faile_CB_SCHEDULES
Branching_CB_SCHEDULES >> Validation_End


feedname_EMM_DELIVERY_KPI = 'EMM_DELIVERY_KPI'.upper()
feedname2_EMM_DELIVERY_KPI = 'EMM_DELIVERY_KPI'.lower()

dedup_command_EMM_DELIVERY_KPI="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_EMM_DELIVERY_KPI)
logging.info(dedup_command_EMM_DELIVERY_KPI) 

pcf_command_EMM_DELIVERY_KPI = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_EMM_DELIVERY_KPI)
logging.info(pcf_command_EMM_DELIVERY_KPI)

PCFCheck_command_EMM_DELIVERY_KPI = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EMM_DELIVERY_KPI,run_date,sleeptime)
logging.info(PCFCheck_command_EMM_DELIVERY_KPI)

branchScript_EMM_DELIVERY_KPI = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_EMM_DELIVERY_KPI,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_EMM_DELIVERY_KPI)

def branch_EMM_DELIVERY_KPI():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EMM_DELIVERY_KPI,feedname2_EMM_DELIVERY_KPI)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EMM_DELIVERY_KPI)
    x = readcsv(filepath,feedname2_EMM_DELIVERY_KPI)
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
    priority_weight=1
)

Dedup_EMM_DELIVERY_KPI = BashOperator(
    task_id='Dedup_EMM_DELIVERY_KPI',
    bash_command= dedup_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_EMM_DELIVERY_KPI = BashOperator(
    task_id='Dedup2_EMM_DELIVERY_KPI',
    bash_command= dedup_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_EMM_DELIVERY_KPI = BashOperator(
    task_id='PCFCheck_EMM_DELIVERY_KPI',
    bash_command=PCFCheck_command_EMM_DELIVERY_KPI,
    dag=dag,
    run_as_user='daasuser',
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

delay_python_task_EMM_DELIVERY_KPI: PythonOperator = PythonOperator(task_id="waitForFlush_EMM_DELIVERY_KPI",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_EMM_DELIVERY_KPI >> PCF_EMM_DELIVERY_KPI >> PCFCheck_EMM_DELIVERY_KPI >> delay_python_task_EMM_DELIVERY_KPI >> Dedup_EMM_DELIVERY_KPI >> Validation_End 
Branching_EMM_DELIVERY_KPI >> Dedup2_EMM_DELIVERY_KPI >> Validation_End
Branching_EMM_DELIVERY_KPI >> faile_EMM_DELIVERY_KPI
Branching_EMM_DELIVERY_KPI >> Validation_End


feedname_EBU_GROUP_ESCALATIONS = 'EBU_GROUP_ESCALATIONS'.upper()
feedname2_EBU_GROUP_ESCALATIONS = 'EBU_GROUP_ESCALATIONS'.lower()

dedup_command_EBU_GROUP_ESCALATIONS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_EBU_GROUP_ESCALATIONS)
logging.info(dedup_command_EBU_GROUP_ESCALATIONS) 

pcf_command_EBU_GROUP_ESCALATIONS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_EBU_GROUP_ESCALATIONS)
logging.info(pcf_command_EBU_GROUP_ESCALATIONS)

PCFCheck_command_EBU_GROUP_ESCALATIONS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EBU_GROUP_ESCALATIONS,run_date,sleeptime)
logging.info(PCFCheck_command_EBU_GROUP_ESCALATIONS)

branchScript_EBU_GROUP_ESCALATIONS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_EBU_GROUP_ESCALATIONS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_EBU_GROUP_ESCALATIONS)

def branch_EBU_GROUP_ESCALATIONS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EBU_GROUP_ESCALATIONS,feedname2_EBU_GROUP_ESCALATIONS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EBU_GROUP_ESCALATIONS)
    x = readcsv(filepath,feedname2_EBU_GROUP_ESCALATIONS)
    logging.info('branch_EBU_GROUP_ESCALATIONS' + x)
    return x

Branching_EBU_GROUP_ESCALATIONS = BranchPythonOperator(
    task_id='branchid_EBU_GROUP_ESCALATIONS',
    python_callable=branch_EBU_GROUP_ESCALATIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_EBU_GROUP_ESCALATIONS = BashOperator(
    task_id='PCF_EBU_GROUP_ESCALATIONS',
    bash_command= pcf_command_EBU_GROUP_ESCALATIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_EBU_GROUP_ESCALATIONS = BashOperator(
    task_id='Dedup_EBU_GROUP_ESCALATIONS',
    bash_command= dedup_command_EBU_GROUP_ESCALATIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_EBU_GROUP_ESCALATIONS = BashOperator(
    task_id='Dedup2_EBU_GROUP_ESCALATIONS',
    bash_command= dedup_command_EBU_GROUP_ESCALATIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_EBU_GROUP_ESCALATIONS = BashOperator(
    task_id='PCFCheck_EBU_GROUP_ESCALATIONS',
    bash_command=PCFCheck_command_EBU_GROUP_ESCALATIONS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_EBU_GROUP_ESCALATIONS = EmailOperator(
    task_id='faile_EBU_GROUP_ESCALATIONS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_EBU_GROUP_ESCALATIONS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_EBU_GROUP_ESCALATIONS: PythonOperator = PythonOperator(task_id="waitForFlush_EBU_GROUP_ESCALATIONS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_EBU_GROUP_ESCALATIONS >> PCF_EBU_GROUP_ESCALATIONS >> PCFCheck_EBU_GROUP_ESCALATIONS >> delay_python_task_EBU_GROUP_ESCALATIONS >> Dedup_EBU_GROUP_ESCALATIONS >> Validation_End 
Branching_EBU_GROUP_ESCALATIONS >> Dedup2_EBU_GROUP_ESCALATIONS >> Validation_End
Branching_EBU_GROUP_ESCALATIONS >> faile_EBU_GROUP_ESCALATIONS
Branching_EBU_GROUP_ESCALATIONS >> Validation_End


feedname_CB_NIN_UPDT_REQUEST_POOL = 'CB_NIN_UPDT_REQUEST_POOL'.upper()
feedname2_CB_NIN_UPDT_REQUEST_POOL = 'CB_NIN_UPDT_REQUEST_POOL'.lower()

dedup_command_CB_NIN_UPDT_REQUEST_POOL="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CB_NIN_UPDT_REQUEST_POOL)
logging.info(dedup_command_CB_NIN_UPDT_REQUEST_POOL) 

pcf_command_CB_NIN_UPDT_REQUEST_POOL = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CB_NIN_UPDT_REQUEST_POOL)
logging.info(pcf_command_CB_NIN_UPDT_REQUEST_POOL)

PCFCheck_command_CB_NIN_UPDT_REQUEST_POOL = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_NIN_UPDT_REQUEST_POOL,run_date,sleeptime)
logging.info(PCFCheck_command_CB_NIN_UPDT_REQUEST_POOL)

branchScript_CB_NIN_UPDT_REQUEST_POOL = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_NIN_UPDT_REQUEST_POOL,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_NIN_UPDT_REQUEST_POOL)

def branch_CB_NIN_UPDT_REQUEST_POOL():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_NIN_UPDT_REQUEST_POOL,feedname2_CB_NIN_UPDT_REQUEST_POOL)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_NIN_UPDT_REQUEST_POOL)
    x = readcsv(filepath,feedname2_CB_NIN_UPDT_REQUEST_POOL)
    logging.info('branch_CB_NIN_UPDT_REQUEST_POOL' + x)
    return x

Branching_CB_NIN_UPDT_REQUEST_POOL = BranchPythonOperator(
    task_id='branchid_CB_NIN_UPDT_REQUEST_POOL',
    python_callable=branch_CB_NIN_UPDT_REQUEST_POOL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CB_NIN_UPDT_REQUEST_POOL = BashOperator(
    task_id='PCF_CB_NIN_UPDT_REQUEST_POOL',
    bash_command= pcf_command_CB_NIN_UPDT_REQUEST_POOL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CB_NIN_UPDT_REQUEST_POOL = BashOperator(
    task_id='Dedup_CB_NIN_UPDT_REQUEST_POOL',
    bash_command= dedup_command_CB_NIN_UPDT_REQUEST_POOL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CB_NIN_UPDT_REQUEST_POOL = BashOperator(
    task_id='Dedup2_CB_NIN_UPDT_REQUEST_POOL',
    bash_command= dedup_command_CB_NIN_UPDT_REQUEST_POOL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CB_NIN_UPDT_REQUEST_POOL = BashOperator(
    task_id='PCFCheck_CB_NIN_UPDT_REQUEST_POOL',
    bash_command=PCFCheck_command_CB_NIN_UPDT_REQUEST_POOL,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CB_NIN_UPDT_REQUEST_POOL = EmailOperator(
    task_id='faile_CB_NIN_UPDT_REQUEST_POOL',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_NIN_UPDT_REQUEST_POOL),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CB_NIN_UPDT_REQUEST_POOL: PythonOperator = PythonOperator(task_id="waitForFlush_CB_NIN_UPDT_REQUEST_POOL",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_NIN_UPDT_REQUEST_POOL >> PCF_CB_NIN_UPDT_REQUEST_POOL >> PCFCheck_CB_NIN_UPDT_REQUEST_POOL >> delay_python_task_CB_NIN_UPDT_REQUEST_POOL >> Dedup_CB_NIN_UPDT_REQUEST_POOL >> Validation_End 
Branching_CB_NIN_UPDT_REQUEST_POOL >> Dedup2_CB_NIN_UPDT_REQUEST_POOL >> Validation_End
Branching_CB_NIN_UPDT_REQUEST_POOL >> faile_CB_NIN_UPDT_REQUEST_POOL
Branching_CB_NIN_UPDT_REQUEST_POOL >> Validation_End


feedname_TBL_DATA_AGENT_REGISTRATION_VW = 'TBL_DATA_AGENT_REGISTRATION_VW'.upper()
feedname2_TBL_DATA_AGENT_REGISTRATION_VW = 'TBL_DATA_AGENT_REGISTRATION_VW'.lower()

dedup_command_TBL_DATA_AGENT_REGISTRATION_VW="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_TBL_DATA_AGENT_REGISTRATION_VW)
logging.info(dedup_command_TBL_DATA_AGENT_REGISTRATION_VW) 

pcf_command_TBL_DATA_AGENT_REGISTRATION_VW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_TBL_DATA_AGENT_REGISTRATION_VW)
logging.info(pcf_command_TBL_DATA_AGENT_REGISTRATION_VW)

PCFCheck_command_TBL_DATA_AGENT_REGISTRATION_VW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_TBL_DATA_AGENT_REGISTRATION_VW,run_date,sleeptime)
logging.info(PCFCheck_command_TBL_DATA_AGENT_REGISTRATION_VW)

branchScript_TBL_DATA_AGENT_REGISTRATION_VW = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_TBL_DATA_AGENT_REGISTRATION_VW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_TBL_DATA_AGENT_REGISTRATION_VW)

def branch_TBL_DATA_AGENT_REGISTRATION_VW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_TBL_DATA_AGENT_REGISTRATION_VW,feedname2_TBL_DATA_AGENT_REGISTRATION_VW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_TBL_DATA_AGENT_REGISTRATION_VW)
    x = readcsv(filepath,feedname2_TBL_DATA_AGENT_REGISTRATION_VW)
    logging.info('branch_TBL_DATA_AGENT_REGISTRATION_VW' + x)
    return x

Branching_TBL_DATA_AGENT_REGISTRATION_VW = BranchPythonOperator(
    task_id='branchid_TBL_DATA_AGENT_REGISTRATION_VW',
    python_callable=branch_TBL_DATA_AGENT_REGISTRATION_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_TBL_DATA_AGENT_REGISTRATION_VW = BashOperator(
    task_id='PCF_TBL_DATA_AGENT_REGISTRATION_VW',
    bash_command= pcf_command_TBL_DATA_AGENT_REGISTRATION_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_TBL_DATA_AGENT_REGISTRATION_VW = BashOperator(
    task_id='Dedup_TBL_DATA_AGENT_REGISTRATION_VW',
    bash_command= dedup_command_TBL_DATA_AGENT_REGISTRATION_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_TBL_DATA_AGENT_REGISTRATION_VW = BashOperator(
    task_id='Dedup2_TBL_DATA_AGENT_REGISTRATION_VW',
    bash_command= dedup_command_TBL_DATA_AGENT_REGISTRATION_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_TBL_DATA_AGENT_REGISTRATION_VW = BashOperator(
    task_id='PCFCheck_TBL_DATA_AGENT_REGISTRATION_VW',
    bash_command=PCFCheck_command_TBL_DATA_AGENT_REGISTRATION_VW,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_TBL_DATA_AGENT_REGISTRATION_VW = EmailOperator(
    task_id='faile_TBL_DATA_AGENT_REGISTRATION_VW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_TBL_DATA_AGENT_REGISTRATION_VW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_TBL_DATA_AGENT_REGISTRATION_VW: PythonOperator = PythonOperator(task_id="waitForFlush_TBL_DATA_AGENT_REGISTRATION_VW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_TBL_DATA_AGENT_REGISTRATION_VW >> PCF_TBL_DATA_AGENT_REGISTRATION_VW >> PCFCheck_TBL_DATA_AGENT_REGISTRATION_VW >> delay_python_task_TBL_DATA_AGENT_REGISTRATION_VW >> Dedup_TBL_DATA_AGENT_REGISTRATION_VW >> Validation_End 
Branching_TBL_DATA_AGENT_REGISTRATION_VW >> Dedup2_TBL_DATA_AGENT_REGISTRATION_VW >> Validation_End
Branching_TBL_DATA_AGENT_REGISTRATION_VW >> faile_TBL_DATA_AGENT_REGISTRATION_VW
Branching_TBL_DATA_AGENT_REGISTRATION_VW >> Validation_End


feedname_EBU_GROUP_ESCALATIONS_PENDING = 'EBU_GROUP_ESCALATIONS_PENDING'.upper()
feedname2_EBU_GROUP_ESCALATIONS_PENDING = 'EBU_GROUP_ESCALATIONS_PENDING'.lower()

dedup_command_EBU_GROUP_ESCALATIONS_PENDING="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_EBU_GROUP_ESCALATIONS_PENDING)
logging.info(dedup_command_EBU_GROUP_ESCALATIONS_PENDING) 

pcf_command_EBU_GROUP_ESCALATIONS_PENDING = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_EBU_GROUP_ESCALATIONS_PENDING)
logging.info(pcf_command_EBU_GROUP_ESCALATIONS_PENDING)

PCFCheck_command_EBU_GROUP_ESCALATIONS_PENDING = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EBU_GROUP_ESCALATIONS_PENDING,run_date,sleeptime)
logging.info(PCFCheck_command_EBU_GROUP_ESCALATIONS_PENDING)

branchScript_EBU_GROUP_ESCALATIONS_PENDING = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_EBU_GROUP_ESCALATIONS_PENDING,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_EBU_GROUP_ESCALATIONS_PENDING)

def branch_EBU_GROUP_ESCALATIONS_PENDING():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EBU_GROUP_ESCALATIONS_PENDING,feedname2_EBU_GROUP_ESCALATIONS_PENDING)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EBU_GROUP_ESCALATIONS_PENDING)
    x = readcsv(filepath,feedname2_EBU_GROUP_ESCALATIONS_PENDING)
    logging.info('branch_EBU_GROUP_ESCALATIONS_PENDING' + x)
    return x

Branching_EBU_GROUP_ESCALATIONS_PENDING = BranchPythonOperator(
    task_id='branchid_EBU_GROUP_ESCALATIONS_PENDING',
    python_callable=branch_EBU_GROUP_ESCALATIONS_PENDING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_EBU_GROUP_ESCALATIONS_PENDING = BashOperator(
    task_id='PCF_EBU_GROUP_ESCALATIONS_PENDING',
    bash_command= pcf_command_EBU_GROUP_ESCALATIONS_PENDING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_EBU_GROUP_ESCALATIONS_PENDING = BashOperator(
    task_id='Dedup_EBU_GROUP_ESCALATIONS_PENDING',
    bash_command= dedup_command_EBU_GROUP_ESCALATIONS_PENDING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_EBU_GROUP_ESCALATIONS_PENDING = BashOperator(
    task_id='Dedup2_EBU_GROUP_ESCALATIONS_PENDING',
    bash_command= dedup_command_EBU_GROUP_ESCALATIONS_PENDING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_EBU_GROUP_ESCALATIONS_PENDING = BashOperator(
    task_id='PCFCheck_EBU_GROUP_ESCALATIONS_PENDING',
    bash_command=PCFCheck_command_EBU_GROUP_ESCALATIONS_PENDING,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_EBU_GROUP_ESCALATIONS_PENDING = EmailOperator(
    task_id='faile_EBU_GROUP_ESCALATIONS_PENDING',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_EBU_GROUP_ESCALATIONS_PENDING),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_EBU_GROUP_ESCALATIONS_PENDING: PythonOperator = PythonOperator(task_id="waitForFlush_EBU_GROUP_ESCALATIONS_PENDING",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_EBU_GROUP_ESCALATIONS_PENDING >> PCF_EBU_GROUP_ESCALATIONS_PENDING >> PCFCheck_EBU_GROUP_ESCALATIONS_PENDING >> delay_python_task_EBU_GROUP_ESCALATIONS_PENDING >> Dedup_EBU_GROUP_ESCALATIONS_PENDING >> Validation_End 
Branching_EBU_GROUP_ESCALATIONS_PENDING >> Dedup2_EBU_GROUP_ESCALATIONS_PENDING >> Validation_End
Branching_EBU_GROUP_ESCALATIONS_PENDING >> faile_EBU_GROUP_ESCALATIONS_PENDING
Branching_EBU_GROUP_ESCALATIONS_PENDING >> Validation_End


feedname_EBU_GROUP_ESCALATIONS_CLOSED = 'EBU_GROUP_ESCALATIONS_CLOSED'.upper()
feedname2_EBU_GROUP_ESCALATIONS_CLOSED = 'EBU_GROUP_ESCALATIONS_CLOSED'.lower()

dedup_command_EBU_GROUP_ESCALATIONS_CLOSED="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_EBU_GROUP_ESCALATIONS_CLOSED)
logging.info(dedup_command_EBU_GROUP_ESCALATIONS_CLOSED) 

pcf_command_EBU_GROUP_ESCALATIONS_CLOSED = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_EBU_GROUP_ESCALATIONS_CLOSED)
logging.info(pcf_command_EBU_GROUP_ESCALATIONS_CLOSED)

PCFCheck_command_EBU_GROUP_ESCALATIONS_CLOSED = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_EBU_GROUP_ESCALATIONS_CLOSED,run_date,sleeptime)
logging.info(PCFCheck_command_EBU_GROUP_ESCALATIONS_CLOSED)

branchScript_EBU_GROUP_ESCALATIONS_CLOSED = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_EBU_GROUP_ESCALATIONS_CLOSED,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_EBU_GROUP_ESCALATIONS_CLOSED)

def branch_EBU_GROUP_ESCALATIONS_CLOSED():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_EBU_GROUP_ESCALATIONS_CLOSED,feedname2_EBU_GROUP_ESCALATIONS_CLOSED)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_EBU_GROUP_ESCALATIONS_CLOSED)
    x = readcsv(filepath,feedname2_EBU_GROUP_ESCALATIONS_CLOSED)
    logging.info('branch_EBU_GROUP_ESCALATIONS_CLOSED' + x)
    return x

Branching_EBU_GROUP_ESCALATIONS_CLOSED = BranchPythonOperator(
    task_id='branchid_EBU_GROUP_ESCALATIONS_CLOSED',
    python_callable=branch_EBU_GROUP_ESCALATIONS_CLOSED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_EBU_GROUP_ESCALATIONS_CLOSED = BashOperator(
    task_id='PCF_EBU_GROUP_ESCALATIONS_CLOSED',
    bash_command= pcf_command_EBU_GROUP_ESCALATIONS_CLOSED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_EBU_GROUP_ESCALATIONS_CLOSED = BashOperator(
    task_id='Dedup_EBU_GROUP_ESCALATIONS_CLOSED',
    bash_command= dedup_command_EBU_GROUP_ESCALATIONS_CLOSED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_EBU_GROUP_ESCALATIONS_CLOSED = BashOperator(
    task_id='Dedup2_EBU_GROUP_ESCALATIONS_CLOSED',
    bash_command= dedup_command_EBU_GROUP_ESCALATIONS_CLOSED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_EBU_GROUP_ESCALATIONS_CLOSED = BashOperator(
    task_id='PCFCheck_EBU_GROUP_ESCALATIONS_CLOSED',
    bash_command=PCFCheck_command_EBU_GROUP_ESCALATIONS_CLOSED,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_EBU_GROUP_ESCALATIONS_CLOSED = EmailOperator(
    task_id='faile_EBU_GROUP_ESCALATIONS_CLOSED',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_EBU_GROUP_ESCALATIONS_CLOSED),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_EBU_GROUP_ESCALATIONS_CLOSED: PythonOperator = PythonOperator(task_id="waitForFlush_EBU_GROUP_ESCALATIONS_CLOSED",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_EBU_GROUP_ESCALATIONS_CLOSED >> PCF_EBU_GROUP_ESCALATIONS_CLOSED >> PCFCheck_EBU_GROUP_ESCALATIONS_CLOSED >> delay_python_task_EBU_GROUP_ESCALATIONS_CLOSED >> Dedup_EBU_GROUP_ESCALATIONS_CLOSED >> Validation_End 
Branching_EBU_GROUP_ESCALATIONS_CLOSED >> Dedup2_EBU_GROUP_ESCALATIONS_CLOSED >> Validation_End
Branching_EBU_GROUP_ESCALATIONS_CLOSED >> faile_EBU_GROUP_ESCALATIONS_CLOSED
Branching_EBU_GROUP_ESCALATIONS_CLOSED >> Validation_End


feedname_MNP_PORTING_BROADCAST = 'MNP_PORTING_BROADCAST'.upper()
feedname2_MNP_PORTING_BROADCAST = 'MNP_PORTING_BROADCAST'.lower()

dedup_command_MNP_PORTING_BROADCAST="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MNP_PORTING_BROADCAST)
logging.info(dedup_command_MNP_PORTING_BROADCAST) 

pcf_command_MNP_PORTING_BROADCAST = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MNP_PORTING_BROADCAST)
logging.info(pcf_command_MNP_PORTING_BROADCAST)

PCFCheck_command_MNP_PORTING_BROADCAST = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MNP_PORTING_BROADCAST,run_date,sleeptime)
logging.info(PCFCheck_command_MNP_PORTING_BROADCAST)

branchScript_MNP_PORTING_BROADCAST = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MNP_PORTING_BROADCAST,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MNP_PORTING_BROADCAST)

def branch_MNP_PORTING_BROADCAST():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MNP_PORTING_BROADCAST,feedname2_MNP_PORTING_BROADCAST)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MNP_PORTING_BROADCAST)
    x = readcsv(filepath,feedname2_MNP_PORTING_BROADCAST)
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

delay_python_task_MNP_PORTING_BROADCAST: PythonOperator = PythonOperator(task_id="waitForFlush_MNP_PORTING_BROADCAST",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MNP_PORTING_BROADCAST >> PCF_MNP_PORTING_BROADCAST >> PCFCheck_MNP_PORTING_BROADCAST >> delay_python_task_MNP_PORTING_BROADCAST >> Dedup_MNP_PORTING_BROADCAST >> Validation_End 
Branching_MNP_PORTING_BROADCAST >> Dedup2_MNP_PORTING_BROADCAST >> Validation_End
Branching_MNP_PORTING_BROADCAST >> faile_MNP_PORTING_BROADCAST
Branching_MNP_PORTING_BROADCAST >> Validation_End


feedname_PREPAID_PAYMENTS_LOG = 'PREPAID_PAYMENTS_LOG'.upper()
feedname2_PREPAID_PAYMENTS_LOG = 'PREPAID_PAYMENTS_LOG'.lower()

dedup_command_PREPAID_PAYMENTS_LOG="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_PREPAID_PAYMENTS_LOG)
logging.info(dedup_command_PREPAID_PAYMENTS_LOG) 

pcf_command_PREPAID_PAYMENTS_LOG = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_PREPAID_PAYMENTS_LOG)
logging.info(pcf_command_PREPAID_PAYMENTS_LOG)

PCFCheck_command_PREPAID_PAYMENTS_LOG = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_PREPAID_PAYMENTS_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_PREPAID_PAYMENTS_LOG)

branchScript_PREPAID_PAYMENTS_LOG = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_PREPAID_PAYMENTS_LOG,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_PREPAID_PAYMENTS_LOG)

def branch_PREPAID_PAYMENTS_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_PREPAID_PAYMENTS_LOG,feedname2_PREPAID_PAYMENTS_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_PREPAID_PAYMENTS_LOG)
    x = readcsv(filepath,feedname2_PREPAID_PAYMENTS_LOG)
    logging.info('branch_PREPAID_PAYMENTS_LOG' + x)
    return x

Branching_PREPAID_PAYMENTS_LOG = BranchPythonOperator(
    task_id='branchid_PREPAID_PAYMENTS_LOG',
    python_callable=branch_PREPAID_PAYMENTS_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_PREPAID_PAYMENTS_LOG = BashOperator(
    task_id='PCF_PREPAID_PAYMENTS_LOG',
    bash_command= pcf_command_PREPAID_PAYMENTS_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_PREPAID_PAYMENTS_LOG = BashOperator(
    task_id='Dedup_PREPAID_PAYMENTS_LOG',
    bash_command= dedup_command_PREPAID_PAYMENTS_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_PREPAID_PAYMENTS_LOG = BashOperator(
    task_id='Dedup2_PREPAID_PAYMENTS_LOG',
    bash_command= dedup_command_PREPAID_PAYMENTS_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_PREPAID_PAYMENTS_LOG = BashOperator(
    task_id='PCFCheck_PREPAID_PAYMENTS_LOG',
    bash_command=PCFCheck_command_PREPAID_PAYMENTS_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_PREPAID_PAYMENTS_LOG = EmailOperator(
    task_id='faile_PREPAID_PAYMENTS_LOG',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_PREPAID_PAYMENTS_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_PREPAID_PAYMENTS_LOG: PythonOperator = PythonOperator(task_id="waitForFlush_PREPAID_PAYMENTS_LOG",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_PREPAID_PAYMENTS_LOG >> PCF_PREPAID_PAYMENTS_LOG >> PCFCheck_PREPAID_PAYMENTS_LOG >> delay_python_task_PREPAID_PAYMENTS_LOG >> Dedup_PREPAID_PAYMENTS_LOG >> Validation_End 
Branching_PREPAID_PAYMENTS_LOG >> Dedup2_PREPAID_PAYMENTS_LOG >> Validation_End
Branching_PREPAID_PAYMENTS_LOG >> faile_PREPAID_PAYMENTS_LOG
Branching_PREPAID_PAYMENTS_LOG >> Validation_End


feedname_AGENT_USER_BIB = 'AGENT_USER_BIB'.upper()
feedname2_AGENT_USER_BIB = 'AGENT_USER_BIB'.lower()

dedup_command_AGENT_USER_BIB="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_AGENT_USER_BIB)
logging.info(dedup_command_AGENT_USER_BIB) 

pcf_command_AGENT_USER_BIB = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_AGENT_USER_BIB)
logging.info(pcf_command_AGENT_USER_BIB)

PCFCheck_command_AGENT_USER_BIB = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_AGENT_USER_BIB,run_date,sleeptime)
logging.info(PCFCheck_command_AGENT_USER_BIB)

branchScript_AGENT_USER_BIB = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_AGENT_USER_BIB,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_AGENT_USER_BIB)

def branch_AGENT_USER_BIB():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_AGENT_USER_BIB,feedname2_AGENT_USER_BIB)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_AGENT_USER_BIB)
    x = readcsv(filepath,feedname2_AGENT_USER_BIB)
    logging.info('branch_AGENT_USER_BIB' + x)
    return x

Branching_AGENT_USER_BIB = BranchPythonOperator(
    task_id='branchid_AGENT_USER_BIB',
    python_callable=branch_AGENT_USER_BIB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_AGENT_USER_BIB = BashOperator(
    task_id='PCF_AGENT_USER_BIB',
    bash_command= pcf_command_AGENT_USER_BIB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_AGENT_USER_BIB = BashOperator(
    task_id='Dedup_AGENT_USER_BIB',
    bash_command= dedup_command_AGENT_USER_BIB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_AGENT_USER_BIB = BashOperator(
    task_id='Dedup2_AGENT_USER_BIB',
    bash_command= dedup_command_AGENT_USER_BIB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_AGENT_USER_BIB = BashOperator(
    task_id='PCFCheck_AGENT_USER_BIB',
    bash_command=PCFCheck_command_AGENT_USER_BIB,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_AGENT_USER_BIB = EmailOperator(
    task_id='faile_AGENT_USER_BIB',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_AGENT_USER_BIB),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_AGENT_USER_BIB: PythonOperator = PythonOperator(task_id="waitForFlush_AGENT_USER_BIB",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_AGENT_USER_BIB >> PCF_AGENT_USER_BIB >> PCFCheck_AGENT_USER_BIB >> delay_python_task_AGENT_USER_BIB >> Dedup_AGENT_USER_BIB >> Validation_End 
Branching_AGENT_USER_BIB >> Dedup2_AGENT_USER_BIB >> Validation_End
Branching_AGENT_USER_BIB >> faile_AGENT_USER_BIB
Branching_AGENT_USER_BIB >> Validation_End


feedname_UT_DUMP = 'UT_DUMP'.upper()
feedname2_UT_DUMP = 'UT_DUMP'.lower()

dedup_command_UT_DUMP="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_UT_DUMP)
logging.info(dedup_command_UT_DUMP) 

pcf_command_UT_DUMP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_UT_DUMP)
logging.info(pcf_command_UT_DUMP)

PCFCheck_command_UT_DUMP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_UT_DUMP,run_date,sleeptime)
logging.info(PCFCheck_command_UT_DUMP)

branchScript_UT_DUMP = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_UT_DUMP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_UT_DUMP)

def branch_UT_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_UT_DUMP,feedname2_UT_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_UT_DUMP)
    x = readcsv(filepath,feedname2_UT_DUMP)
    logging.info('branch_UT_DUMP' + x)
    return x

Branching_UT_DUMP = BranchPythonOperator(
    task_id='branchid_UT_DUMP',
    python_callable=branch_UT_DUMP,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCF_UT_DUMP = BashOperator(
    task_id='PCF_UT_DUMP',
    bash_command= pcf_command_UT_DUMP,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_UT_DUMP = BashOperator(
    task_id='Dedup_UT_DUMP',
    bash_command= dedup_command_UT_DUMP,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_UT_DUMP = BashOperator(
    task_id='Dedup2_UT_DUMP',
    bash_command= dedup_command_UT_DUMP,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_UT_DUMP = BashOperator(
    task_id='PCFCheck_UT_DUMP',
    bash_command=PCFCheck_command_UT_DUMP,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_UT_DUMP = EmailOperator(
    task_id='faile_UT_DUMP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_UT_DUMP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_UT_DUMP: PythonOperator = PythonOperator(task_id="waitForFlush_UT_DUMP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_UT_DUMP >> PCF_UT_DUMP >> PCFCheck_UT_DUMP >> delay_python_task_UT_DUMP >> Dedup_UT_DUMP >> Validation_End 
Branching_UT_DUMP >> Dedup2_UT_DUMP >> Validation_End
Branching_UT_DUMP >> faile_UT_DUMP
Branching_UT_DUMP >> Validation_End
