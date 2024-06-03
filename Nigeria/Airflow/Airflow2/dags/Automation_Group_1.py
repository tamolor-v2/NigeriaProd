import airflow
import os
import csv
import os.path
import logging
from airflow.models import Variable
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
d_1 = Variable.get("rerunDate", deserialize_json=True)
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

dag = DAG('New_Automation_Group_1', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group1 -c config.json' %(d_1)
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
run_as_user='daasuser',
dag=dag,
priority_weight=100
)

'''
feedname_CS6_CCN_CDR = 'CS6_CCN_CDR'.upper()
feedname2_CS6_CCN_CDR = 'CS6_CCN_CDR'.lower()

#CCN_CDR_GPRS,CCN_CDR_SMS,CCN_CDR_VOICE

dedup_command_CS6_CCN_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS6_CCN_CDR)
logging.info(dedup_command_CS6_CCN_CDR) 

pcf_command_CS6_CCN_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS6_CCN_CDR)
logging.info(pcf_command_CS6_CCN_CDR)

PCFCheck_command_CS6_CCN_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS6_CCN_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_CS6_CCN_CDR)

branchScript_CS6_CCN_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS6_CCN_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS6_CCN_CDR)

def branch_CS6_CCN_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS6_CCN_CDR,feedname2_CS6_CCN_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS6_CCN_CDR)
    x = readcsv(filepath,feedname2_CS6_CCN_CDR)
    logging.info('branch_CS6_CCN_CDR' + x)
    return x

Branching_CS6_CCN_CDR = BranchPythonOperator(
    task_id='branchid_CS6_CCN_CDR',
    python_callable=branch_CS6_CCN_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)
#CCN_CDR_GPRS,CCN_CDR_SMS,CCN_CDR_VOICE
PCF_CS6_CCN_CDR = BashOperator(
    task_id='PCF_CS6_CCN_CDR',
    bash_command= pcf_command_CS6_CCN_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS6_CCN_CDR = BashOperator(
    task_id='Dedup_CS6_CCN_CDR',
    bash_command= dedup_command_CS6_CCN_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS6_CCN_CDR = BashOperator(
    task_id='Dedup2_CS6_CCN_CDR',
    bash_command= dedup_command_CS6_CCN_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS6_CCN_CDR = BashOperator(
    task_id='PCFCheck_CS6_CCN_CDR',
    bash_command=PCFCheck_command_CS6_CCN_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS6_CCN_CDR = EmailOperator(
    task_id='faile_CS6_CCN_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS6_CCN_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS6_CCN_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_CS6_CCN_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS6_CCN_CDR >> PCF_CS6_CCN_CDR >> PCFCheck_CS6_CCN_CDR >> delay_python_task_CS6_CCN_CDR >> Dedup_CS6_CCN_CDR
Branching_CS6_CCN_CDR >> Dedup2_CS6_CCN_CDR
Branching_CS6_CCN_CDR >> faile_CS6_CCN_CDR
Branching_CS6_CCN_CDR >> Validation_End
'''
feedname_CS6_AIR_CDR = 'CS6_AIR_CDR'.upper()
feedname2_CS6_AIR_CDR = 'CS6_AIR_CDR'.lower()


dedup_command_CS6_AIR_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS6_AIR_CDR)
logging.info(dedup_command_CS6_AIR_CDR) 

pcf_command_CS6_AIR_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS6_AIR_CDR)
logging.info(pcf_command_CS6_AIR_CDR)

PCFCheck_command_CS6_AIR_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS6_AIR_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_CS6_AIR_CDR)

branchScript_CS6_AIR_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS6_AIR_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS6_AIR_CDR)

def branch_CS6_AIR_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS6_AIR_CDR,feedname2_CS6_AIR_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS6_AIR_CDR)
    x = readcsv(filepath,feedname2_CS6_AIR_CDR)
    logging.info('branch_CS6_AIR_CDR' + x)
    return x

Branching_CS6_AIR_CDR = BranchPythonOperator(
    task_id='branchid_CS6_AIR_CDR',
    python_callable=branch_CS6_AIR_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS6_AIR_CDR = BashOperator(
    task_id='PCF_CS6_AIR_CDR',
    bash_command= pcf_command_CS6_AIR_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS6_AIR_CDR = BashOperator(
    task_id='Dedup_CS6_AIR_CDR',
    bash_command= dedup_command_CS6_AIR_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS6_AIR_CDR = BashOperator(
    task_id='Dedup2_CS6_AIR_CDR',
    bash_command= dedup_command_CS6_AIR_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS6_AIR_CDR = BashOperator(
    task_id='PCFCheck_CS6_AIR_CDR',
    bash_command=PCFCheck_command_CS6_AIR_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS6_AIR_CDR = EmailOperator(
    task_id='faile_CS6_AIR_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS6_AIR_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS6_AIR_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_CS6_AIR_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS6_AIR_CDR >> PCF_CS6_AIR_CDR >> PCFCheck_CS6_AIR_CDR >> delay_python_task_CS6_AIR_CDR >> Dedup_CS6_AIR_CDR
Branching_CS6_AIR_CDR >> Dedup2_CS6_AIR_CDR
Branching_CS6_AIR_CDR >> faile_CS6_AIR_CDR
Branching_CS6_AIR_CDR >> Validation_End

feedname_CS6_SDP_CDR = 'CS6_SDP_CDR'.upper()
feedname2_CS6_SDP_CDR = 'CS6_SDP_CDR'.lower()

dedup_command_CS6_SDP_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS6_SDP_CDR)
logging.info(dedup_command_CS6_SDP_CDR) 

pcf_command_CS6_SDP_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS6_SDP_CDR)
logging.info(pcf_command_CS6_SDP_CDR)

PCFCheck_command_CS6_SDP_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS6_SDP_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_CS6_SDP_CDR)

branchScript_CS6_SDP_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS6_SDP_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS6_SDP_CDR)

def branch_CS6_SDP_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS6_SDP_CDR,feedname2_CS6_SDP_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS6_SDP_CDR)
    x = readcsv(filepath,feedname2_CS6_SDP_CDR)
    logging.info('branch_CS6_SDP_CDR' + x)
    return x

Branching_CS6_SDP_CDR = BranchPythonOperator(
    task_id='branchid_CS6_SDP_CDR',
    python_callable=branch_CS6_SDP_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS6_SDP_CDR = BashOperator(
    task_id='PCF_CS6_SDP_CDR',
    bash_command= pcf_command_CS6_SDP_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS6_SDP_CDR = BashOperator(
    task_id='Dedup_CS6_SDP_CDR',
    bash_command= dedup_command_CS6_SDP_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS6_SDP_CDR = BashOperator(
    task_id='Dedup2_CS6_SDP_CDR',
    bash_command= dedup_command_CS6_SDP_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS6_SDP_CDR = BashOperator(
    task_id='PCFCheck_CS6_SDP_CDR',
    bash_command=PCFCheck_command_CS6_SDP_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS6_SDP_CDR = EmailOperator(
    task_id='faile_CS6_SDP_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS6_SDP_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS6_SDP_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_CS6_SDP_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS6_SDP_CDR >> PCF_CS6_SDP_CDR >> PCFCheck_CS6_SDP_CDR >> delay_python_task_CS6_SDP_CDR >> Dedup_CS6_SDP_CDR
Branching_CS6_SDP_CDR >> Dedup2_CS6_SDP_CDR
Branching_CS6_SDP_CDR >> faile_CS6_SDP_CDR
Branching_CS6_SDP_CDR >> Validation_End

feedname_BUNDLE4U_GPRS = 'BUNDLE4U_GPRS'.upper()
feedname2_BUNDLE4U_GPRS = 'BUNDLE4U_GPRS'.lower()

dedup_command_BUNDLE4U_GPRS="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_BUNDLE4U_GPRS)
logging.info(dedup_command_BUNDLE4U_GPRS) 

pcf_command_BUNDLE4U_GPRS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_BUNDLE4U_GPRS)
logging.info(pcf_command_BUNDLE4U_GPRS)

PCFCheck_command_BUNDLE4U_GPRS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BUNDLE4U_GPRS,run_date,sleeptime)
logging.info(PCFCheck_command_BUNDLE4U_GPRS)

branchScript_BUNDLE4U_GPRS = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BUNDLE4U_GPRS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BUNDLE4U_GPRS)

def branch_BUNDLE4U_GPRS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BUNDLE4U_GPRS,feedname2_BUNDLE4U_GPRS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BUNDLE4U_GPRS)
    x = readcsv(filepath,feedname2_BUNDLE4U_GPRS)
    logging.info('branch_BUNDLE4U_GPRS' + x)
    return x

Branching_BUNDLE4U_GPRS = BranchPythonOperator(
    task_id='branchid_BUNDLE4U_GPRS',
    python_callable=branch_BUNDLE4U_GPRS,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_BUNDLE4U_GPRS = BashOperator(
    task_id='PCF_BUNDLE4U_GPRS',
    bash_command= pcf_command_BUNDLE4U_GPRS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_BUNDLE4U_GPRS = BashOperator(
    task_id='Dedup_BUNDLE4U_GPRS',
    bash_command= dedup_command_BUNDLE4U_GPRS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_BUNDLE4U_GPRS = BashOperator(
    task_id='Dedup2_BUNDLE4U_GPRS',
    bash_command= dedup_command_BUNDLE4U_GPRS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_BUNDLE4U_GPRS = BashOperator(
    task_id='PCFCheck_BUNDLE4U_GPRS',
    bash_command=PCFCheck_command_BUNDLE4U_GPRS,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_BUNDLE4U_GPRS = EmailOperator(
    task_id='faile_BUNDLE4U_GPRS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BUNDLE4U_GPRS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BUNDLE4U_GPRS: PythonOperator = PythonOperator(task_id="waitForFlush_BUNDLE4U_GPRS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BUNDLE4U_GPRS >> PCF_BUNDLE4U_GPRS >> PCFCheck_BUNDLE4U_GPRS >> delay_python_task_BUNDLE4U_GPRS >> Dedup_BUNDLE4U_GPRS
Branching_BUNDLE4U_GPRS >> Dedup2_BUNDLE4U_GPRS
Branching_BUNDLE4U_GPRS >> faile_BUNDLE4U_GPRS
Branching_BUNDLE4U_GPRS >> Validation_End

feedname_BUNDLE4U_VOICE = 'BUNDLE4U_VOICE'.upper()
feedname2_BUNDLE4U_VOICE = 'BUNDLE4U_VOICE'.lower()

dedup_command_BUNDLE4U_VOICE="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_BUNDLE4U_VOICE)
logging.info(dedup_command_BUNDLE4U_VOICE) 

pcf_command_BUNDLE4U_VOICE = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_BUNDLE4U_VOICE)
logging.info(pcf_command_BUNDLE4U_VOICE)

PCFCheck_command_BUNDLE4U_VOICE = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_BUNDLE4U_VOICE,run_date,sleeptime)
logging.info(PCFCheck_command_BUNDLE4U_VOICE)

branchScript_BUNDLE4U_VOICE = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_BUNDLE4U_VOICE,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_BUNDLE4U_VOICE)

def branch_BUNDLE4U_VOICE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_BUNDLE4U_VOICE,feedname2_BUNDLE4U_VOICE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_BUNDLE4U_VOICE)
    x = readcsv(filepath,feedname2_BUNDLE4U_VOICE)
    logging.info('branch_BUNDLE4U_VOICE' + x)
    return x

Branching_BUNDLE4U_VOICE = BranchPythonOperator(
    task_id='branchid_BUNDLE4U_VOICE',
    python_callable=branch_BUNDLE4U_VOICE,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_BUNDLE4U_VOICE = BashOperator(
    task_id='PCF_BUNDLE4U_VOICE',
    bash_command= pcf_command_BUNDLE4U_VOICE,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_BUNDLE4U_VOICE = BashOperator(
    task_id='Dedup_BUNDLE4U_VOICE',
    bash_command= dedup_command_BUNDLE4U_VOICE,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_BUNDLE4U_VOICE = BashOperator(
    task_id='Dedup2_BUNDLE4U_VOICE',
    bash_command= dedup_command_BUNDLE4U_VOICE,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_BUNDLE4U_VOICE = BashOperator(
    task_id='PCFCheck_BUNDLE4U_VOICE',
    bash_command=PCFCheck_command_BUNDLE4U_VOICE,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_BUNDLE4U_VOICE = EmailOperator(
    task_id='faile_BUNDLE4U_VOICE',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_BUNDLE4U_VOICE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_BUNDLE4U_VOICE: PythonOperator = PythonOperator(task_id="waitForFlush_BUNDLE4U_VOICE",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_BUNDLE4U_VOICE >> PCF_BUNDLE4U_VOICE >> PCFCheck_BUNDLE4U_VOICE >> delay_python_task_BUNDLE4U_VOICE >> Dedup_BUNDLE4U_VOICE
Branching_BUNDLE4U_VOICE >> Dedup2_BUNDLE4U_VOICE
Branching_BUNDLE4U_VOICE >> faile_BUNDLE4U_VOICE
Branching_BUNDLE4U_VOICE >> Validation_End

feedname_CS5_AIR_ADJ_MA = 'CS5_AIR_ADJ_MA'.upper()
feedname2_CS5_AIR_ADJ_MA = 'CS5_AIR_ADJ_MA'.lower()

dedup_command_CS5_AIR_ADJ_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS5_AIR_ADJ_MA)
logging.info(dedup_command_CS5_AIR_ADJ_MA) 

pcf_command_CS5_AIR_ADJ_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_AIR_ADJ_MA)
logging.info(pcf_command_CS5_AIR_ADJ_MA)

PCFCheck_command_CS5_AIR_ADJ_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_AIR_ADJ_MA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_AIR_ADJ_MA)

branchScript_CS5_AIR_ADJ_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_AIR_ADJ_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_AIR_ADJ_MA)

def branch_CS5_AIR_ADJ_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_AIR_ADJ_MA,feedname2_CS5_AIR_ADJ_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_AIR_ADJ_MA)
    x = readcsv(filepath,feedname2_CS5_AIR_ADJ_MA)
    logging.info('branch_CS5_AIR_ADJ_MA' + x)
    return x

Branching_CS5_AIR_ADJ_MA = BranchPythonOperator(
    task_id='branchid_CS5_AIR_ADJ_MA',
    python_callable=branch_CS5_AIR_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS5_AIR_ADJ_MA = BashOperator(
    task_id='PCF_CS5_AIR_ADJ_MA',
    bash_command= pcf_command_CS5_AIR_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS5_AIR_ADJ_MA = BashOperator(
    task_id='Dedup_CS5_AIR_ADJ_MA',
    bash_command= dedup_command_CS5_AIR_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS5_AIR_ADJ_MA = BashOperator(
    task_id='Dedup2_CS5_AIR_ADJ_MA',
    bash_command= dedup_command_CS5_AIR_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS5_AIR_ADJ_MA = BashOperator(
    task_id='PCFCheck_CS5_AIR_ADJ_MA',
    bash_command=PCFCheck_command_CS5_AIR_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS5_AIR_ADJ_MA = EmailOperator(
    task_id='faile_CS5_AIR_ADJ_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_AIR_ADJ_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_AIR_ADJ_MA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_AIR_ADJ_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_AIR_ADJ_MA >> PCF_CS5_AIR_ADJ_MA >> PCFCheck_CS5_AIR_ADJ_MA >> delay_python_task_CS5_AIR_ADJ_MA >> Dedup_CS5_AIR_ADJ_MA
Branching_CS5_AIR_ADJ_MA >> Dedup2_CS5_AIR_ADJ_MA
Branching_CS5_AIR_ADJ_MA >> faile_CS5_AIR_ADJ_MA
Branching_CS5_AIR_ADJ_MA >> Validation_End

feedname_CS5_CCN_GPRS_MA = 'CS5_CCN_GPRS_MA'.upper()
feedname2_CS5_CCN_GPRS_MA = 'CS5_CCN_GPRS_MA'.lower()

dedup_command_CS5_CCN_GPRS_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_GPRS_MA)
logging.info(dedup_command_CS5_CCN_GPRS_MA) 

pcf_command_CS5_CCN_GPRS_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_GPRS_MA)
logging.info(pcf_command_CS5_CCN_GPRS_MA)

PCFCheck_command_CS5_CCN_GPRS_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_GPRS_MA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_GPRS_MA)

branchScript_CS5_CCN_GPRS_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_GPRS_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_GPRS_MA)

def branch_CS5_CCN_GPRS_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_GPRS_MA,feedname2_CS5_CCN_GPRS_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_GPRS_MA)
    x = readcsv(filepath,feedname2_CS5_CCN_GPRS_MA)
    logging.info('branch_CS5_CCN_GPRS_MA' + x)
    return x

Branching_CS5_CCN_GPRS_MA = BranchPythonOperator(
    task_id='branchid_CS5_CCN_GPRS_MA',
    python_callable=branch_CS5_CCN_GPRS_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS5_CCN_GPRS_MA = BashOperator(
    task_id='PCF_CS5_CCN_GPRS_MA',
    bash_command= pcf_command_CS5_CCN_GPRS_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS5_CCN_GPRS_MA = BashOperator(
    task_id='Dedup_CS5_CCN_GPRS_MA',
    bash_command= dedup_command_CS5_CCN_GPRS_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS5_CCN_GPRS_MA = BashOperator(
    task_id='Dedup2_CS5_CCN_GPRS_MA',
    bash_command= dedup_command_CS5_CCN_GPRS_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS5_CCN_GPRS_MA = BashOperator(
    task_id='PCFCheck_CS5_CCN_GPRS_MA',
    bash_command=PCFCheck_command_CS5_CCN_GPRS_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS5_CCN_GPRS_MA = EmailOperator(
    task_id='faile_CS5_CCN_GPRS_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_GPRS_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_GPRS_MA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_GPRS_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_GPRS_MA >> PCF_CS5_CCN_GPRS_MA >> PCFCheck_CS5_CCN_GPRS_MA >> delay_python_task_CS5_CCN_GPRS_MA >> Dedup_CS5_CCN_GPRS_MA
Branching_CS5_CCN_GPRS_MA >> Dedup2_CS5_CCN_GPRS_MA
Branching_CS5_CCN_GPRS_MA >> faile_CS5_CCN_GPRS_MA
Branching_CS5_CCN_GPRS_MA >> Validation_End

feedname_CS5_CCN_SMS_MA = 'CS5_CCN_SMS_MA'.upper()
feedname2_CS5_CCN_SMS_MA = 'CS5_CCN_SMS_MA'.lower()

dedup_command_CS5_CCN_SMS_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_SMS_MA)
logging.info(dedup_command_CS5_CCN_SMS_MA) 

pcf_command_CS5_CCN_SMS_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_SMS_MA)
logging.info(pcf_command_CS5_CCN_SMS_MA)

PCFCheck_command_CS5_CCN_SMS_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_SMS_MA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_SMS_MA)

branchScript_CS5_CCN_SMS_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_SMS_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_SMS_MA)

def branch_CS5_CCN_SMS_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_SMS_MA,feedname2_CS5_CCN_SMS_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_SMS_MA)
    x = readcsv(filepath,feedname2_CS5_CCN_SMS_MA)
    logging.info('branch_CS5_CCN_SMS_MA' + x)
    return x

Branching_CS5_CCN_SMS_MA = BranchPythonOperator(
    task_id='branchid_CS5_CCN_SMS_MA',
    python_callable=branch_CS5_CCN_SMS_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS5_CCN_SMS_MA = BashOperator(
    task_id='PCF_CS5_CCN_SMS_MA',
    bash_command= pcf_command_CS5_CCN_SMS_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS5_CCN_SMS_MA = BashOperator(
    task_id='Dedup_CS5_CCN_SMS_MA',
    bash_command= dedup_command_CS5_CCN_SMS_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS5_CCN_SMS_MA = BashOperator(
    task_id='Dedup2_CS5_CCN_SMS_MA',
    bash_command= dedup_command_CS5_CCN_SMS_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS5_CCN_SMS_MA = BashOperator(
    task_id='PCFCheck_CS5_CCN_SMS_MA',
    bash_command=PCFCheck_command_CS5_CCN_SMS_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS5_CCN_SMS_MA = EmailOperator(
    task_id='faile_CS5_CCN_SMS_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_SMS_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_SMS_MA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_SMS_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_SMS_MA >> PCF_CS5_CCN_SMS_MA >> PCFCheck_CS5_CCN_SMS_MA >> delay_python_task_CS5_CCN_SMS_MA >> Dedup_CS5_CCN_SMS_MA
Branching_CS5_CCN_SMS_MA >> Dedup2_CS5_CCN_SMS_MA
Branching_CS5_CCN_SMS_MA >> faile_CS5_CCN_SMS_MA
Branching_CS5_CCN_SMS_MA >> Validation_End

feedname_CS5_SDP_ACC_ADJ_MA = 'CS5_SDP_ACC_ADJ_MA'.upper()
feedname2_CS5_SDP_ACC_ADJ_MA = 'CS5_SDP_ACC_ADJ_MA'.lower()

dedup_command_CS5_SDP_ACC_ADJ_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS5_SDP_ACC_ADJ_MA)
logging.info(dedup_command_CS5_SDP_ACC_ADJ_MA) 

pcf_command_CS5_SDP_ACC_ADJ_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_SDP_ACC_ADJ_MA)
logging.info(pcf_command_CS5_SDP_ACC_ADJ_MA)

PCFCheck_command_CS5_SDP_ACC_ADJ_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_SDP_ACC_ADJ_MA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_SDP_ACC_ADJ_MA)

branchScript_CS5_SDP_ACC_ADJ_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_SDP_ACC_ADJ_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_SDP_ACC_ADJ_MA)

def branch_CS5_SDP_ACC_ADJ_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_SDP_ACC_ADJ_MA,feedname2_CS5_SDP_ACC_ADJ_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_SDP_ACC_ADJ_MA)
    x = readcsv(filepath,feedname2_CS5_SDP_ACC_ADJ_MA)
    logging.info('branch_CS5_SDP_ACC_ADJ_MA' + x)
    return x

Branching_CS5_SDP_ACC_ADJ_MA = BranchPythonOperator(
    task_id='branchid_CS5_SDP_ACC_ADJ_MA',
    python_callable=branch_CS5_SDP_ACC_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS5_SDP_ACC_ADJ_MA = BashOperator(
    task_id='PCF_CS5_SDP_ACC_ADJ_MA',
    bash_command= pcf_command_CS5_SDP_ACC_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS5_SDP_ACC_ADJ_MA = BashOperator(
    task_id='Dedup_CS5_SDP_ACC_ADJ_MA',
    bash_command= dedup_command_CS5_SDP_ACC_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS5_SDP_ACC_ADJ_MA = BashOperator(
    task_id='Dedup2_CS5_SDP_ACC_ADJ_MA',
    bash_command= dedup_command_CS5_SDP_ACC_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS5_SDP_ACC_ADJ_MA = BashOperator(
    task_id='PCFCheck_CS5_SDP_ACC_ADJ_MA',
    bash_command=PCFCheck_command_CS5_SDP_ACC_ADJ_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS5_SDP_ACC_ADJ_MA = EmailOperator(
    task_id='faile_CS5_SDP_ACC_ADJ_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_SDP_ACC_ADJ_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_SDP_ACC_ADJ_MA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_SDP_ACC_ADJ_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_SDP_ACC_ADJ_MA >> PCF_CS5_SDP_ACC_ADJ_MA >> PCFCheck_CS5_SDP_ACC_ADJ_MA >> delay_python_task_CS5_SDP_ACC_ADJ_MA >> Dedup_CS5_SDP_ACC_ADJ_MA
Branching_CS5_SDP_ACC_ADJ_MA >> Dedup2_CS5_SDP_ACC_ADJ_MA
Branching_CS5_SDP_ACC_ADJ_MA >> faile_CS5_SDP_ACC_ADJ_MA
Branching_CS5_SDP_ACC_ADJ_MA >> Validation_End

feedname_DMC_DUMP_ALL = 'DMC_DUMP_ALL'.upper()
feedname2_DMC_DUMP_ALL = 'DMC_DUMP_ALL'.lower()

dedup_command_DMC_DUMP_ALL="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_DMC_DUMP_ALL)
logging.info(dedup_command_DMC_DUMP_ALL) 

pcf_command_DMC_DUMP_ALL = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_DMC_DUMP_ALL)
logging.info(pcf_command_DMC_DUMP_ALL)

PCFCheck_command_DMC_DUMP_ALL = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_DMC_DUMP_ALL,run_date,sleeptime)
logging.info(PCFCheck_command_DMC_DUMP_ALL)

branchScript_DMC_DUMP_ALL = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_DMC_DUMP_ALL,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_DMC_DUMP_ALL)

def branch_DMC_DUMP_ALL():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_DMC_DUMP_ALL,feedname2_DMC_DUMP_ALL)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_DMC_DUMP_ALL)
    x = readcsv(filepath,feedname2_DMC_DUMP_ALL)
    logging.info('branch_DMC_DUMP_ALL' + x)
    return x

Branching_DMC_DUMP_ALL = BranchPythonOperator(
    task_id='branchid_DMC_DUMP_ALL',
    python_callable=branch_DMC_DUMP_ALL,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_DMC_DUMP_ALL = BashOperator(
    task_id='PCF_DMC_DUMP_ALL',
    bash_command= pcf_command_DMC_DUMP_ALL,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_DMC_DUMP_ALL = BashOperator(
    task_id='Dedup_DMC_DUMP_ALL',
    bash_command= dedup_command_DMC_DUMP_ALL,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_DMC_DUMP_ALL = BashOperator(
    task_id='Dedup2_DMC_DUMP_ALL',
    bash_command= dedup_command_DMC_DUMP_ALL,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_DMC_DUMP_ALL = BashOperator(
    task_id='PCFCheck_DMC_DUMP_ALL',
    bash_command=PCFCheck_command_DMC_DUMP_ALL,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_DMC_DUMP_ALL = EmailOperator(
    task_id='faile_DMC_DUMP_ALL',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_DMC_DUMP_ALL),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_DMC_DUMP_ALL: PythonOperator = PythonOperator(task_id="waitForFlush_DMC_DUMP_ALL",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_DMC_DUMP_ALL >> PCF_DMC_DUMP_ALL >> PCFCheck_DMC_DUMP_ALL >> delay_python_task_DMC_DUMP_ALL >> Dedup_DMC_DUMP_ALL
Branching_DMC_DUMP_ALL >> Dedup2_DMC_DUMP_ALL
Branching_DMC_DUMP_ALL >> faile_DMC_DUMP_ALL
Branching_DMC_DUMP_ALL >> Validation_End

feedname_MSC_CDR = 'MSC_CDR'.upper()
feedname2_MSC_CDR = 'MSC_CDR'.lower()

dedup_command_MSC_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_MSC_CDR)
logging.info(dedup_command_MSC_CDR) 

pcf_command_MSC_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_MSC_CDR)
logging.info(pcf_command_MSC_CDR)

PCFCheck_command_MSC_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSC_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_MSC_CDR)

branchScript_MSC_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MSC_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MSC_CDR)

def branch_MSC_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSC_CDR,feedname2_MSC_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSC_CDR)
    x = readcsv(filepath,feedname2_MSC_CDR)
    logging.info('branch_MSC_CDR' + x)
    return x

Branching_MSC_CDR = BranchPythonOperator(
    task_id='branchid_MSC_CDR',
    python_callable=branch_MSC_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_MSC_CDR = BashOperator(
    task_id='PCF_MSC_CDR',
    bash_command= pcf_command_MSC_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_MSC_CDR = BashOperator(
    task_id='Dedup_MSC_CDR',
    bash_command= dedup_command_MSC_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_MSC_CDR = BashOperator(
    task_id='Dedup2_MSC_CDR',
    bash_command= dedup_command_MSC_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_MSC_CDR = BashOperator(
    task_id='PCFCheck_MSC_CDR',
    bash_command=PCFCheck_command_MSC_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_MSC_CDR = EmailOperator(
    task_id='faile_MSC_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSC_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MSC_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_MSC_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MSC_CDR >> PCF_MSC_CDR >> PCFCheck_MSC_CDR >> delay_python_task_MSC_CDR >> Dedup_MSC_CDR
Branching_MSC_CDR >> Dedup2_MSC_CDR
Branching_MSC_CDR >> faile_MSC_CDR
Branching_MSC_CDR >> Validation_End

feedname_SDP_DMP_MA = 'SDP_DMP_MA'.upper()
feedname2_SDP_DMP_MA = 'SDP_DMP_MA'.lower()

dedup_command_SDP_DMP_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_SDP_DMP_MA)
logging.info(dedup_command_SDP_DMP_MA) 

pcf_command_SDP_DMP_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SDP_DMP_MA)
logging.info(pcf_command_SDP_DMP_MA)

PCFCheck_command_SDP_DMP_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SDP_DMP_MA,run_date,sleeptime)
logging.info(PCFCheck_command_SDP_DMP_MA)

branchScript_SDP_DMP_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SDP_DMP_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SDP_DMP_MA)

def branch_SDP_DMP_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SDP_DMP_MA,feedname2_SDP_DMP_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SDP_DMP_MA)
    x = readcsv(filepath,feedname2_SDP_DMP_MA)
    logging.info('branch_SDP_DMP_MA' + x)
    return x

Branching_SDP_DMP_MA = BranchPythonOperator(
    task_id='branchid_SDP_DMP_MA',
    python_callable=branch_SDP_DMP_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_SDP_DMP_MA = BashOperator(
    task_id='PCF_SDP_DMP_MA',
    bash_command= pcf_command_SDP_DMP_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_SDP_DMP_MA = BashOperator(
    task_id='Dedup_SDP_DMP_MA',
    bash_command= dedup_command_SDP_DMP_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_SDP_DMP_MA = BashOperator(
    task_id='Dedup2_SDP_DMP_MA',
    bash_command= dedup_command_SDP_DMP_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_SDP_DMP_MA = BashOperator(
    task_id='PCFCheck_SDP_DMP_MA',
    bash_command=PCFCheck_command_SDP_DMP_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_SDP_DMP_MA = EmailOperator(
    task_id='faile_SDP_DMP_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SDP_DMP_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SDP_DMP_MA: PythonOperator = PythonOperator(task_id="waitForFlush_SDP_DMP_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SDP_DMP_MA >> PCF_SDP_DMP_MA >> PCFCheck_SDP_DMP_MA >> delay_python_task_SDP_DMP_MA >> Dedup_SDP_DMP_MA
Branching_SDP_DMP_MA >> Dedup2_SDP_DMP_MA
Branching_SDP_DMP_MA >> faile_SDP_DMP_MA
Branching_SDP_DMP_MA >> Validation_End

feedname_MOBILE_MONEY = 'MOBILE_MONEY'.upper()
feedname2_MOBILE_MONEY = 'MOBILE_MONEY'.lower()

dedup_command_MOBILE_MONEY="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_MOBILE_MONEY)
logging.info(dedup_command_MOBILE_MONEY) 

pcf_command_MOBILE_MONEY = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_MOBILE_MONEY)
logging.info(pcf_command_MOBILE_MONEY)

PCFCheck_command_MOBILE_MONEY = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MOBILE_MONEY,run_date,sleeptime)
logging.info(PCFCheck_command_MOBILE_MONEY)

branchScript_MOBILE_MONEY = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MOBILE_MONEY,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MOBILE_MONEY)

def branch_MOBILE_MONEY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MOBILE_MONEY,feedname2_MOBILE_MONEY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MOBILE_MONEY)
    x = readcsv(filepath,feedname2_MOBILE_MONEY)
    logging.info('branch_MOBILE_MONEY' + x)
    return x

Branching_MOBILE_MONEY = BranchPythonOperator(
    task_id='branchid_MOBILE_MONEY',
    python_callable=branch_MOBILE_MONEY,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_MOBILE_MONEY = BashOperator(
    task_id='PCF_MOBILE_MONEY',
    bash_command= pcf_command_MOBILE_MONEY,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_MOBILE_MONEY = BashOperator(
    task_id='Dedup_MOBILE_MONEY',
    bash_command= dedup_command_MOBILE_MONEY,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_MOBILE_MONEY = BashOperator(
    task_id='Dedup2_MOBILE_MONEY',
    bash_command= dedup_command_MOBILE_MONEY,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_MOBILE_MONEY = BashOperator(
    task_id='PCFCheck_MOBILE_MONEY',
    bash_command=PCFCheck_command_MOBILE_MONEY,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_MOBILE_MONEY = EmailOperator(
    task_id='faile_MOBILE_MONEY',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MOBILE_MONEY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MOBILE_MONEY: PythonOperator = PythonOperator(task_id="waitForFlush_MOBILE_MONEY",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MOBILE_MONEY >> PCF_MOBILE_MONEY >> PCFCheck_MOBILE_MONEY >> delay_python_task_MOBILE_MONEY >> Dedup_MOBILE_MONEY
Branching_MOBILE_MONEY >> Dedup2_MOBILE_MONEY
Branching_MOBILE_MONEY >> faile_MOBILE_MONEY
Branching_MOBILE_MONEY >> Validation_End

feedname_CS5_AIR_REFILL_MA = 'CS5_AIR_REFILL_MA'.upper()
feedname2_CS5_AIR_REFILL_MA = 'CS5_AIR_REFILL_MA'.lower()

dedup_command_CS5_AIR_REFILL_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS5_AIR_REFILL_MA)
logging.info(dedup_command_CS5_AIR_REFILL_MA) 

pcf_command_CS5_AIR_REFILL_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_AIR_REFILL_MA)
logging.info(pcf_command_CS5_AIR_REFILL_MA)

PCFCheck_command_CS5_AIR_REFILL_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_AIR_REFILL_MA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_AIR_REFILL_MA)

branchScript_CS5_AIR_REFILL_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_AIR_REFILL_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_AIR_REFILL_MA)

def branch_CS5_AIR_REFILL_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_AIR_REFILL_MA,feedname2_CS5_AIR_REFILL_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_AIR_REFILL_MA)
    x = readcsv(filepath,feedname2_CS5_AIR_REFILL_MA)
    logging.info('branch_CS5_AIR_REFILL_MA' + x)
    return x

Branching_CS5_AIR_REFILL_MA = BranchPythonOperator(
    task_id='branchid_CS5_AIR_REFILL_MA',
    python_callable=branch_CS5_AIR_REFILL_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS5_AIR_REFILL_MA = BashOperator(
    task_id='PCF_CS5_AIR_REFILL_MA',
    bash_command= pcf_command_CS5_AIR_REFILL_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS5_AIR_REFILL_MA = BashOperator(
    task_id='Dedup_CS5_AIR_REFILL_MA',
    bash_command= dedup_command_CS5_AIR_REFILL_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS5_AIR_REFILL_MA = BashOperator(
    task_id='Dedup2_CS5_AIR_REFILL_MA',
    bash_command= dedup_command_CS5_AIR_REFILL_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS5_AIR_REFILL_MA = BashOperator(
    task_id='PCFCheck_CS5_AIR_REFILL_MA',
    bash_command=PCFCheck_command_CS5_AIR_REFILL_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS5_AIR_REFILL_MA = EmailOperator(
    task_id='faile_CS5_AIR_REFILL_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_AIR_REFILL_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_AIR_REFILL_MA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_AIR_REFILL_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_AIR_REFILL_MA >> PCF_CS5_AIR_REFILL_MA >> PCFCheck_CS5_AIR_REFILL_MA >> delay_python_task_CS5_AIR_REFILL_MA >> Dedup_CS5_AIR_REFILL_MA
Branching_CS5_AIR_REFILL_MA >> Dedup2_CS5_AIR_REFILL_MA
Branching_CS5_AIR_REFILL_MA >> faile_CS5_AIR_REFILL_MA
Branching_CS5_AIR_REFILL_MA >> Validation_End

feedname_CS5_CCN_VOICE_MA = 'CS5_CCN_VOICE_MA'.upper()
feedname2_CS5_CCN_VOICE_MA = 'CS5_CCN_VOICE_MA'.lower()

dedup_command_CS5_CCN_VOICE_MA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CS5_CCN_VOICE_MA)
logging.info(dedup_command_CS5_CCN_VOICE_MA) 

pcf_command_CS5_CCN_VOICE_MA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CS5_CCN_VOICE_MA)
logging.info(pcf_command_CS5_CCN_VOICE_MA)

PCFCheck_command_CS5_CCN_VOICE_MA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CS5_CCN_VOICE_MA,run_date,sleeptime)
logging.info(PCFCheck_command_CS5_CCN_VOICE_MA)

branchScript_CS5_CCN_VOICE_MA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CS5_CCN_VOICE_MA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CS5_CCN_VOICE_MA)

def branch_CS5_CCN_VOICE_MA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CS5_CCN_VOICE_MA,feedname2_CS5_CCN_VOICE_MA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CS5_CCN_VOICE_MA)
    x = readcsv(filepath,feedname2_CS5_CCN_VOICE_MA)
    logging.info('branch_CS5_CCN_VOICE_MA' + x)
    return x

Branching_CS5_CCN_VOICE_MA = BranchPythonOperator(
    task_id='branchid_CS5_CCN_VOICE_MA',
    python_callable=branch_CS5_CCN_VOICE_MA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CS5_CCN_VOICE_MA = BashOperator(
    task_id='PCF_CS5_CCN_VOICE_MA',
    bash_command= pcf_command_CS5_CCN_VOICE_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CS5_CCN_VOICE_MA = BashOperator(
    task_id='Dedup_CS5_CCN_VOICE_MA',
    bash_command= dedup_command_CS5_CCN_VOICE_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CS5_CCN_VOICE_MA = BashOperator(
    task_id='Dedup2_CS5_CCN_VOICE_MA',
    bash_command= dedup_command_CS5_CCN_VOICE_MA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CS5_CCN_VOICE_MA = BashOperator(
    task_id='PCFCheck_CS5_CCN_VOICE_MA',
    bash_command=PCFCheck_command_CS5_CCN_VOICE_MA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CS5_CCN_VOICE_MA = EmailOperator(
    task_id='faile_CS5_CCN_VOICE_MA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CS5_CCN_VOICE_MA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CS5_CCN_VOICE_MA: PythonOperator = PythonOperator(task_id="waitForFlush_CS5_CCN_VOICE_MA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CS5_CCN_VOICE_MA >> PCF_CS5_CCN_VOICE_MA >> PCFCheck_CS5_CCN_VOICE_MA >> delay_python_task_CS5_CCN_VOICE_MA >> Dedup_CS5_CCN_VOICE_MA
Branching_CS5_CCN_VOICE_MA >> Dedup2_CS5_CCN_VOICE_MA
Branching_CS5_CCN_VOICE_MA >> faile_CS5_CCN_VOICE_MA
Branching_CS5_CCN_VOICE_MA >> Validation_End

feedname_SDP_DMP_DA = 'SDP_DMP_DA'.upper()
feedname2_SDP_DMP_DA = 'SDP_DMP_DA'.lower()

dedup_command_SDP_DMP_DA="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_SDP_DMP_DA)
logging.info(dedup_command_SDP_DMP_DA) 

pcf_command_SDP_DMP_DA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_SDP_DMP_DA)
logging.info(pcf_command_SDP_DMP_DA)

PCFCheck_command_SDP_DMP_DA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SDP_DMP_DA,run_date,sleeptime)
logging.info(PCFCheck_command_SDP_DMP_DA)

branchScript_SDP_DMP_DA = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SDP_DMP_DA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SDP_DMP_DA)

def branch_SDP_DMP_DA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SDP_DMP_DA,feedname2_SDP_DMP_DA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SDP_DMP_DA)
    x = readcsv(filepath,feedname2_SDP_DMP_DA)
    logging.info('branch_SDP_DMP_DA' + x)
    return x

Branching_SDP_DMP_DA = BranchPythonOperator(
    task_id='branchid_SDP_DMP_DA',
    python_callable=branch_SDP_DMP_DA,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_SDP_DMP_DA = BashOperator(
    task_id='PCF_SDP_DMP_DA',
    bash_command= pcf_command_SDP_DMP_DA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_SDP_DMP_DA = BashOperator(
    task_id='Dedup_SDP_DMP_DA',
    bash_command= dedup_command_SDP_DMP_DA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_SDP_DMP_DA = BashOperator(
    task_id='Dedup2_SDP_DMP_DA',
    bash_command= dedup_command_SDP_DMP_DA,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_SDP_DMP_DA = BashOperator(
    task_id='PCFCheck_SDP_DMP_DA',
    bash_command=PCFCheck_command_SDP_DMP_DA,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_SDP_DMP_DA = EmailOperator(
    task_id='faile_SDP_DMP_DA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SDP_DMP_DA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SDP_DMP_DA: PythonOperator = PythonOperator(task_id="waitForFlush_SDP_DMP_DA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SDP_DMP_DA >> PCF_SDP_DMP_DA >> PCFCheck_SDP_DMP_DA >> delay_python_task_SDP_DMP_DA >> Dedup_SDP_DMP_DA
Branching_SDP_DMP_DA >> Dedup2_SDP_DMP_DA
Branching_SDP_DMP_DA >> faile_SDP_DMP_DA
Branching_SDP_DMP_DA >> Validation_End

feedname_UC_DUMP = 'UC_DUMP'.upper()
feedname2_UC_DUMP = 'UC_DUMP'.lower()

dedup_command_UC_DUMP="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_UC_DUMP)
logging.info(dedup_command_UC_DUMP) 

pcf_command_UC_DUMP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_UC_DUMP)
logging.info(pcf_command_UC_DUMP)

PCFCheck_command_UC_DUMP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_UC_DUMP,run_date,sleeptime)
logging.info(PCFCheck_command_UC_DUMP)

branchScript_UC_DUMP = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_UC_DUMP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_UC_DUMP)

def branch_UC_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_UC_DUMP,feedname2_UC_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_UC_DUMP)
    x = readcsv(filepath,feedname2_UC_DUMP)
    logging.info('branch_UC_DUMP' + x)
    return x

Branching_UC_DUMP = BranchPythonOperator(
    task_id='branchid_UC_DUMP',
    python_callable=branch_UC_DUMP,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_UC_DUMP = BashOperator(
    task_id='PCF_UC_DUMP',
    bash_command= pcf_command_UC_DUMP,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_UC_DUMP = BashOperator(
    task_id='Dedup_UC_DUMP',
    bash_command= dedup_command_UC_DUMP,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_UC_DUMP = BashOperator(
    task_id='Dedup2_UC_DUMP',
    bash_command= dedup_command_UC_DUMP,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_UC_DUMP = BashOperator(
    task_id='PCFCheck_UC_DUMP',
    bash_command=PCFCheck_command_UC_DUMP,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_UC_DUMP = EmailOperator(
    task_id='faile_UC_DUMP',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_UC_DUMP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_UC_DUMP: PythonOperator = PythonOperator(task_id="waitForFlush_UC_DUMP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_UC_DUMP >> PCF_UC_DUMP >> PCFCheck_UC_DUMP >> delay_python_task_UC_DUMP >> Dedup_UC_DUMP
Branching_UC_DUMP >> Dedup2_UC_DUMP
Branching_UC_DUMP >> faile_UC_DUMP
Branching_UC_DUMP >> Validation_End

feedname_OFFER_DUMP = 'OFFER_DUMP'.upper()
feedname2_OFFER_DUMP = 'OFFER_DUMP'.lower()

dedup_command_OFFER_DUMP="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_OFFER_DUMP)
logging.info(dedup_command_OFFER_DUMP) 

pcf_command_OFFER_DUMP = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_OFFER_DUMP)
logging.info(pcf_command_OFFER_DUMP)

PCFCheck_command_OFFER_DUMP = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_OFFER_DUMP,run_date,sleeptime)
logging.info(PCFCheck_command_OFFER_DUMP)

branchScript_OFFER_DUMP = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_OFFER_DUMP,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_OFFER_DUMP)

def branch_OFFER_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_OFFER_DUMP,feedname2_OFFER_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_OFFER_DUMP)
    x = readcsv(filepath,feedname2_OFFER_DUMP)
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
    queue='edge01002',
	priority_weight=1
)

Dedup_OFFER_DUMP = BashOperator(
    task_id='Dedup_OFFER_DUMP',
    bash_command= dedup_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_OFFER_DUMP = BashOperator(
    task_id='Dedup2_OFFER_DUMP',
    bash_command= dedup_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_OFFER_DUMP = BashOperator(
    task_id='PCFCheck_OFFER_DUMP',
    bash_command=PCFCheck_command_OFFER_DUMP,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
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

delay_python_task_OFFER_DUMP: PythonOperator = PythonOperator(task_id="waitForFlush_OFFER_DUMP",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_OFFER_DUMP >> PCF_OFFER_DUMP >> PCFCheck_OFFER_DUMP >> delay_python_task_OFFER_DUMP >> Dedup_OFFER_DUMP
Branching_OFFER_DUMP >> Dedup2_OFFER_DUMP
Branching_OFFER_DUMP >> faile_OFFER_DUMP
Branching_OFFER_DUMP >> Validation_End

feedname_MVAS_DND_MSISDN_REPORT = 'MVAS_DND_MSISDN_REPORT'.upper()
feedname2_MVAS_DND_MSISDN_REPORT = 'MVAS_DND_MSISDN_REPORT'.lower()

dedup_command_MVAS_DND_MSISDN_REPORT="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_MVAS_DND_MSISDN_REPORT)
logging.info(dedup_command_MVAS_DND_MSISDN_REPORT) 

pcf_command_MVAS_DND_MSISDN_REPORT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_MVAS_DND_MSISDN_REPORT)
logging.info(pcf_command_MVAS_DND_MSISDN_REPORT)

PCFCheck_command_MVAS_DND_MSISDN_REPORT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MVAS_DND_MSISDN_REPORT,run_date,sleeptime)
logging.info(PCFCheck_command_MVAS_DND_MSISDN_REPORT)

branchScript_MVAS_DND_MSISDN_REPORT = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MVAS_DND_MSISDN_REPORT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MVAS_DND_MSISDN_REPORT)

def branch_MVAS_DND_MSISDN_REPORT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MVAS_DND_MSISDN_REPORT,feedname2_MVAS_DND_MSISDN_REPORT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MVAS_DND_MSISDN_REPORT)
    x = readcsv(filepath,feedname2_MVAS_DND_MSISDN_REPORT)
    logging.info('branch_MVAS_DND_MSISDN_REPORT' + x)
    return x

Branching_MVAS_DND_MSISDN_REPORT = BranchPythonOperator(
    task_id='branchid_MVAS_DND_MSISDN_REPORT',
    python_callable=branch_MVAS_DND_MSISDN_REPORT,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_MVAS_DND_MSISDN_REPORT = BashOperator(
    task_id='PCF_MVAS_DND_MSISDN_REPORT',
    bash_command= pcf_command_MVAS_DND_MSISDN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_MVAS_DND_MSISDN_REPORT = BashOperator(
    task_id='Dedup_MVAS_DND_MSISDN_REPORT',
    bash_command= dedup_command_MVAS_DND_MSISDN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_MVAS_DND_MSISDN_REPORT = BashOperator(
    task_id='Dedup2_MVAS_DND_MSISDN_REPORT',
    bash_command= dedup_command_MVAS_DND_MSISDN_REPORT,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_MVAS_DND_MSISDN_REPORT = BashOperator(
    task_id='PCFCheck_MVAS_DND_MSISDN_REPORT',
    bash_command=PCFCheck_command_MVAS_DND_MSISDN_REPORT,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_MVAS_DND_MSISDN_REPORT = EmailOperator(
    task_id='faile_MVAS_DND_MSISDN_REPORT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MVAS_DND_MSISDN_REPORT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MVAS_DND_MSISDN_REPORT: PythonOperator = PythonOperator(task_id="waitForFlush_MVAS_DND_MSISDN_REPORT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MVAS_DND_MSISDN_REPORT >> PCF_MVAS_DND_MSISDN_REPORT >> PCFCheck_MVAS_DND_MSISDN_REPORT >> delay_python_task_MVAS_DND_MSISDN_REPORT >> Dedup_MVAS_DND_MSISDN_REPORT
Branching_MVAS_DND_MSISDN_REPORT >> Dedup2_MVAS_DND_MSISDN_REPORT
Branching_MVAS_DND_MSISDN_REPORT >> faile_MVAS_DND_MSISDN_REPORT
Branching_MVAS_DND_MSISDN_REPORT >> Validation_End

feedname_USSD_CDR = 'USSD_CDR'.upper()
feedname2_USSD_CDR = 'USSD_CDR'.lower()

dedup_command_USSD_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_USSD_CDR)
logging.info(dedup_command_USSD_CDR) 

pcf_command_USSD_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_USSD_CDR)
logging.info(pcf_command_USSD_CDR)

PCFCheck_command_USSD_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_USSD_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_USSD_CDR)

branchScript_USSD_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_USSD_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_USSD_CDR)

def branch_USSD_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_USSD_CDR,feedname2_USSD_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_USSD_CDR)
    x = readcsv(filepath,feedname2_USSD_CDR)
    logging.info('branch_USSD_CDR' + x)
    return x

Branching_USSD_CDR = BranchPythonOperator(
    task_id='branchid_USSD_CDR',
    python_callable=branch_USSD_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_USSD_CDR = BashOperator(
    task_id='PCF_USSD_CDR',
    bash_command= pcf_command_USSD_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_USSD_CDR = BashOperator(
    task_id='Dedup_USSD_CDR',
    bash_command= dedup_command_USSD_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_USSD_CDR = BashOperator(
    task_id='Dedup2_USSD_CDR',
    bash_command= dedup_command_USSD_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_USSD_CDR = BashOperator(
    task_id='PCFCheck_USSD_CDR',
    bash_command=PCFCheck_command_USSD_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_USSD_CDR = EmailOperator(
    task_id='faile_USSD_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_USSD_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_USSD_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_USSD_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_USSD_CDR >> PCF_USSD_CDR >> PCFCheck_USSD_CDR >> delay_python_task_USSD_CDR >> Dedup_USSD_CDR
Branching_USSD_CDR >> Dedup2_USSD_CDR
Branching_USSD_CDR >> faile_USSD_CDR
Branching_USSD_CDR >> Validation_End

feedname_MSO_REVERSED_BILLING = 'MSO_REVERSED_BILLING'.upper()
feedname2_MSO_REVERSED_BILLING = 'MSO_REVERSED_BILLING'.lower()

dedup_command_MSO_REVERSED_BILLING="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_MSO_REVERSED_BILLING)
logging.info(dedup_command_MSO_REVERSED_BILLING) 

pcf_command_MSO_REVERSED_BILLING = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_MSO_REVERSED_BILLING)
logging.info(pcf_command_MSO_REVERSED_BILLING)

PCFCheck_command_MSO_REVERSED_BILLING = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MSO_REVERSED_BILLING,run_date,sleeptime)
logging.info(PCFCheck_command_MSO_REVERSED_BILLING)

branchScript_MSO_REVERSED_BILLING = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MSO_REVERSED_BILLING,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MSO_REVERSED_BILLING)

def branch_MSO_REVERSED_BILLING():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MSO_REVERSED_BILLING,feedname2_MSO_REVERSED_BILLING)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MSO_REVERSED_BILLING)
    x = readcsv(filepath,feedname2_MSO_REVERSED_BILLING)
    logging.info('branch_MSO_REVERSED_BILLING' + x)
    return x

Branching_MSO_REVERSED_BILLING = BranchPythonOperator(
    task_id='branchid_MSO_REVERSED_BILLING',
    python_callable=branch_MSO_REVERSED_BILLING,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_MSO_REVERSED_BILLING = BashOperator(
    task_id='PCF_MSO_REVERSED_BILLING',
    bash_command= pcf_command_MSO_REVERSED_BILLING,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_MSO_REVERSED_BILLING = BashOperator(
    task_id='Dedup_MSO_REVERSED_BILLING',
    bash_command= dedup_command_MSO_REVERSED_BILLING,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_MSO_REVERSED_BILLING = BashOperator(
    task_id='Dedup2_MSO_REVERSED_BILLING',
    bash_command= dedup_command_MSO_REVERSED_BILLING,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_MSO_REVERSED_BILLING = BashOperator(
    task_id='PCFCheck_MSO_REVERSED_BILLING',
    bash_command=PCFCheck_command_MSO_REVERSED_BILLING,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_MSO_REVERSED_BILLING = EmailOperator(
    task_id='faile_MSO_REVERSED_BILLING',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MSO_REVERSED_BILLING),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MSO_REVERSED_BILLING: PythonOperator = PythonOperator(task_id="waitForFlush_MSO_REVERSED_BILLING",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MSO_REVERSED_BILLING >> PCF_MSO_REVERSED_BILLING >> PCFCheck_MSO_REVERSED_BILLING >> delay_python_task_MSO_REVERSED_BILLING >> Dedup_MSO_REVERSED_BILLING
Branching_MSO_REVERSED_BILLING >> Dedup2_MSO_REVERSED_BILLING
Branching_MSO_REVERSED_BILLING >> faile_MSO_REVERSED_BILLING
Branching_MSO_REVERSED_BILLING >> Validation_End

feedname_NGVS_CDR = 'NGVS_CDR'.upper()
feedname2_NGVS_CDR = 'NGVS_CDR'.lower()

dedup_command_NGVS_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_NGVS_CDR)
logging.info(dedup_command_NGVS_CDR) 

pcf_command_NGVS_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_NGVS_CDR)
logging.info(pcf_command_NGVS_CDR)

PCFCheck_command_NGVS_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NGVS_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_NGVS_CDR)

branchScript_NGVS_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_NGVS_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_NGVS_CDR)

def branch_NGVS_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NGVS_CDR,feedname2_NGVS_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NGVS_CDR)
    x = readcsv(filepath,feedname2_NGVS_CDR)
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
    queue='edge01002',
	priority_weight=1
)

Dedup_NGVS_CDR = BashOperator(
    task_id='Dedup_NGVS_CDR',
    bash_command= dedup_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_NGVS_CDR = BashOperator(
    task_id='Dedup2_NGVS_CDR',
    bash_command= dedup_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_NGVS_CDR = BashOperator(
    task_id='PCFCheck_NGVS_CDR',
    bash_command=PCFCheck_command_NGVS_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
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

delay_python_task_NGVS_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_NGVS_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_NGVS_CDR >> PCF_NGVS_CDR >> PCFCheck_NGVS_CDR >> delay_python_task_NGVS_CDR >> Dedup_NGVS_CDR
Branching_NGVS_CDR >> Dedup2_NGVS_CDR
Branching_NGVS_CDR >> faile_NGVS_CDR
Branching_NGVS_CDR >> Validation_End

feedname_LTE_HSS = 'LTE_HSS'.upper()
feedname2_LTE_HSS = 'LTE_HSS'.lower()

dedup_command_LTE_HSS="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_LTE_HSS)
logging.info(dedup_command_LTE_HSS) 

pcf_command_LTE_HSS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_LTE_HSS)
logging.info(pcf_command_LTE_HSS)

PCFCheck_command_LTE_HSS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_LTE_HSS,run_date,sleeptime)
logging.info(PCFCheck_command_LTE_HSS)

branchScript_LTE_HSS = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_LTE_HSS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_LTE_HSS)

def branch_LTE_HSS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_LTE_HSS,feedname2_LTE_HSS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_LTE_HSS)
    x = readcsv(filepath,feedname2_LTE_HSS)
    logging.info('branch_LTE_HSS' + x)
    return x

Branching_LTE_HSS = BranchPythonOperator(
    task_id='branchid_LTE_HSS',
    python_callable=branch_LTE_HSS,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_LTE_HSS = BashOperator(
    task_id='PCF_LTE_HSS',
    bash_command= pcf_command_LTE_HSS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_LTE_HSS = BashOperator(
    task_id='Dedup_LTE_HSS',
    bash_command= dedup_command_LTE_HSS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_LTE_HSS = BashOperator(
    task_id='Dedup2_LTE_HSS',
    bash_command= dedup_command_LTE_HSS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_LTE_HSS = BashOperator(
    task_id='PCFCheck_LTE_HSS',
    bash_command=PCFCheck_command_LTE_HSS,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_LTE_HSS = EmailOperator(
    task_id='faile_LTE_HSS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_LTE_HSS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_LTE_HSS: PythonOperator = PythonOperator(task_id="waitForFlush_LTE_HSS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_LTE_HSS >> PCF_LTE_HSS >> PCFCheck_LTE_HSS >> delay_python_task_LTE_HSS >> Dedup_LTE_HSS
Branching_LTE_HSS >> Dedup2_LTE_HSS
Branching_LTE_HSS >> faile_LTE_HSS
Branching_LTE_HSS >> Validation_End

feedname_CIS_CDR = 'CIS_CDR'.upper()
feedname2_CIS_CDR = 'CIS_CDR'.lower()

dedup_command_CIS_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CIS_CDR)
logging.info(dedup_command_CIS_CDR) 

pcf_command_CIS_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CIS_CDR)
logging.info(pcf_command_CIS_CDR)

PCFCheck_command_CIS_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CIS_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_CIS_CDR)

branchScript_CIS_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CIS_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CIS_CDR)

def branch_CIS_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CIS_CDR,feedname2_CIS_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CIS_CDR)
    x = readcsv(filepath,feedname2_CIS_CDR)
    logging.info('branch_CIS_CDR' + x)
    return x

Branching_CIS_CDR = BranchPythonOperator(
    task_id='branchid_CIS_CDR',
    python_callable=branch_CIS_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CIS_CDR = BashOperator(
    task_id='PCF_CIS_CDR',
    bash_command= pcf_command_CIS_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CIS_CDR = BashOperator(
    task_id='Dedup_CIS_CDR',
    bash_command= dedup_command_CIS_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CIS_CDR = BashOperator(
    task_id='Dedup2_CIS_CDR',
    bash_command= dedup_command_CIS_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CIS_CDR = BashOperator(
    task_id='PCFCheck_CIS_CDR',
    bash_command=PCFCheck_command_CIS_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CIS_CDR = EmailOperator(
    task_id='faile_CIS_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CIS_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CIS_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_CIS_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CIS_CDR >> PCF_CIS_CDR >> PCFCheck_CIS_CDR >> delay_python_task_CIS_CDR >> Dedup_CIS_CDR
Branching_CIS_CDR >> Dedup2_CIS_CDR
Branching_CIS_CDR >> faile_CIS_CDR
Branching_CIS_CDR >> Validation_End

feedname_HSDP_CDR = 'HSDP_CDR'.upper()
feedname2_HSDP_CDR = 'HSDP_CDR'.lower()

dedup_command_HSDP_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_HSDP_CDR)
logging.info(dedup_command_HSDP_CDR) 

pcf_command_HSDP_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004 " % (d_1,d_1,feedname_HSDP_CDR)
logging.info(pcf_command_HSDP_CDR)

PCFCheck_command_HSDP_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_HSDP_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_HSDP_CDR)

branchScript_HSDP_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_HSDP_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_HSDP_CDR)

def branch_HSDP_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_HSDP_CDR,feedname2_HSDP_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_HSDP_CDR)
    x = readcsv(filepath,feedname2_HSDP_CDR)
    logging.info('branch_HSDP_CDR' + x)
    return x

Branching_HSDP_CDR = BranchPythonOperator(
    task_id='branchid_HSDP_CDR',
    python_callable=branch_HSDP_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_HSDP_CDR = BashOperator(
    task_id='PCF_HSDP_CDR',
    bash_command= pcf_command_HSDP_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_HSDP_CDR = BashOperator(
    task_id='Dedup_HSDP_CDR',
    bash_command= dedup_command_HSDP_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_HSDP_CDR = BashOperator(
    task_id='Dedup2_HSDP_CDR',
    bash_command= dedup_command_HSDP_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_HSDP_CDR = BashOperator(
    task_id='PCFCheck_HSDP_CDR',
    bash_command=PCFCheck_command_HSDP_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_HSDP_CDR = EmailOperator(
    task_id='faile_HSDP_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_HSDP_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_HSDP_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_HSDP_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_HSDP_CDR >> PCF_HSDP_CDR >> PCFCheck_HSDP_CDR >> delay_python_task_HSDP_CDR >> Dedup_HSDP_CDR
Branching_HSDP_CDR >> Dedup2_HSDP_CDR
Branching_HSDP_CDR >> faile_HSDP_CDR
Branching_HSDP_CDR >> Validation_End

feedname_DPI_CDR = 'DPI_CDR'.upper()
feedname2_DPI_CDR = 'DPI_CDR'.lower()

dedup_command_DPI_CDR="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_DPI_CDR)
logging.info(dedup_command_DPI_CDR) 

pcf_command_DPI_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_DPI_CDR)
logging.info(pcf_command_DPI_CDR)

PCFCheck_command_DPI_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_DPI_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_DPI_CDR)

branchScript_DPI_CDR = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_DPI_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_DPI_CDR)

def branch_DPI_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_DPI_CDR,feedname2_DPI_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_DPI_CDR)
    x = readcsv(filepath,feedname2_DPI_CDR)
    logging.info('branch_DPI_CDR' + x)
    return x

Branching_DPI_CDR = BranchPythonOperator(
    task_id='branchid_DPI_CDR',
    python_callable=branch_DPI_CDR,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_DPI_CDR = BashOperator(
    task_id='PCF_DPI_CDR',
    bash_command= pcf_command_DPI_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_DPI_CDR = BashOperator(
    task_id='Dedup_DPI_CDR',
    bash_command= dedup_command_DPI_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_DPI_CDR = BashOperator(
    task_id='Dedup2_DPI_CDR',
    bash_command= dedup_command_DPI_CDR,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_DPI_CDR = BashOperator(
    task_id='PCFCheck_DPI_CDR',
    bash_command=PCFCheck_command_DPI_CDR,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_DPI_CDR = EmailOperator(
    task_id='faile_DPI_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_DPI_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_DPI_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_DPI_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_DPI_CDR >> PCF_DPI_CDR >> PCFCheck_DPI_CDR >> delay_python_task_DPI_CDR >> Dedup_DPI_CDR
Branching_DPI_CDR >> Dedup2_DPI_CDR
Branching_DPI_CDR >> faile_DPI_CDR
Branching_DPI_CDR >> Validation_End

feedname_CB_SERV_MAST_VIEW = 'CB_SERV_MAST_VIEW'.upper()
feedname2_CB_SERV_MAST_VIEW = 'CB_SERV_MAST_VIEW'.lower()

dedup_command_CB_SERV_MAST_VIEW="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CB_SERV_MAST_VIEW)
logging.info(dedup_command_CB_SERV_MAST_VIEW) 

pcf_command_CB_SERV_MAST_VIEW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_CB_SERV_MAST_VIEW)
logging.info(pcf_command_CB_SERV_MAST_VIEW)

PCFCheck_command_CB_SERV_MAST_VIEW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CB_SERV_MAST_VIEW,run_date,sleeptime)
logging.info(PCFCheck_command_CB_SERV_MAST_VIEW)

branchScript_CB_SERV_MAST_VIEW = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CB_SERV_MAST_VIEW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CB_SERV_MAST_VIEW)

def branch_CB_SERV_MAST_VIEW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CB_SERV_MAST_VIEW,feedname2_CB_SERV_MAST_VIEW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CB_SERV_MAST_VIEW)
    x = readcsv(filepath,feedname2_CB_SERV_MAST_VIEW)
    logging.info('branch_CB_SERV_MAST_VIEW' + x)
    return x

Branching_CB_SERV_MAST_VIEW = BranchPythonOperator(
    task_id='branchid_CB_SERV_MAST_VIEW',
    python_callable=branch_CB_SERV_MAST_VIEW,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CB_SERV_MAST_VIEW = BashOperator(
    task_id='PCF_CB_SERV_MAST_VIEW',
    bash_command= pcf_command_CB_SERV_MAST_VIEW,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CB_SERV_MAST_VIEW = BashOperator(
    task_id='Dedup_CB_SERV_MAST_VIEW',
    bash_command= dedup_command_CB_SERV_MAST_VIEW,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CB_SERV_MAST_VIEW = BashOperator(
    task_id='Dedup2_CB_SERV_MAST_VIEW',
    bash_command= dedup_command_CB_SERV_MAST_VIEW,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CB_SERV_MAST_VIEW = BashOperator(
    task_id='PCFCheck_CB_SERV_MAST_VIEW',
    bash_command=PCFCheck_command_CB_SERV_MAST_VIEW,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CB_SERV_MAST_VIEW = EmailOperator(
    task_id='faile_CB_SERV_MAST_VIEW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CB_SERV_MAST_VIEW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CB_SERV_MAST_VIEW: PythonOperator = PythonOperator(task_id="waitForFlush_CB_SERV_MAST_VIEW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CB_SERV_MAST_VIEW >> PCF_CB_SERV_MAST_VIEW >> PCFCheck_CB_SERV_MAST_VIEW >> delay_python_task_CB_SERV_MAST_VIEW >> Dedup_CB_SERV_MAST_VIEW
Branching_CB_SERV_MAST_VIEW >> Dedup2_CB_SERV_MAST_VIEW
Branching_CB_SERV_MAST_VIEW >> faile_CB_SERV_MAST_VIEW
Branching_CB_SERV_MAST_VIEW >> Validation_End

feedname_WBS_PM_RATED_CDRS = 'WBS_PM_RATED_CDRS'.upper()
feedname2_WBS_PM_RATED_CDRS = 'WBS_PM_RATED_CDRS'.lower()

dedup_command_WBS_PM_RATED_CDRS="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_WBS_PM_RATED_CDRS)
logging.info(dedup_command_WBS_PM_RATED_CDRS) 

pcf_command_WBS_PM_RATED_CDRS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_WBS_PM_RATED_CDRS)
logging.info(pcf_command_WBS_PM_RATED_CDRS)

PCFCheck_command_WBS_PM_RATED_CDRS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_WBS_PM_RATED_CDRS,run_date,sleeptime)
logging.info(PCFCheck_command_WBS_PM_RATED_CDRS)

branchScript_WBS_PM_RATED_CDRS = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_WBS_PM_RATED_CDRS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_WBS_PM_RATED_CDRS)

def branch_WBS_PM_RATED_CDRS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_WBS_PM_RATED_CDRS,feedname2_WBS_PM_RATED_CDRS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_WBS_PM_RATED_CDRS)
    x = readcsv(filepath,feedname2_WBS_PM_RATED_CDRS)
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
    queue='edge01002',
	priority_weight=1
)

Dedup_WBS_PM_RATED_CDRS = BashOperator(
    task_id='Dedup_WBS_PM_RATED_CDRS',
    bash_command= dedup_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_WBS_PM_RATED_CDRS = BashOperator(
    task_id='Dedup2_WBS_PM_RATED_CDRS',
    bash_command= dedup_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_WBS_PM_RATED_CDRS = BashOperator(
    task_id='PCFCheck_WBS_PM_RATED_CDRS',
    bash_command=PCFCheck_command_WBS_PM_RATED_CDRS,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
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

delay_python_task_WBS_PM_RATED_CDRS: PythonOperator = PythonOperator(task_id="waitForFlush_WBS_PM_RATED_CDRS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_WBS_PM_RATED_CDRS >> PCF_WBS_PM_RATED_CDRS >> PCFCheck_WBS_PM_RATED_CDRS >> delay_python_task_WBS_PM_RATED_CDRS >> Dedup_WBS_PM_RATED_CDRS
Branching_WBS_PM_RATED_CDRS >> Dedup2_WBS_PM_RATED_CDRS
Branching_WBS_PM_RATED_CDRS >> faile_WBS_PM_RATED_CDRS
Branching_WBS_PM_RATED_CDRS >> Validation_End

feedname_NEWREG_BIOUPDT_POOL_DAILY = 'NEWREG_BIOUPDT_POOL_DAILY'.upper()
feedname2_NEWREG_BIOUPDT_POOL_DAILY = 'NEWREG_BIOUPDT_POOL_DAILY'.lower()

dedup_command_NEWREG_BIOUPDT_POOL_DAILY="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_NEWREG_BIOUPDT_POOL_DAILY)
logging.info(dedup_command_NEWREG_BIOUPDT_POOL_DAILY) 

pcf_command_NEWREG_BIOUPDT_POOL_DAILY = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_NEWREG_BIOUPDT_POOL_DAILY)
logging.info(pcf_command_NEWREG_BIOUPDT_POOL_DAILY)

PCFCheck_command_NEWREG_BIOUPDT_POOL_DAILY = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_NEWREG_BIOUPDT_POOL_DAILY,run_date,sleeptime)
logging.info(PCFCheck_command_NEWREG_BIOUPDT_POOL_DAILY)

branchScript_NEWREG_BIOUPDT_POOL_DAILY = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_NEWREG_BIOUPDT_POOL_DAILY,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_NEWREG_BIOUPDT_POOL_DAILY)

def branch_NEWREG_BIOUPDT_POOL_DAILY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_NEWREG_BIOUPDT_POOL_DAILY,feedname2_NEWREG_BIOUPDT_POOL_DAILY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_NEWREG_BIOUPDT_POOL_DAILY)
    x = readcsv(filepath,feedname2_NEWREG_BIOUPDT_POOL_DAILY)
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
    queue='edge01002',
	priority_weight=1
)

Dedup_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='Dedup_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command= dedup_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='Dedup2_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command= dedup_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_NEWREG_BIOUPDT_POOL_DAILY = BashOperator(
    task_id='PCFCheck_NEWREG_BIOUPDT_POOL_DAILY',
    bash_command=PCFCheck_command_NEWREG_BIOUPDT_POOL_DAILY,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
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

delay_python_task_NEWREG_BIOUPDT_POOL_DAILY: PythonOperator = PythonOperator(task_id="waitForFlush_NEWREG_BIOUPDT_POOL_DAILY",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_NEWREG_BIOUPDT_POOL_DAILY >> PCF_NEWREG_BIOUPDT_POOL_DAILY >> PCFCheck_NEWREG_BIOUPDT_POOL_DAILY >> delay_python_task_NEWREG_BIOUPDT_POOL_DAILY >> Dedup_NEWREG_BIOUPDT_POOL_DAILY
Branching_NEWREG_BIOUPDT_POOL_DAILY >> Dedup2_NEWREG_BIOUPDT_POOL_DAILY
Branching_NEWREG_BIOUPDT_POOL_DAILY >> faile_NEWREG_BIOUPDT_POOL_DAILY
Branching_NEWREG_BIOUPDT_POOL_DAILY >> Validation_End

feedname_CUG_ACCESS_FEES = 'CUG_ACCESS_FEES'.upper()
feedname2_CUG_ACCESS_FEES = 'CUG_ACCESS_FEES'.lower()

dedup_command_CUG_ACCESS_FEES="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_CUG_ACCESS_FEES)
logging.info(dedup_command_CUG_ACCESS_FEES) 

pcf_command_CUG_ACCESS_FEES = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName m1004 " % (d_1,d_1,feedname_CUG_ACCESS_FEES)
logging.info(pcf_command_CUG_ACCESS_FEES)

PCFCheck_command_CUG_ACCESS_FEES = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CUG_ACCESS_FEES,run_date,sleeptime)
logging.info(PCFCheck_command_CUG_ACCESS_FEES)

branchScript_CUG_ACCESS_FEES = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CUG_ACCESS_FEES,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CUG_ACCESS_FEES)

def branch_CUG_ACCESS_FEES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CUG_ACCESS_FEES,feedname2_CUG_ACCESS_FEES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CUG_ACCESS_FEES)
    x = readcsv(filepath,feedname2_CUG_ACCESS_FEES)
    logging.info('branch_CUG_ACCESS_FEES' + x)
    return x

Branching_CUG_ACCESS_FEES = BranchPythonOperator(
    task_id='branchid_CUG_ACCESS_FEES',
    python_callable=branch_CUG_ACCESS_FEES,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_CUG_ACCESS_FEES = BashOperator(
    task_id='PCF_CUG_ACCESS_FEES',
    bash_command= pcf_command_CUG_ACCESS_FEES,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_CUG_ACCESS_FEES = BashOperator(
    task_id='Dedup_CUG_ACCESS_FEES',
    bash_command= dedup_command_CUG_ACCESS_FEES,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_CUG_ACCESS_FEES = BashOperator(
    task_id='Dedup2_CUG_ACCESS_FEES',
    bash_command= dedup_command_CUG_ACCESS_FEES,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_CUG_ACCESS_FEES = BashOperator(
    task_id='PCFCheck_CUG_ACCESS_FEES',
    bash_command=PCFCheck_command_CUG_ACCESS_FEES,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_CUG_ACCESS_FEES = EmailOperator(
    task_id='faile_CUG_ACCESS_FEES',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CUG_ACCESS_FEES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CUG_ACCESS_FEES: PythonOperator = PythonOperator(task_id="waitForFlush_CUG_ACCESS_FEES",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CUG_ACCESS_FEES >> PCF_CUG_ACCESS_FEES >> PCFCheck_CUG_ACCESS_FEES >> delay_python_task_CUG_ACCESS_FEES >> Dedup_CUG_ACCESS_FEES
Branching_CUG_ACCESS_FEES >> Dedup2_CUG_ACCESS_FEES
Branching_CUG_ACCESS_FEES >> faile_CUG_ACCESS_FEES
Branching_CUG_ACCESS_FEES >> Validation_End

feedname_ERS_VEND_NEW = 'ERS_VEND_NEW'.upper()
feedname2_ERS_VEND_NEW = 'ERS_VEND_NEW'.lower()

dedup_command_ERS_VEND_NEW="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_ERS_VEND_NEW)
logging.info(dedup_command_ERS_VEND_NEW) 

pcf_command_ERS_VEND_NEW = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_ERS_VEND_NEW)
logging.info(pcf_command_ERS_VEND_NEW)

PCFCheck_command_ERS_VEND_NEW = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ERS_VEND_NEW,run_date,sleeptime)
logging.info(PCFCheck_command_ERS_VEND_NEW)

branchScript_ERS_VEND_NEW = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ERS_VEND_NEW,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ERS_VEND_NEW)

def branch_ERS_VEND_NEW():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ERS_VEND_NEW,feedname2_ERS_VEND_NEW)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ERS_VEND_NEW)
    x = readcsv(filepath,feedname2_ERS_VEND_NEW)
    logging.info('branch_ERS_VEND_NEW' + x)
    return x

Branching_ERS_VEND_NEW = BranchPythonOperator(
    task_id='branchid_ERS_VEND_NEW',
    python_callable=branch_ERS_VEND_NEW,
    dag=dag,
    run_as_user='daasuser',
	priority_weight=1
)

PCF_ERS_VEND_NEW = BashOperator(
    task_id='PCF_ERS_VEND_NEW',
    bash_command= pcf_command_ERS_VEND_NEW,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

Dedup_ERS_VEND_NEW = BashOperator(
    task_id='Dedup_ERS_VEND_NEW',
    bash_command= dedup_command_ERS_VEND_NEW,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)
Dedup2_ERS_VEND_NEW = BashOperator(
    task_id='Dedup2_ERS_VEND_NEW',
    bash_command= dedup_command_ERS_VEND_NEW,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
	priority_weight=1
)

PCFCheck_ERS_VEND_NEW = BashOperator(
    task_id='PCFCheck_ERS_VEND_NEW',
    bash_command=PCFCheck_command_ERS_VEND_NEW,
    dag=dag,
    run_as_user='daasuser',
	queue='edge01002',
    priority_weight=1
)

faile_ERS_VEND_NEW = EmailOperator(
    task_id='faile_ERS_VEND_NEW',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ERS_VEND_NEW),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ERS_VEND_NEW: PythonOperator = PythonOperator(task_id="waitForFlush_ERS_VEND_NEW",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ERS_VEND_NEW >> PCF_ERS_VEND_NEW >> PCFCheck_ERS_VEND_NEW >> delay_python_task_ERS_VEND_NEW >> Dedup_ERS_VEND_NEW
Branching_ERS_VEND_NEW >> Dedup2_ERS_VEND_NEW
Branching_ERS_VEND_NEW >> faile_ERS_VEND_NEW
Branching_ERS_VEND_NEW >> Validation_End




#Branching_CS6_CCN_CDR
Validation >> Branching_CS6_AIR_CDR
Validation >> Branching_CS6_SDP_CDR
Validation >> Branching_BUNDLE4U_GPRS
Validation >> Branching_BUNDLE4U_VOICE
Validation >> Branching_CS5_AIR_ADJ_MA
Validation >> Branching_CS5_CCN_GPRS_MA
Validation >> Branching_CS5_CCN_SMS_MA
Validation >> Branching_CS5_SDP_ACC_ADJ_MA
Validation >> Branching_DMC_DUMP_ALL
Validation >> Branching_MSC_CDR
Validation >> Branching_SDP_DMP_MA
Validation >> Branching_MOBILE_MONEY
Validation >> Branching_CS5_AIR_REFILL_MA
Validation >> Branching_CS5_CCN_VOICE_MA
Validation >> Branching_SDP_DMP_DA
Validation >> Branching_UC_DUMP
Validation >> Branching_OFFER_DUMP
Validation >> Branching_MVAS_DND_MSISDN_REPORT
Validation >> Branching_USSD_CDR
Validation >> Branching_MSO_REVERSED_BILLING
Validation >> Branching_NGVS_CDR
Validation >> Branching_LTE_HSS
Validation >> Branching_CIS_CDR
Validation >> Branching_HSDP_CDR
Validation >> Branching_DPI_CDR
Validation >> Branching_CB_SERV_MAST_VIEW
Validation >> Branching_WBS_PM_RATED_CDRS
Validation >> Branching_NEWREG_BIOUPDT_POOL_DAILY
Validation >> Branching_CUG_ACCESS_FEES
Validation >> Branching_ERS_VEND_NEW



#Dedup_CS6_CCN_CDR >> Validation_End
#Dedup2_CS6_CCN_CDR >> Validation_End
#Branching_CS6_CCN_CDR >> Validation_End
Dedup_CS6_AIR_CDR >> Validation_End
Dedup2_CS6_AIR_CDR >> Validation_End
Branching_CS6_AIR_CDR >> Validation_End
Dedup_CS6_SDP_CDR >> Validation_End
Dedup2_CS6_SDP_CDR >> Validation_End
Branching_CS6_SDP_CDR >> Validation_End
Dedup_BUNDLE4U_GPRS >> Validation_End
Dedup2_BUNDLE4U_GPRS >> Validation_End
Branching_BUNDLE4U_GPRS >> Validation_End
Dedup_BUNDLE4U_VOICE >> Validation_End
Dedup2_BUNDLE4U_VOICE >> Validation_End
Branching_BUNDLE4U_VOICE >> Validation_End
Dedup_CS5_AIR_ADJ_MA >> Validation_End
Dedup2_CS5_AIR_ADJ_MA >> Validation_End
Branching_CS5_AIR_ADJ_MA >> Validation_End
Dedup_CS5_CCN_GPRS_MA >> Validation_End
Dedup2_CS5_CCN_GPRS_MA >> Validation_End
Branching_CS5_CCN_GPRS_MA >> Validation_End
Dedup_CS5_CCN_SMS_MA >> Validation_End
Dedup2_CS5_CCN_SMS_MA >> Validation_End
Branching_CS5_CCN_SMS_MA >> Validation_End
Dedup_CS5_SDP_ACC_ADJ_MA >> Validation_End
Dedup2_CS5_SDP_ACC_ADJ_MA >> Validation_End
Branching_CS5_SDP_ACC_ADJ_MA >> Validation_End
Dedup_DMC_DUMP_ALL >> Validation_End
Dedup2_DMC_DUMP_ALL >> Validation_End
Branching_DMC_DUMP_ALL >> Validation_End
Dedup_MSC_CDR >> Validation_End
Dedup2_MSC_CDR >> Validation_End
Branching_MSC_CDR >> Validation_End
Dedup_SDP_DMP_MA >> Validation_End
Dedup2_SDP_DMP_MA >> Validation_End
Branching_SDP_DMP_MA >> Validation_End
Dedup_MOBILE_MONEY >> Validation_End
Dedup2_MOBILE_MONEY >> Validation_End
Branching_MOBILE_MONEY >> Validation_End
Dedup_CS5_AIR_REFILL_MA >> Validation_End
Dedup2_CS5_AIR_REFILL_MA >> Validation_End
Branching_CS5_AIR_REFILL_MA >> Validation_End
Dedup_CS5_CCN_VOICE_MA >> Validation_End
Dedup2_CS5_CCN_VOICE_MA >> Validation_End
Branching_CS5_CCN_VOICE_MA >> Validation_End
Dedup_SDP_DMP_DA >> Validation_End
Dedup2_SDP_DMP_DA >> Validation_End
Branching_SDP_DMP_DA >> Validation_End
Dedup_UC_DUMP >> Validation_End
Dedup2_UC_DUMP >> Validation_End
Branching_UC_DUMP >> Validation_End
Dedup_OFFER_DUMP >> Validation_End
Dedup2_OFFER_DUMP >> Validation_End
Branching_OFFER_DUMP >> Validation_End
Dedup_MVAS_DND_MSISDN_REPORT >> Validation_End
Dedup2_MVAS_DND_MSISDN_REPORT >> Validation_End
Branching_MVAS_DND_MSISDN_REPORT >> Validation_End
Dedup_USSD_CDR >> Validation_End
Dedup2_USSD_CDR >> Validation_End
Branching_USSD_CDR >> Validation_End
Dedup_MSO_REVERSED_BILLING >> Validation_End
Dedup2_MSO_REVERSED_BILLING >> Validation_End
Branching_MSO_REVERSED_BILLING >> Validation_End
Dedup_NGVS_CDR >> Validation_End
Dedup2_NGVS_CDR >> Validation_End
Branching_NGVS_CDR >> Validation_End
Dedup_LTE_HSS >> Validation_End
Dedup2_LTE_HSS >> Validation_End
Branching_LTE_HSS >> Validation_End
Dedup_CIS_CDR >> Validation_End
Dedup2_CIS_CDR >> Validation_End
Branching_CIS_CDR >> Validation_End
Dedup_HSDP_CDR >> Validation_End
Dedup2_HSDP_CDR >> Validation_End
Branching_HSDP_CDR >> Validation_End
Dedup_DPI_CDR >> Validation_End
Dedup2_DPI_CDR >> Validation_End
Branching_DPI_CDR >> Validation_End
Dedup_CB_SERV_MAST_VIEW >> Validation_End
Dedup2_CB_SERV_MAST_VIEW >> Validation_End
Branching_CB_SERV_MAST_VIEW >> Validation_End
Dedup_WBS_PM_RATED_CDRS >> Validation_End
Dedup2_WBS_PM_RATED_CDRS >> Validation_End
Branching_WBS_PM_RATED_CDRS >> Validation_End
Dedup_NEWREG_BIOUPDT_POOL_DAILY >> Validation_End
Dedup2_NEWREG_BIOUPDT_POOL_DAILY >> Validation_End
Branching_NEWREG_BIOUPDT_POOL_DAILY >> Validation_End
Dedup_CUG_ACCESS_FEES >> Validation_End
Dedup2_CUG_ACCESS_FEES >> Validation_End
Branching_CUG_ACCESS_FEES >> Validation_End
Dedup_ERS_VEND_NEW >> Validation_End
Dedup2_ERS_VEND_NEW >> Validation_End
Branching_ERS_VEND_NEW >> Validation_End
Validation_End >> Success
