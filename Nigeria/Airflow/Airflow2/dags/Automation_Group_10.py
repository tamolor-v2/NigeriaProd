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
d_1 = Variable.get("rerunDate10", deserialize_json=True)
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

dag = DAG('New_Automation_Group_10', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

ValidationCode = 'python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f group10 -c config.json' %(d_1)
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


feedname_HUAMSC_DAAS = 'HUAMSC_DAAS'.upper()
feedname2_HUAMSC_DAAS = 'HUAMSC_DAAS'.lower()

dedup_command_HUAMSC_DAAS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_HUAMSC_DAAS)
logging.info(dedup_command_HUAMSC_DAAS) 

pcf_command_HUAMSC_DAAS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_HUAMSC_DAAS)
logging.info(pcf_command_HUAMSC_DAAS)

PCFCheck_command_HUAMSC_DAAS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_HUAMSC_DAAS,run_date,sleeptime)
logging.info(PCFCheck_command_HUAMSC_DAAS)

branchScript_HUAMSC_DAAS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_HUAMSC_DAAS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_HUAMSC_DAAS)

def branch_HUAMSC_DAAS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_HUAMSC_DAAS,feedname2_HUAMSC_DAAS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_HUAMSC_DAAS)
    x = readcsv(filepath,feedname2_HUAMSC_DAAS)
    logging.info('branch_HUAMSC_DAAS' + x)
    return x

Branching_HUAMSC_DAAS = BranchPythonOperator(
    task_id='branchid_HUAMSC_DAAS',
    python_callable=branch_HUAMSC_DAAS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_HUAMSC_DAAS = BashOperator(
    task_id='PCF_HUAMSC_DAAS',
    bash_command= pcf_command_HUAMSC_DAAS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_HUAMSC_DAAS = BashOperator(
    task_id='Dedup_HUAMSC_DAAS',
    bash_command= dedup_command_HUAMSC_DAAS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_HUAMSC_DAAS = BashOperator(
    task_id='Dedup2_HUAMSC_DAAS',
    bash_command= dedup_command_HUAMSC_DAAS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_HUAMSC_DAAS = BashOperator(
    task_id='PCFCheck_HUAMSC_DAAS',
    bash_command=PCFCheck_command_HUAMSC_DAAS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_HUAMSC_DAAS = EmailOperator(
    task_id='faile_HUAMSC_DAAS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_HUAMSC_DAAS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_HUAMSC_DAAS: PythonOperator = PythonOperator(task_id="waitForFlush_HUAMSC_DAAS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_HUAMSC_DAAS >> PCF_HUAMSC_DAAS >> PCFCheck_HUAMSC_DAAS >> delay_python_task_HUAMSC_DAAS >> Dedup_HUAMSC_DAAS >> Validation_End 
Branching_HUAMSC_DAAS >> Dedup2_HUAMSC_DAAS >> Validation_End
Branching_HUAMSC_DAAS >> faile_HUAMSC_DAAS
Branching_HUAMSC_DAAS >> Validation_End


feedname_MYMTNWEB_RECHARGES = 'MYMTNWEB_RECHARGES'.upper()
feedname2_MYMTNWEB_RECHARGES = 'MYMTNWEB_RECHARGES'.lower()

dedup_command_MYMTNWEB_RECHARGES="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MYMTNWEB_RECHARGES)
logging.info(dedup_command_MYMTNWEB_RECHARGES) 

pcf_command_MYMTNWEB_RECHARGES = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MYMTNWEB_RECHARGES)
logging.info(pcf_command_MYMTNWEB_RECHARGES)

PCFCheck_command_MYMTNWEB_RECHARGES = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MYMTNWEB_RECHARGES,run_date,sleeptime)
logging.info(PCFCheck_command_MYMTNWEB_RECHARGES)

branchScript_MYMTNWEB_RECHARGES = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MYMTNWEB_RECHARGES,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MYMTNWEB_RECHARGES)

def branch_MYMTNWEB_RECHARGES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MYMTNWEB_RECHARGES,feedname2_MYMTNWEB_RECHARGES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MYMTNWEB_RECHARGES)
    x = readcsv(filepath,feedname2_MYMTNWEB_RECHARGES)
    logging.info('branch_MYMTNWEB_RECHARGES' + x)
    return x

Branching_MYMTNWEB_RECHARGES = BranchPythonOperator(
    task_id='branchid_MYMTNWEB_RECHARGES',
    python_callable=branch_MYMTNWEB_RECHARGES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MYMTNWEB_RECHARGES = BashOperator(
    task_id='PCF_MYMTNWEB_RECHARGES',
    bash_command= pcf_command_MYMTNWEB_RECHARGES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MYMTNWEB_RECHARGES = BashOperator(
    task_id='Dedup_MYMTNWEB_RECHARGES',
    bash_command= dedup_command_MYMTNWEB_RECHARGES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MYMTNWEB_RECHARGES = BashOperator(
    task_id='Dedup2_MYMTNWEB_RECHARGES',
    bash_command= dedup_command_MYMTNWEB_RECHARGES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MYMTNWEB_RECHARGES = BashOperator(
    task_id='PCFCheck_MYMTNWEB_RECHARGES',
    bash_command=PCFCheck_command_MYMTNWEB_RECHARGES,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MYMTNWEB_RECHARGES = EmailOperator(
    task_id='faile_MYMTNWEB_RECHARGES',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MYMTNWEB_RECHARGES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MYMTNWEB_RECHARGES: PythonOperator = PythonOperator(task_id="waitForFlush_MYMTNWEB_RECHARGES",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MYMTNWEB_RECHARGES >> PCF_MYMTNWEB_RECHARGES >> PCFCheck_MYMTNWEB_RECHARGES >> delay_python_task_MYMTNWEB_RECHARGES >> Dedup_MYMTNWEB_RECHARGES >> Validation_End 
Branching_MYMTNWEB_RECHARGES >> Dedup2_MYMTNWEB_RECHARGES >> Validation_End
Branching_MYMTNWEB_RECHARGES >> faile_MYMTNWEB_RECHARGES
Branching_MYMTNWEB_RECHARGES >> Validation_End


feedname_MYMTNWEB_PAGELOAD = 'MYMTNWEB_PAGELOAD'.upper()
feedname2_MYMTNWEB_PAGELOAD = 'MYMTNWEB_PAGELOAD'.lower()

dedup_command_MYMTNWEB_PAGELOAD="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MYMTNWEB_PAGELOAD)
logging.info(dedup_command_MYMTNWEB_PAGELOAD) 

pcf_command_MYMTNWEB_PAGELOAD = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MYMTNWEB_PAGELOAD)
logging.info(pcf_command_MYMTNWEB_PAGELOAD)

PCFCheck_command_MYMTNWEB_PAGELOAD = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MYMTNWEB_PAGELOAD,run_date,sleeptime)
logging.info(PCFCheck_command_MYMTNWEB_PAGELOAD)

branchScript_MYMTNWEB_PAGELOAD = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MYMTNWEB_PAGELOAD,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MYMTNWEB_PAGELOAD)

def branch_MYMTNWEB_PAGELOAD():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MYMTNWEB_PAGELOAD,feedname2_MYMTNWEB_PAGELOAD)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MYMTNWEB_PAGELOAD)
    x = readcsv(filepath,feedname2_MYMTNWEB_PAGELOAD)
    logging.info('branch_MYMTNWEB_PAGELOAD' + x)
    return x

Branching_MYMTNWEB_PAGELOAD = BranchPythonOperator(
    task_id='branchid_MYMTNWEB_PAGELOAD',
    python_callable=branch_MYMTNWEB_PAGELOAD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MYMTNWEB_PAGELOAD = BashOperator(
    task_id='PCF_MYMTNWEB_PAGELOAD',
    bash_command= pcf_command_MYMTNWEB_PAGELOAD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MYMTNWEB_PAGELOAD = BashOperator(
    task_id='Dedup_MYMTNWEB_PAGELOAD',
    bash_command= dedup_command_MYMTNWEB_PAGELOAD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MYMTNWEB_PAGELOAD = BashOperator(
    task_id='Dedup2_MYMTNWEB_PAGELOAD',
    bash_command= dedup_command_MYMTNWEB_PAGELOAD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MYMTNWEB_PAGELOAD = BashOperator(
    task_id='PCFCheck_MYMTNWEB_PAGELOAD',
    bash_command=PCFCheck_command_MYMTNWEB_PAGELOAD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MYMTNWEB_PAGELOAD = EmailOperator(
    task_id='faile_MYMTNWEB_PAGELOAD',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MYMTNWEB_PAGELOAD),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MYMTNWEB_PAGELOAD: PythonOperator = PythonOperator(task_id="waitForFlush_MYMTNWEB_PAGELOAD",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MYMTNWEB_PAGELOAD >> PCF_MYMTNWEB_PAGELOAD >> PCFCheck_MYMTNWEB_PAGELOAD >> delay_python_task_MYMTNWEB_PAGELOAD >> Dedup_MYMTNWEB_PAGELOAD >> Validation_End 
Branching_MYMTNWEB_PAGELOAD >> Dedup2_MYMTNWEB_PAGELOAD >> Validation_End
Branching_MYMTNWEB_PAGELOAD >> faile_MYMTNWEB_PAGELOAD
Branching_MYMTNWEB_PAGELOAD >> Validation_End


feedname_MYMTNWEB_ACTIVEUSERS = 'MYMTNWEB_ACTIVEUSERS'.upper()
feedname2_MYMTNWEB_ACTIVEUSERS = 'MYMTNWEB_ACTIVEUSERS'.lower()

dedup_command_MYMTNWEB_ACTIVEUSERS="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MYMTNWEB_ACTIVEUSERS)
logging.info(dedup_command_MYMTNWEB_ACTIVEUSERS) 

pcf_command_MYMTNWEB_ACTIVEUSERS = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MYMTNWEB_ACTIVEUSERS)
logging.info(pcf_command_MYMTNWEB_ACTIVEUSERS)

PCFCheck_command_MYMTNWEB_ACTIVEUSERS = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MYMTNWEB_ACTIVEUSERS,run_date,sleeptime)
logging.info(PCFCheck_command_MYMTNWEB_ACTIVEUSERS)

branchScript_MYMTNWEB_ACTIVEUSERS = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MYMTNWEB_ACTIVEUSERS,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MYMTNWEB_ACTIVEUSERS)

def branch_MYMTNWEB_ACTIVEUSERS():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MYMTNWEB_ACTIVEUSERS,feedname2_MYMTNWEB_ACTIVEUSERS)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MYMTNWEB_ACTIVEUSERS)
    x = readcsv(filepath,feedname2_MYMTNWEB_ACTIVEUSERS)
    logging.info('branch_MYMTNWEB_ACTIVEUSERS' + x)
    return x

Branching_MYMTNWEB_ACTIVEUSERS = BranchPythonOperator(
    task_id='branchid_MYMTNWEB_ACTIVEUSERS',
    python_callable=branch_MYMTNWEB_ACTIVEUSERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MYMTNWEB_ACTIVEUSERS = BashOperator(
    task_id='PCF_MYMTNWEB_ACTIVEUSERS',
    bash_command= pcf_command_MYMTNWEB_ACTIVEUSERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MYMTNWEB_ACTIVEUSERS = BashOperator(
    task_id='Dedup_MYMTNWEB_ACTIVEUSERS',
    bash_command= dedup_command_MYMTNWEB_ACTIVEUSERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MYMTNWEB_ACTIVEUSERS = BashOperator(
    task_id='Dedup2_MYMTNWEB_ACTIVEUSERS',
    bash_command= dedup_command_MYMTNWEB_ACTIVEUSERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MYMTNWEB_ACTIVEUSERS = BashOperator(
    task_id='PCFCheck_MYMTNWEB_ACTIVEUSERS',
    bash_command=PCFCheck_command_MYMTNWEB_ACTIVEUSERS,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MYMTNWEB_ACTIVEUSERS = EmailOperator(
    task_id='faile_MYMTNWEB_ACTIVEUSERS',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MYMTNWEB_ACTIVEUSERS),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MYMTNWEB_ACTIVEUSERS: PythonOperator = PythonOperator(task_id="waitForFlush_MYMTNWEB_ACTIVEUSERS",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MYMTNWEB_ACTIVEUSERS >> PCF_MYMTNWEB_ACTIVEUSERS >> PCFCheck_MYMTNWEB_ACTIVEUSERS >> delay_python_task_MYMTNWEB_ACTIVEUSERS >> Dedup_MYMTNWEB_ACTIVEUSERS >> Validation_End 
Branching_MYMTNWEB_ACTIVEUSERS >> Dedup2_MYMTNWEB_ACTIVEUSERS >> Validation_End
Branching_MYMTNWEB_ACTIVEUSERS >> faile_MYMTNWEB_ACTIVEUSERS
Branching_MYMTNWEB_ACTIVEUSERS >> Validation_End


feedname_FLYTXT_CAMPAIGN_EVENTS_DATA = 'FLYTXT_CAMPAIGN_EVENTS_DATA'.upper()
feedname2_FLYTXT_CAMPAIGN_EVENTS_DATA = 'FLYTXT_CAMPAIGN_EVENTS_DATA'.lower()

dedup_command_FLYTXT_CAMPAIGN_EVENTS_DATA="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_FLYTXT_CAMPAIGN_EVENTS_DATA)
logging.info(dedup_command_FLYTXT_CAMPAIGN_EVENTS_DATA) 

pcf_command_FLYTXT_CAMPAIGN_EVENTS_DATA = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_FLYTXT_CAMPAIGN_EVENTS_DATA)
logging.info(pcf_command_FLYTXT_CAMPAIGN_EVENTS_DATA)

PCFCheck_command_FLYTXT_CAMPAIGN_EVENTS_DATA = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_FLYTXT_CAMPAIGN_EVENTS_DATA,run_date,sleeptime)
logging.info(PCFCheck_command_FLYTXT_CAMPAIGN_EVENTS_DATA)

branchScript_FLYTXT_CAMPAIGN_EVENTS_DATA = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_FLYTXT_CAMPAIGN_EVENTS_DATA,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_FLYTXT_CAMPAIGN_EVENTS_DATA)

def branch_FLYTXT_CAMPAIGN_EVENTS_DATA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_FLYTXT_CAMPAIGN_EVENTS_DATA,feedname2_FLYTXT_CAMPAIGN_EVENTS_DATA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_FLYTXT_CAMPAIGN_EVENTS_DATA)
    x = readcsv(filepath,feedname2_FLYTXT_CAMPAIGN_EVENTS_DATA)
    logging.info('branch_FLYTXT_CAMPAIGN_EVENTS_DATA' + x)
    return x

Branching_FLYTXT_CAMPAIGN_EVENTS_DATA = BranchPythonOperator(
    task_id='branchid_FLYTXT_CAMPAIGN_EVENTS_DATA',
    python_callable=branch_FLYTXT_CAMPAIGN_EVENTS_DATA,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_FLYTXT_CAMPAIGN_EVENTS_DATA = BashOperator(
    task_id='PCF_FLYTXT_CAMPAIGN_EVENTS_DATA',
    bash_command= pcf_command_FLYTXT_CAMPAIGN_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_FLYTXT_CAMPAIGN_EVENTS_DATA = BashOperator(
    task_id='Dedup_FLYTXT_CAMPAIGN_EVENTS_DATA',
    bash_command= dedup_command_FLYTXT_CAMPAIGN_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_FLYTXT_CAMPAIGN_EVENTS_DATA = BashOperator(
    task_id='Dedup2_FLYTXT_CAMPAIGN_EVENTS_DATA',
    bash_command= dedup_command_FLYTXT_CAMPAIGN_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_FLYTXT_CAMPAIGN_EVENTS_DATA = BashOperator(
    task_id='PCFCheck_FLYTXT_CAMPAIGN_EVENTS_DATA',
    bash_command=PCFCheck_command_FLYTXT_CAMPAIGN_EVENTS_DATA,
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser',
    priority_weight=1
)

faile_FLYTXT_CAMPAIGN_EVENTS_DATA = EmailOperator(
    task_id='faile_FLYTXT_CAMPAIGN_EVENTS_DATA',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_FLYTXT_CAMPAIGN_EVENTS_DATA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_FLYTXT_CAMPAIGN_EVENTS_DATA: PythonOperator = PythonOperator(task_id="waitForFlush_FLYTXT_CAMPAIGN_EVENTS_DATA",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_FLYTXT_CAMPAIGN_EVENTS_DATA >> PCF_FLYTXT_CAMPAIGN_EVENTS_DATA >> PCFCheck_FLYTXT_CAMPAIGN_EVENTS_DATA >> delay_python_task_FLYTXT_CAMPAIGN_EVENTS_DATA >> Dedup_FLYTXT_CAMPAIGN_EVENTS_DATA >> Validation_End 
Branching_FLYTXT_CAMPAIGN_EVENTS_DATA >> Dedup2_FLYTXT_CAMPAIGN_EVENTS_DATA >> Validation_End
Branching_FLYTXT_CAMPAIGN_EVENTS_DATA >> faile_FLYTXT_CAMPAIGN_EVENTS_DATA
Branching_FLYTXT_CAMPAIGN_EVENTS_DATA >> Validation_End


feedname_CLM_WBO = 'CLM_WBO'.upper()
feedname2_CLM_WBO = 'CLM_WBO'.lower()

dedup_command_CLM_WBO="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CLM_WBO)
logging.info(dedup_command_CLM_WBO) 

pcf_command_CLM_WBO = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CLM_WBO)
logging.info(pcf_command_CLM_WBO)

PCFCheck_command_CLM_WBO = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CLM_WBO,run_date,sleeptime)
logging.info(PCFCheck_command_CLM_WBO)

branchScript_CLM_WBO = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CLM_WBO,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CLM_WBO)

def branch_CLM_WBO():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CLM_WBO,feedname2_CLM_WBO)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CLM_WBO)
    x = readcsv(filepath,feedname2_CLM_WBO)
    logging.info('branch_CLM_WBO' + x)
    return x

Branching_CLM_WBO = BranchPythonOperator(
    task_id='branchid_CLM_WBO',
    python_callable=branch_CLM_WBO,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CLM_WBO = BashOperator(
    task_id='PCF_CLM_WBO',
    bash_command= pcf_command_CLM_WBO,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CLM_WBO = BashOperator(
    task_id='Dedup_CLM_WBO',
    bash_command= dedup_command_CLM_WBO,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CLM_WBO = BashOperator(
    task_id='Dedup2_CLM_WBO',
    bash_command= dedup_command_CLM_WBO,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CLM_WBO = BashOperator(
    task_id='PCFCheck_CLM_WBO',
    bash_command=PCFCheck_command_CLM_WBO,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CLM_WBO = EmailOperator(
    task_id='faile_CLM_WBO',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CLM_WBO),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CLM_WBO: PythonOperator = PythonOperator(task_id="waitForFlush_CLM_WBO",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CLM_WBO >> PCF_CLM_WBO >> PCFCheck_CLM_WBO >> delay_python_task_CLM_WBO >> Dedup_CLM_WBO >> Validation_End 
Branching_CLM_WBO >> Dedup2_CLM_WBO >> Validation_End
Branching_CLM_WBO >> faile_CLM_WBO
Branching_CLM_WBO >> Validation_End


feedname_PROVISIONING_LOG = 'PROVISIONING_LOG'.upper()
feedname2_PROVISIONING_LOG = 'PROVISIONING_LOG'.lower()

dedup_command_PROVISIONING_LOG="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_PROVISIONING_LOG)
logging.info(dedup_command_PROVISIONING_LOG) 

pcf_command_PROVISIONING_LOG = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_PROVISIONING_LOG)
logging.info(pcf_command_PROVISIONING_LOG)

PCFCheck_command_PROVISIONING_LOG = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_PROVISIONING_LOG,run_date,sleeptime)
logging.info(PCFCheck_command_PROVISIONING_LOG)

branchScript_PROVISIONING_LOG = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_PROVISIONING_LOG,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_PROVISIONING_LOG)

def branch_PROVISIONING_LOG():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_PROVISIONING_LOG,feedname2_PROVISIONING_LOG)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_PROVISIONING_LOG)
    x = readcsv(filepath,feedname2_PROVISIONING_LOG)
    logging.info('branch_PROVISIONING_LOG' + x)
    return x

Branching_PROVISIONING_LOG = BranchPythonOperator(
    task_id='branchid_PROVISIONING_LOG',
    python_callable=branch_PROVISIONING_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_PROVISIONING_LOG = BashOperator(
    task_id='PCF_PROVISIONING_LOG',
    bash_command= pcf_command_PROVISIONING_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_PROVISIONING_LOG = BashOperator(
    task_id='Dedup_PROVISIONING_LOG',
    bash_command= dedup_command_PROVISIONING_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_PROVISIONING_LOG = BashOperator(
    task_id='Dedup2_PROVISIONING_LOG',
    bash_command= dedup_command_PROVISIONING_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_PROVISIONING_LOG = BashOperator(
    task_id='PCFCheck_PROVISIONING_LOG',
    bash_command=PCFCheck_command_PROVISIONING_LOG,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_PROVISIONING_LOG = EmailOperator(
    task_id='faile_PROVISIONING_LOG',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_PROVISIONING_LOG),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_PROVISIONING_LOG: PythonOperator = PythonOperator(task_id="waitForFlush_PROVISIONING_LOG",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_PROVISIONING_LOG >> PCF_PROVISIONING_LOG >> PCFCheck_PROVISIONING_LOG >> delay_python_task_PROVISIONING_LOG >> Dedup_PROVISIONING_LOG >> Validation_End 
Branching_PROVISIONING_LOG >> Dedup2_PROVISIONING_LOG >> Validation_End
Branching_PROVISIONING_LOG >> faile_PROVISIONING_LOG
Branching_PROVISIONING_LOG >> Validation_End


feedname_STAT_BILL_USSD = 'STAT_BILL_USSD'.upper()
feedname2_STAT_BILL_USSD = 'STAT_BILL_USSD'.lower()

dedup_command_STAT_BILL_USSD="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_STAT_BILL_USSD)
logging.info(dedup_command_STAT_BILL_USSD) 

pcf_command_STAT_BILL_USSD = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_STAT_BILL_USSD)
logging.info(pcf_command_STAT_BILL_USSD)

PCFCheck_command_STAT_BILL_USSD = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_STAT_BILL_USSD,run_date,sleeptime)
logging.info(PCFCheck_command_STAT_BILL_USSD)

branchScript_STAT_BILL_USSD = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_STAT_BILL_USSD,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_STAT_BILL_USSD)

def branch_STAT_BILL_USSD():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_STAT_BILL_USSD,feedname2_STAT_BILL_USSD)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_STAT_BILL_USSD)
    x = readcsv(filepath,feedname2_STAT_BILL_USSD)
    logging.info('branch_STAT_BILL_USSD' + x)
    return x

Branching_STAT_BILL_USSD = BranchPythonOperator(
    task_id='branchid_STAT_BILL_USSD',
    python_callable=branch_STAT_BILL_USSD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_STAT_BILL_USSD = BashOperator(
    task_id='PCF_STAT_BILL_USSD',
    bash_command= pcf_command_STAT_BILL_USSD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_STAT_BILL_USSD = BashOperator(
    task_id='Dedup_STAT_BILL_USSD',
    bash_command= dedup_command_STAT_BILL_USSD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_STAT_BILL_USSD = BashOperator(
    task_id='Dedup2_STAT_BILL_USSD',
    bash_command= dedup_command_STAT_BILL_USSD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_STAT_BILL_USSD = BashOperator(
    task_id='PCFCheck_STAT_BILL_USSD',
    bash_command=PCFCheck_command_STAT_BILL_USSD,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_STAT_BILL_USSD = EmailOperator(
    task_id='faile_STAT_BILL_USSD',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_STAT_BILL_USSD),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_STAT_BILL_USSD: PythonOperator = PythonOperator(task_id="waitForFlush_STAT_BILL_USSD",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_STAT_BILL_USSD >> PCF_STAT_BILL_USSD >> PCFCheck_STAT_BILL_USSD >> delay_python_task_STAT_BILL_USSD >> Dedup_STAT_BILL_USSD >> Validation_End 
Branching_STAT_BILL_USSD >> Dedup2_STAT_BILL_USSD >> Validation_End
Branching_STAT_BILL_USSD >> faile_STAT_BILL_USSD
Branching_STAT_BILL_USSD >> Validation_End


feedname_ESM_SIMSWAP_METRIC = 'ESM_SIMSWAP_METRIC'.upper()
feedname2_ESM_SIMSWAP_METRIC = 'ESM_SIMSWAP_METRIC'.lower()

dedup_command_ESM_SIMSWAP_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_SIMSWAP_METRIC)
logging.info(dedup_command_ESM_SIMSWAP_METRIC) 

pcf_command_ESM_SIMSWAP_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_SIMSWAP_METRIC)
logging.info(pcf_command_ESM_SIMSWAP_METRIC)

PCFCheck_command_ESM_SIMSWAP_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_SIMSWAP_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_SIMSWAP_METRIC)

branchScript_ESM_SIMSWAP_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_SIMSWAP_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_SIMSWAP_METRIC)

def branch_ESM_SIMSWAP_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_SIMSWAP_METRIC,feedname2_ESM_SIMSWAP_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_SIMSWAP_METRIC)
    x = readcsv(filepath,feedname2_ESM_SIMSWAP_METRIC)
    logging.info('branch_ESM_SIMSWAP_METRIC' + x)
    return x

Branching_ESM_SIMSWAP_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_SIMSWAP_METRIC',
    python_callable=branch_ESM_SIMSWAP_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_SIMSWAP_METRIC = BashOperator(
    task_id='PCF_ESM_SIMSWAP_METRIC',
    bash_command= pcf_command_ESM_SIMSWAP_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_SIMSWAP_METRIC = BashOperator(
    task_id='Dedup_ESM_SIMSWAP_METRIC',
    bash_command= dedup_command_ESM_SIMSWAP_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_SIMSWAP_METRIC = BashOperator(
    task_id='Dedup2_ESM_SIMSWAP_METRIC',
    bash_command= dedup_command_ESM_SIMSWAP_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_SIMSWAP_METRIC = BashOperator(
    task_id='PCFCheck_ESM_SIMSWAP_METRIC',
    bash_command=PCFCheck_command_ESM_SIMSWAP_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_SIMSWAP_METRIC = EmailOperator(
    task_id='faile_ESM_SIMSWAP_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_SIMSWAP_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_SIMSWAP_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_SIMSWAP_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_SIMSWAP_METRIC >> PCF_ESM_SIMSWAP_METRIC >> PCFCheck_ESM_SIMSWAP_METRIC >> delay_python_task_ESM_SIMSWAP_METRIC >> Dedup_ESM_SIMSWAP_METRIC >> Validation_End 
Branching_ESM_SIMSWAP_METRIC >> Dedup2_ESM_SIMSWAP_METRIC >> Validation_End
Branching_ESM_SIMSWAP_METRIC >> faile_ESM_SIMSWAP_METRIC
Branching_ESM_SIMSWAP_METRIC >> Validation_End


feedname_ESM_EXTRATIME_METRIC = 'ESM_EXTRATIME_METRIC'.upper()
feedname2_ESM_EXTRATIME_METRIC = 'ESM_EXTRATIME_METRIC'.lower()

dedup_command_ESM_EXTRATIME_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_EXTRATIME_METRIC)
logging.info(dedup_command_ESM_EXTRATIME_METRIC) 

pcf_command_ESM_EXTRATIME_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_EXTRATIME_METRIC)
logging.info(pcf_command_ESM_EXTRATIME_METRIC)

PCFCheck_command_ESM_EXTRATIME_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_EXTRATIME_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_EXTRATIME_METRIC)

branchScript_ESM_EXTRATIME_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_EXTRATIME_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_EXTRATIME_METRIC)

def branch_ESM_EXTRATIME_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_EXTRATIME_METRIC,feedname2_ESM_EXTRATIME_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_EXTRATIME_METRIC)
    x = readcsv(filepath,feedname2_ESM_EXTRATIME_METRIC)
    logging.info('branch_ESM_EXTRATIME_METRIC' + x)
    return x

Branching_ESM_EXTRATIME_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_EXTRATIME_METRIC',
    python_callable=branch_ESM_EXTRATIME_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_EXTRATIME_METRIC = BashOperator(
    task_id='PCF_ESM_EXTRATIME_METRIC',
    bash_command= pcf_command_ESM_EXTRATIME_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_EXTRATIME_METRIC = BashOperator(
    task_id='Dedup_ESM_EXTRATIME_METRIC',
    bash_command= dedup_command_ESM_EXTRATIME_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_EXTRATIME_METRIC = BashOperator(
    task_id='Dedup2_ESM_EXTRATIME_METRIC',
    bash_command= dedup_command_ESM_EXTRATIME_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_EXTRATIME_METRIC = BashOperator(
    task_id='PCFCheck_ESM_EXTRATIME_METRIC',
    bash_command=PCFCheck_command_ESM_EXTRATIME_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_EXTRATIME_METRIC = EmailOperator(
    task_id='faile_ESM_EXTRATIME_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_EXTRATIME_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_EXTRATIME_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_EXTRATIME_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_EXTRATIME_METRIC >> PCF_ESM_EXTRATIME_METRIC >> PCFCheck_ESM_EXTRATIME_METRIC >> delay_python_task_ESM_EXTRATIME_METRIC >> Dedup_ESM_EXTRATIME_METRIC >> Validation_End 
Branching_ESM_EXTRATIME_METRIC >> Dedup2_ESM_EXTRATIME_METRIC >> Validation_End
Branching_ESM_EXTRATIME_METRIC >> faile_ESM_EXTRATIME_METRIC
Branching_ESM_EXTRATIME_METRIC >> Validation_End


feedname_ESM_DYA_METRIC = 'ESM_DYA_METRIC'.upper()
feedname2_ESM_DYA_METRIC = 'ESM_DYA_METRIC'.lower()

dedup_command_ESM_DYA_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_DYA_METRIC)
logging.info(dedup_command_ESM_DYA_METRIC) 

pcf_command_ESM_DYA_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_DYA_METRIC)
logging.info(pcf_command_ESM_DYA_METRIC)

PCFCheck_command_ESM_DYA_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_DYA_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_DYA_METRIC)

branchScript_ESM_DYA_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_DYA_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_DYA_METRIC)

def branch_ESM_DYA_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_DYA_METRIC,feedname2_ESM_DYA_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_DYA_METRIC)
    x = readcsv(filepath,feedname2_ESM_DYA_METRIC)
    logging.info('branch_ESM_DYA_METRIC' + x)
    return x

Branching_ESM_DYA_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_DYA_METRIC',
    python_callable=branch_ESM_DYA_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_DYA_METRIC = BashOperator(
    task_id='PCF_ESM_DYA_METRIC',
    bash_command= pcf_command_ESM_DYA_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_DYA_METRIC = BashOperator(
    task_id='Dedup_ESM_DYA_METRIC',
    bash_command= dedup_command_ESM_DYA_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_DYA_METRIC = BashOperator(
    task_id='Dedup2_ESM_DYA_METRIC',
    bash_command= dedup_command_ESM_DYA_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_DYA_METRIC = BashOperator(
    task_id='PCFCheck_ESM_DYA_METRIC',
    bash_command=PCFCheck_command_ESM_DYA_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_DYA_METRIC = EmailOperator(
    task_id='faile_ESM_DYA_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_DYA_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_DYA_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_DYA_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_DYA_METRIC >> PCF_ESM_DYA_METRIC >> PCFCheck_ESM_DYA_METRIC >> delay_python_task_ESM_DYA_METRIC >> Dedup_ESM_DYA_METRIC >> Validation_End 
Branching_ESM_DYA_METRIC >> Dedup2_ESM_DYA_METRIC >> Validation_End
Branching_ESM_DYA_METRIC >> faile_ESM_DYA_METRIC
Branching_ESM_DYA_METRIC >> Validation_End


feedname_ESM_PMT_METRIC = 'ESM_PMT_METRIC'.upper()
feedname2_ESM_PMT_METRIC = 'ESM_PMT_METRIC'.lower()

dedup_command_ESM_PMT_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_PMT_METRIC)
logging.info(dedup_command_ESM_PMT_METRIC) 

pcf_command_ESM_PMT_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_PMT_METRIC)
logging.info(pcf_command_ESM_PMT_METRIC)

PCFCheck_command_ESM_PMT_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_PMT_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_PMT_METRIC)

branchScript_ESM_PMT_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_PMT_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_PMT_METRIC)

def branch_ESM_PMT_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_PMT_METRIC,feedname2_ESM_PMT_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_PMT_METRIC)
    x = readcsv(filepath,feedname2_ESM_PMT_METRIC)
    logging.info('branch_ESM_PMT_METRIC' + x)
    return x

Branching_ESM_PMT_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_PMT_METRIC',
    python_callable=branch_ESM_PMT_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_PMT_METRIC = BashOperator(
    task_id='PCF_ESM_PMT_METRIC',
    bash_command= pcf_command_ESM_PMT_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_PMT_METRIC = BashOperator(
    task_id='Dedup_ESM_PMT_METRIC',
    bash_command= dedup_command_ESM_PMT_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_PMT_METRIC = BashOperator(
    task_id='Dedup2_ESM_PMT_METRIC',
    bash_command= dedup_command_ESM_PMT_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_PMT_METRIC = BashOperator(
    task_id='PCFCheck_ESM_PMT_METRIC',
    bash_command=PCFCheck_command_ESM_PMT_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_PMT_METRIC = EmailOperator(
    task_id='faile_ESM_PMT_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_PMT_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_PMT_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_PMT_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_PMT_METRIC >> PCF_ESM_PMT_METRIC >> PCFCheck_ESM_PMT_METRIC >> delay_python_task_ESM_PMT_METRIC >> Dedup_ESM_PMT_METRIC >> Validation_End 
Branching_ESM_PMT_METRIC >> Dedup2_ESM_PMT_METRIC >> Validation_End
Branching_ESM_PMT_METRIC >> faile_ESM_PMT_METRIC
Branching_ESM_PMT_METRIC >> Validation_End


feedname_ESM_REFILL_METRIC = 'ESM_REFILL_METRIC'.upper()
feedname2_ESM_REFILL_METRIC = 'ESM_REFILL_METRIC'.lower()

dedup_command_ESM_REFILL_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_REFILL_METRIC)
logging.info(dedup_command_ESM_REFILL_METRIC) 

pcf_command_ESM_REFILL_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_REFILL_METRIC)
logging.info(pcf_command_ESM_REFILL_METRIC)

PCFCheck_command_ESM_REFILL_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_REFILL_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_REFILL_METRIC)

branchScript_ESM_REFILL_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_REFILL_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_REFILL_METRIC)

def branch_ESM_REFILL_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_REFILL_METRIC,feedname2_ESM_REFILL_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_REFILL_METRIC)
    x = readcsv(filepath,feedname2_ESM_REFILL_METRIC)
    logging.info('branch_ESM_REFILL_METRIC' + x)
    return x

Branching_ESM_REFILL_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_REFILL_METRIC',
    python_callable=branch_ESM_REFILL_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_REFILL_METRIC = BashOperator(
    task_id='PCF_ESM_REFILL_METRIC',
    bash_command= pcf_command_ESM_REFILL_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_REFILL_METRIC = BashOperator(
    task_id='Dedup_ESM_REFILL_METRIC',
    bash_command= dedup_command_ESM_REFILL_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_REFILL_METRIC = BashOperator(
    task_id='Dedup2_ESM_REFILL_METRIC',
    bash_command= dedup_command_ESM_REFILL_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_REFILL_METRIC = BashOperator(
    task_id='PCFCheck_ESM_REFILL_METRIC',
    bash_command=PCFCheck_command_ESM_REFILL_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_REFILL_METRIC = EmailOperator(
    task_id='faile_ESM_REFILL_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_REFILL_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_REFILL_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_REFILL_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_REFILL_METRIC >> PCF_ESM_REFILL_METRIC >> PCFCheck_ESM_REFILL_METRIC >> delay_python_task_ESM_REFILL_METRIC >> Dedup_ESM_REFILL_METRIC >> Validation_End 
Branching_ESM_REFILL_METRIC >> Dedup2_ESM_REFILL_METRIC >> Validation_End
Branching_ESM_REFILL_METRIC >> faile_ESM_REFILL_METRIC
Branching_ESM_REFILL_METRIC >> Validation_End


feedname_ESM_SIM_REG_METRIC = 'ESM_SIM_REG_METRIC'.upper()
feedname2_ESM_SIM_REG_METRIC = 'ESM_SIM_REG_METRIC'.lower()

dedup_command_ESM_SIM_REG_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_SIM_REG_METRIC)
logging.info(dedup_command_ESM_SIM_REG_METRIC) 

pcf_command_ESM_SIM_REG_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_SIM_REG_METRIC)
logging.info(pcf_command_ESM_SIM_REG_METRIC)

PCFCheck_command_ESM_SIM_REG_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_SIM_REG_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_SIM_REG_METRIC)

branchScript_ESM_SIM_REG_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_SIM_REG_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_SIM_REG_METRIC)

def branch_ESM_SIM_REG_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_SIM_REG_METRIC,feedname2_ESM_SIM_REG_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_SIM_REG_METRIC)
    x = readcsv(filepath,feedname2_ESM_SIM_REG_METRIC)
    logging.info('branch_ESM_SIM_REG_METRIC' + x)
    return x

Branching_ESM_SIM_REG_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_SIM_REG_METRIC',
    python_callable=branch_ESM_SIM_REG_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_SIM_REG_METRIC = BashOperator(
    task_id='PCF_ESM_SIM_REG_METRIC',
    bash_command= pcf_command_ESM_SIM_REG_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_SIM_REG_METRIC = BashOperator(
    task_id='Dedup_ESM_SIM_REG_METRIC',
    bash_command= dedup_command_ESM_SIM_REG_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_SIM_REG_METRIC = BashOperator(
    task_id='Dedup2_ESM_SIM_REG_METRIC',
    bash_command= dedup_command_ESM_SIM_REG_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_SIM_REG_METRIC = BashOperator(
    task_id='PCFCheck_ESM_SIM_REG_METRIC',
    bash_command=PCFCheck_command_ESM_SIM_REG_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_SIM_REG_METRIC = EmailOperator(
    task_id='faile_ESM_SIM_REG_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_SIM_REG_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_SIM_REG_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_SIM_REG_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_SIM_REG_METRIC >> PCF_ESM_SIM_REG_METRIC >> PCFCheck_ESM_SIM_REG_METRIC >> delay_python_task_ESM_SIM_REG_METRIC >> Dedup_ESM_SIM_REG_METRIC >> Validation_End 
Branching_ESM_SIM_REG_METRIC >> Dedup2_ESM_SIM_REG_METRIC >> Validation_End
Branching_ESM_SIM_REG_METRIC >> faile_ESM_SIM_REG_METRIC
Branching_ESM_SIM_REG_METRIC >> Validation_End


feedname_ESM_SUB_METRIC = 'ESM_SUB_METRIC'.upper()
feedname2_ESM_SUB_METRIC = 'ESM_SUB_METRIC'.lower()

dedup_command_ESM_SUB_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_SUB_METRIC)
logging.info(dedup_command_ESM_SUB_METRIC) 

pcf_command_ESM_SUB_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_SUB_METRIC)
logging.info(pcf_command_ESM_SUB_METRIC)

PCFCheck_command_ESM_SUB_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_SUB_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_SUB_METRIC)

branchScript_ESM_SUB_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_SUB_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_SUB_METRIC)

def branch_ESM_SUB_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_SUB_METRIC,feedname2_ESM_SUB_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_SUB_METRIC)
    x = readcsv(filepath,feedname2_ESM_SUB_METRIC)
    logging.info('branch_ESM_SUB_METRIC' + x)
    return x

Branching_ESM_SUB_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_SUB_METRIC',
    python_callable=branch_ESM_SUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_SUB_METRIC = BashOperator(
    task_id='PCF_ESM_SUB_METRIC',
    bash_command= pcf_command_ESM_SUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_SUB_METRIC = BashOperator(
    task_id='Dedup_ESM_SUB_METRIC',
    bash_command= dedup_command_ESM_SUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_SUB_METRIC = BashOperator(
    task_id='Dedup2_ESM_SUB_METRIC',
    bash_command= dedup_command_ESM_SUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_SUB_METRIC = BashOperator(
    task_id='PCFCheck_ESM_SUB_METRIC',
    bash_command=PCFCheck_command_ESM_SUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_SUB_METRIC = EmailOperator(
    task_id='faile_ESM_SUB_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_SUB_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_SUB_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_SUB_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_SUB_METRIC >> PCF_ESM_SUB_METRIC >> PCFCheck_ESM_SUB_METRIC >> delay_python_task_ESM_SUB_METRIC >> Dedup_ESM_SUB_METRIC >> Validation_End 
Branching_ESM_SUB_METRIC >> Dedup2_ESM_SUB_METRIC >> Validation_End
Branching_ESM_SUB_METRIC >> faile_ESM_SUB_METRIC
Branching_ESM_SUB_METRIC >> Validation_End


feedname_ESM_UNSUB_METRIC = 'ESM_UNSUB_METRIC'.upper()
feedname2_ESM_UNSUB_METRIC = 'ESM_UNSUB_METRIC'.lower()

dedup_command_ESM_UNSUB_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_UNSUB_METRIC)
logging.info(dedup_command_ESM_UNSUB_METRIC) 

pcf_command_ESM_UNSUB_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_UNSUB_METRIC)
logging.info(pcf_command_ESM_UNSUB_METRIC)

PCFCheck_command_ESM_UNSUB_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_UNSUB_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_UNSUB_METRIC)

branchScript_ESM_UNSUB_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_UNSUB_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_UNSUB_METRIC)

def branch_ESM_UNSUB_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_UNSUB_METRIC,feedname2_ESM_UNSUB_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_UNSUB_METRIC)
    x = readcsv(filepath,feedname2_ESM_UNSUB_METRIC)
    logging.info('branch_ESM_UNSUB_METRIC' + x)
    return x

Branching_ESM_UNSUB_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_UNSUB_METRIC',
    python_callable=branch_ESM_UNSUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_UNSUB_METRIC = BashOperator(
    task_id='PCF_ESM_UNSUB_METRIC',
    bash_command= pcf_command_ESM_UNSUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_UNSUB_METRIC = BashOperator(
    task_id='Dedup_ESM_UNSUB_METRIC',
    bash_command= dedup_command_ESM_UNSUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_UNSUB_METRIC = BashOperator(
    task_id='Dedup2_ESM_UNSUB_METRIC',
    bash_command= dedup_command_ESM_UNSUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_UNSUB_METRIC = BashOperator(
    task_id='PCFCheck_ESM_UNSUB_METRIC',
    bash_command=PCFCheck_command_ESM_UNSUB_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_UNSUB_METRIC = EmailOperator(
    task_id='faile_ESM_UNSUB_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_UNSUB_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_UNSUB_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_UNSUB_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_UNSUB_METRIC >> PCF_ESM_UNSUB_METRIC >> PCFCheck_ESM_UNSUB_METRIC >> delay_python_task_ESM_UNSUB_METRIC >> Dedup_ESM_UNSUB_METRIC >> Validation_End 
Branching_ESM_UNSUB_METRIC >> Dedup2_ESM_UNSUB_METRIC >> Validation_End
Branching_ESM_UNSUB_METRIC >> faile_ESM_UNSUB_METRIC
Branching_ESM_UNSUB_METRIC >> Validation_End


feedname_ESM_VTU_VENDING_METRIC = 'ESM_VTU_VENDING_METRIC'.upper()
feedname2_ESM_VTU_VENDING_METRIC = 'ESM_VTU_VENDING_METRIC'.lower()

dedup_command_ESM_VTU_VENDING_METRIC="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_ESM_VTU_VENDING_METRIC)
logging.info(dedup_command_ESM_VTU_VENDING_METRIC) 

pcf_command_ESM_VTU_VENDING_METRIC = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_ESM_VTU_VENDING_METRIC)
logging.info(pcf_command_ESM_VTU_VENDING_METRIC)

PCFCheck_command_ESM_VTU_VENDING_METRIC = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_ESM_VTU_VENDING_METRIC,run_date,sleeptime)
logging.info(PCFCheck_command_ESM_VTU_VENDING_METRIC)

branchScript_ESM_VTU_VENDING_METRIC = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_ESM_VTU_VENDING_METRIC,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_ESM_VTU_VENDING_METRIC)

def branch_ESM_VTU_VENDING_METRIC():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_ESM_VTU_VENDING_METRIC,feedname2_ESM_VTU_VENDING_METRIC)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_ESM_VTU_VENDING_METRIC)
    x = readcsv(filepath,feedname2_ESM_VTU_VENDING_METRIC)
    logging.info('branch_ESM_VTU_VENDING_METRIC' + x)
    return x

Branching_ESM_VTU_VENDING_METRIC = BranchPythonOperator(
    task_id='branchid_ESM_VTU_VENDING_METRIC',
    python_callable=branch_ESM_VTU_VENDING_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='PCF_ESM_VTU_VENDING_METRIC',
    bash_command= pcf_command_ESM_VTU_VENDING_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='Dedup_ESM_VTU_VENDING_METRIC',
    bash_command= dedup_command_ESM_VTU_VENDING_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='Dedup2_ESM_VTU_VENDING_METRIC',
    bash_command= dedup_command_ESM_VTU_VENDING_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_ESM_VTU_VENDING_METRIC = BashOperator(
    task_id='PCFCheck_ESM_VTU_VENDING_METRIC',
    bash_command=PCFCheck_command_ESM_VTU_VENDING_METRIC,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_ESM_VTU_VENDING_METRIC = EmailOperator(
    task_id='faile_ESM_VTU_VENDING_METRIC',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_ESM_VTU_VENDING_METRIC),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_ESM_VTU_VENDING_METRIC: PythonOperator = PythonOperator(task_id="waitForFlush_ESM_VTU_VENDING_METRIC",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_ESM_VTU_VENDING_METRIC >> PCF_ESM_VTU_VENDING_METRIC >> PCFCheck_ESM_VTU_VENDING_METRIC >> delay_python_task_ESM_VTU_VENDING_METRIC >> Dedup_ESM_VTU_VENDING_METRIC >> Validation_End 
Branching_ESM_VTU_VENDING_METRIC >> Dedup2_ESM_VTU_VENDING_METRIC >> Validation_End
Branching_ESM_VTU_VENDING_METRIC >> faile_ESM_VTU_VENDING_METRIC
Branching_ESM_VTU_VENDING_METRIC >> Validation_End


feedname_SMART_APP_CDR = 'SMART_APP_CDR'.upper()
feedname2_SMART_APP_CDR = 'SMART_APP_CDR'.lower()

dedup_command_SMART_APP_CDR="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SMART_APP_CDR)
logging.info(dedup_command_SMART_APP_CDR) 

pcf_command_SMART_APP_CDR = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SMART_APP_CDR)
logging.info(pcf_command_SMART_APP_CDR)

PCFCheck_command_SMART_APP_CDR = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SMART_APP_CDR,run_date,sleeptime)
logging.info(PCFCheck_command_SMART_APP_CDR)

branchScript_SMART_APP_CDR = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SMART_APP_CDR,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SMART_APP_CDR)

def branch_SMART_APP_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SMART_APP_CDR,feedname2_SMART_APP_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SMART_APP_CDR)
    x = readcsv(filepath,feedname2_SMART_APP_CDR)
    logging.info('branch_SMART_APP_CDR' + x)
    return x

Branching_SMART_APP_CDR = BranchPythonOperator(
    task_id='branchid_SMART_APP_CDR',
    python_callable=branch_SMART_APP_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SMART_APP_CDR = BashOperator(
    task_id='PCF_SMART_APP_CDR',
    bash_command= pcf_command_SMART_APP_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SMART_APP_CDR = BashOperator(
    task_id='Dedup_SMART_APP_CDR',
    bash_command= dedup_command_SMART_APP_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SMART_APP_CDR = BashOperator(
    task_id='Dedup2_SMART_APP_CDR',
    bash_command= dedup_command_SMART_APP_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SMART_APP_CDR = BashOperator(
    task_id='PCFCheck_SMART_APP_CDR',
    bash_command=PCFCheck_command_SMART_APP_CDR,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SMART_APP_CDR = EmailOperator(
    task_id='faile_SMART_APP_CDR',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SMART_APP_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SMART_APP_CDR: PythonOperator = PythonOperator(task_id="waitForFlush_SMART_APP_CDR",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SMART_APP_CDR >> PCF_SMART_APP_CDR >> PCFCheck_SMART_APP_CDR >> delay_python_task_SMART_APP_CDR >> Dedup_SMART_APP_CDR >> Validation_End 
Branching_SMART_APP_CDR >> Dedup2_SMART_APP_CDR >> Validation_End
Branching_SMART_APP_CDR >> faile_SMART_APP_CDR
Branching_SMART_APP_CDR >> Validation_End


feedname_CGW_API = 'CGW_API'.upper()
feedname2_CGW_API = 'CGW_API'.lower()

dedup_command_CGW_API="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_CGW_API)
logging.info(dedup_command_CGW_API) 

pcf_command_CGW_API = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_CGW_API)
logging.info(pcf_command_CGW_API)

PCFCheck_command_CGW_API = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_CGW_API,run_date,sleeptime)
logging.info(PCFCheck_command_CGW_API)

branchScript_CGW_API = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_CGW_API,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_CGW_API)

def branch_CGW_API():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_CGW_API,feedname2_CGW_API)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_CGW_API)
    x = readcsv(filepath,feedname2_CGW_API)
    logging.info('branch_CGW_API' + x)
    return x

Branching_CGW_API = BranchPythonOperator(
    task_id='branchid_CGW_API',
    python_callable=branch_CGW_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_CGW_API = BashOperator(
    task_id='PCF_CGW_API',
    bash_command= pcf_command_CGW_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_CGW_API = BashOperator(
    task_id='Dedup_CGW_API',
    bash_command= dedup_command_CGW_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_CGW_API = BashOperator(
    task_id='Dedup2_CGW_API',
    bash_command= dedup_command_CGW_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_CGW_API = BashOperator(
    task_id='PCFCheck_CGW_API',
    bash_command=PCFCheck_command_CGW_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_CGW_API = EmailOperator(
    task_id='faile_CGW_API',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_CGW_API),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_CGW_API: PythonOperator = PythonOperator(task_id="waitForFlush_CGW_API",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_CGW_API >> PCF_CGW_API >> PCFCheck_CGW_API >> delay_python_task_CGW_API >> Dedup_CGW_API >> Validation_End 
Branching_CGW_API >> Dedup2_CGW_API >> Validation_End
Branching_CGW_API >> faile_CGW_API
Branching_CGW_API >> Validation_End


feedname_GROUP_IOT_M2M = 'GROUP_IOT_M2M'.upper()
feedname2_GROUP_IOT_M2M = 'GROUP_IOT_M2M'.lower()

dedup_command_GROUP_IOT_M2M="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_GROUP_IOT_M2M)
logging.info(dedup_command_GROUP_IOT_M2M) 

pcf_command_GROUP_IOT_M2M = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_GROUP_IOT_M2M)
logging.info(pcf_command_GROUP_IOT_M2M)

PCFCheck_command_GROUP_IOT_M2M = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_GROUP_IOT_M2M,run_date,sleeptime)
logging.info(PCFCheck_command_GROUP_IOT_M2M)

branchScript_GROUP_IOT_M2M = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_GROUP_IOT_M2M,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_GROUP_IOT_M2M)

def branch_GROUP_IOT_M2M():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_GROUP_IOT_M2M,feedname2_GROUP_IOT_M2M)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_GROUP_IOT_M2M)
    x = readcsv(filepath,feedname2_GROUP_IOT_M2M)
    logging.info('branch_GROUP_IOT_M2M' + x)
    return x

Branching_GROUP_IOT_M2M = BranchPythonOperator(
    task_id='branchid_GROUP_IOT_M2M',
    python_callable=branch_GROUP_IOT_M2M,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_GROUP_IOT_M2M = BashOperator(
    task_id='PCF_GROUP_IOT_M2M',
    bash_command= pcf_command_GROUP_IOT_M2M,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_GROUP_IOT_M2M = BashOperator(
    task_id='Dedup_GROUP_IOT_M2M',
    bash_command= dedup_command_GROUP_IOT_M2M,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_GROUP_IOT_M2M = BashOperator(
    task_id='Dedup2_GROUP_IOT_M2M',
    bash_command= dedup_command_GROUP_IOT_M2M,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_GROUP_IOT_M2M = BashOperator(
    task_id='PCFCheck_GROUP_IOT_M2M',
    bash_command=PCFCheck_command_GROUP_IOT_M2M,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_GROUP_IOT_M2M = EmailOperator(
    task_id='faile_GROUP_IOT_M2M',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_GROUP_IOT_M2M),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_GROUP_IOT_M2M: PythonOperator = PythonOperator(task_id="waitForFlush_GROUP_IOT_M2M",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_GROUP_IOT_M2M >> PCF_GROUP_IOT_M2M >> PCFCheck_GROUP_IOT_M2M >> delay_python_task_GROUP_IOT_M2M >> Dedup_GROUP_IOT_M2M >> Validation_End 
Branching_GROUP_IOT_M2M >> Dedup2_GROUP_IOT_M2M >> Validation_End
Branching_GROUP_IOT_M2M >> faile_GROUP_IOT_M2M
Branching_GROUP_IOT_M2M >> Validation_End


feedname_RDS_SESSIONLOGEVENT = 'RDS_SESSIONLOGEVENT'.upper()
feedname2_RDS_SESSIONLOGEVENT = 'RDS_SESSIONLOGEVENT'.lower()

dedup_command_RDS_SESSIONLOGEVENT="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_RDS_SESSIONLOGEVENT)
logging.info(dedup_command_RDS_SESSIONLOGEVENT) 

pcf_command_RDS_SESSIONLOGEVENT = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_RDS_SESSIONLOGEVENT)
logging.info(pcf_command_RDS_SESSIONLOGEVENT)

PCFCheck_command_RDS_SESSIONLOGEVENT = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_RDS_SESSIONLOGEVENT,run_date,sleeptime)
logging.info(PCFCheck_command_RDS_SESSIONLOGEVENT)

branchScript_RDS_SESSIONLOGEVENT = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_RDS_SESSIONLOGEVENT,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_RDS_SESSIONLOGEVENT)

def branch_RDS_SESSIONLOGEVENT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_RDS_SESSIONLOGEVENT,feedname2_RDS_SESSIONLOGEVENT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_RDS_SESSIONLOGEVENT)
    x = readcsv(filepath,feedname2_RDS_SESSIONLOGEVENT)
    logging.info('branch_RDS_SESSIONLOGEVENT' + x)
    return x

Branching_RDS_SESSIONLOGEVENT = BranchPythonOperator(
    task_id='branchid_RDS_SESSIONLOGEVENT',
    python_callable=branch_RDS_SESSIONLOGEVENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_RDS_SESSIONLOGEVENT = BashOperator(
    task_id='PCF_RDS_SESSIONLOGEVENT',
    bash_command= pcf_command_RDS_SESSIONLOGEVENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_RDS_SESSIONLOGEVENT = BashOperator(
    task_id='Dedup_RDS_SESSIONLOGEVENT',
    bash_command= dedup_command_RDS_SESSIONLOGEVENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_RDS_SESSIONLOGEVENT = BashOperator(
    task_id='Dedup2_RDS_SESSIONLOGEVENT',
    bash_command= dedup_command_RDS_SESSIONLOGEVENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_RDS_SESSIONLOGEVENT = BashOperator(
    task_id='PCFCheck_RDS_SESSIONLOGEVENT',
    bash_command=PCFCheck_command_RDS_SESSIONLOGEVENT,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_RDS_SESSIONLOGEVENT = EmailOperator(
    task_id='faile_RDS_SESSIONLOGEVENT',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_RDS_SESSIONLOGEVENT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_RDS_SESSIONLOGEVENT: PythonOperator = PythonOperator(task_id="waitForFlush_RDS_SESSIONLOGEVENT",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_RDS_SESSIONLOGEVENT >> PCF_RDS_SESSIONLOGEVENT >> PCFCheck_RDS_SESSIONLOGEVENT >> delay_python_task_RDS_SESSIONLOGEVENT >> Dedup_RDS_SESSIONLOGEVENT >> Validation_End 
Branching_RDS_SESSIONLOGEVENT >> Dedup2_RDS_SESSIONLOGEVENT >> Validation_End
Branching_RDS_SESSIONLOGEVENT >> faile_RDS_SESSIONLOGEVENT
Branching_RDS_SESSIONLOGEVENT >> Validation_End


feedname_MyMTNApp = 'MyMTNApp'.upper()
feedname2_MyMTNApp = 'MyMTNApp'.lower()

dedup_command_MyMTNApp="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_MyMTNApp)
logging.info(dedup_command_MyMTNApp) 

pcf_command_MyMTNApp = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_MyMTNApp)
logging.info(pcf_command_MyMTNApp)

PCFCheck_command_MyMTNApp = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_MyMTNApp,run_date,sleeptime)
logging.info(PCFCheck_command_MyMTNApp)

branchScript_MyMTNApp = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_MyMTNApp,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_MyMTNApp)

def branch_MyMTNApp():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_MyMTNApp,feedname2_MyMTNApp)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_MyMTNApp)
    x = readcsv(filepath,feedname2_MyMTNApp)
    logging.info('branch_MyMTNApp' + x)
    return x

Branching_MyMTNApp = BranchPythonOperator(
    task_id='branchid_MyMTNApp',
    python_callable=branch_MyMTNApp,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_MyMTNApp = BashOperator(
    task_id='PCF_MyMTNApp',
    bash_command= pcf_command_MyMTNApp,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_MyMTNApp = BashOperator(
    task_id='Dedup_MyMTNApp',
    bash_command= dedup_command_MyMTNApp,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_MyMTNApp = BashOperator(
    task_id='Dedup2_MyMTNApp',
    bash_command= dedup_command_MyMTNApp,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_MyMTNApp = BashOperator(
    task_id='PCFCheck_MyMTNApp',
    bash_command=PCFCheck_command_MyMTNApp,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_MyMTNApp = EmailOperator(
    task_id='faile_MyMTNApp',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_MyMTNApp),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_MyMTNApp: PythonOperator = PythonOperator(task_id="waitForFlush_MyMTNApp",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_MyMTNApp >> PCF_MyMTNApp >> PCFCheck_MyMTNApp >> delay_python_task_MyMTNApp >> Dedup_MyMTNApp >> Validation_End 
Branching_MyMTNApp >> Dedup2_MyMTNApp >> Validation_End
Branching_MyMTNApp >> faile_MyMTNApp
Branching_MyMTNApp >> Validation_End


feedname_IB_API = 'IB_API'.upper()
feedname2_IB_API = 'IB_API'.lower()

dedup_command_IB_API="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_IB_API)
logging.info(dedup_command_IB_API) 

pcf_command_IB_API = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_IB_API)
logging.info(pcf_command_IB_API)

PCFCheck_command_IB_API = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_IB_API,run_date,sleeptime)
logging.info(PCFCheck_command_IB_API)

branchScript_IB_API = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_IB_API,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_IB_API)

def branch_IB_API():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_IB_API,feedname2_IB_API)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_IB_API)
    x = readcsv(filepath,feedname2_IB_API)
    logging.info('branch_IB_API' + x)
    return x

Branching_IB_API = BranchPythonOperator(
    task_id='branchid_IB_API',
    python_callable=branch_IB_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_IB_API = BashOperator(
    task_id='PCF_IB_API',
    bash_command= pcf_command_IB_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_IB_API = BashOperator(
    task_id='Dedup_IB_API',
    bash_command= dedup_command_IB_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_IB_API = BashOperator(
    task_id='Dedup2_IB_API',
    bash_command= dedup_command_IB_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_IB_API = BashOperator(
    task_id='PCFCheck_IB_API',
    bash_command=PCFCheck_command_IB_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_IB_API = EmailOperator(
    task_id='faile_IB_API',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_IB_API),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_IB_API: PythonOperator = PythonOperator(task_id="waitForFlush_IB_API",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_IB_API >> PCF_IB_API >> PCFCheck_IB_API >> delay_python_task_IB_API >> Dedup_IB_API >> Validation_End 
Branching_IB_API >> Dedup2_IB_API >> Validation_End
Branching_IB_API >> faile_IB_API
Branching_IB_API >> Validation_End


feedname_SAG_API = 'SAG_API'.upper()
feedname2_SAG_API = 'SAG_API'.lower()

dedup_command_SAG_API="bash /nas/share05/tools/DQ/Dedup.sh %s %s " % (d_1,feedname_SAG_API)
logging.info(dedup_command_SAG_API) 

pcf_command_SAG_API = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck --move  --jdbcName m1004 " % (d_1,d_1,feedname_SAG_API)
logging.info(pcf_command_SAG_API)

PCFCheck_command_SAG_API = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s" % (feedname_SAG_API,run_date,sleeptime)
logging.info(PCFCheck_command_SAG_API)

branchScript_SAG_API = "bash /nas/share05/tools/DQ/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_SAG_API,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_SAG_API)

def branch_SAG_API():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_SAG_API,feedname2_SAG_API)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_SAG_API)
    x = readcsv(filepath,feedname2_SAG_API)
    logging.info('branch_SAG_API' + x)
    return x

Branching_SAG_API = BranchPythonOperator(
    task_id='branchid_SAG_API',
    python_callable=branch_SAG_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCF_SAG_API = BashOperator(
    task_id='PCF_SAG_API',
    bash_command= pcf_command_SAG_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

Dedup_SAG_API = BashOperator(
    task_id='Dedup_SAG_API',
    bash_command= dedup_command_SAG_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)
Dedup2_SAG_API = BashOperator(
    task_id='Dedup2_SAG_API',
    bash_command= dedup_command_SAG_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

PCFCheck_SAG_API = BashOperator(
    task_id='PCFCheck_SAG_API',
    bash_command=PCFCheck_command_SAG_API,
    dag=dag,
    run_as_user='daasuser',
    priority_weight=1
)

faile_SAG_API = EmailOperator(
    task_id='faile_SAG_API',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_SAG_API),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_SAG_API: PythonOperator = PythonOperator(task_id="waitForFlush_SAG_API",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_SAG_API >> PCF_SAG_API >> PCFCheck_SAG_API >> delay_python_task_SAG_API >> Dedup_SAG_API >> Validation_End 
Branching_SAG_API >> Dedup2_SAG_API >> Validation_End
Branching_SAG_API >> faile_SAG_API
Branching_SAG_API >> Validation_End
