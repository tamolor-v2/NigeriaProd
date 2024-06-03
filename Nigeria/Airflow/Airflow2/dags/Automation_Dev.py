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
d_1 = Variable.get("rerunDate_Dev", deserialize_json=True)
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
    result = phases[3]+ "Feed"
    if i == '0' :
        result = phases[0]
    elif i == '1' :
        result = phases[1]+ "Feed"
    elif i == '2' :
        result = phases[2]+ "Feed"
    elif i == '3' :
        result = phases[3]+ "Feed"
    return result


def readcsv (path,feedname):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = task (line[0],feedname)
    return result

dag = DAG('Automation_Dev', default_args=default_args, catchup=False,schedule_interval='15 5 * * *')

feedName=Variable.get("feedName",deserialize_json=True)
ValidationCode = '/usr/local/bin/python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f %s -c config.json ' %(d_1,feedName)
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

feedname_Feed = feedName.upper()
feedname2_Feed = feedName.lower()

#CCN_CDR_GPRS,CCN_CDR_SMS,CCN_CDR_VOICE

dedup_command_Feed="bash /nas/share05/tools/DQ2/Dedup.sh %s %s " % (d_1,feedname_Feed)
logging.info(dedup_command_Feed) 

pcf_command_Feed = "java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd %s -ed %s -f %s --logQueries --ignoreDotFilesCheck -p 2 --move " % (d_1,d_1,feedname_Feed)
logging.info(pcf_command_Feed)

PCFCheck_command_Feed = "hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname %s -date %s -sleeptime %s " % (feedname_Feed,run_date,sleeptime)
logging.info(PCFCheck_command_Feed)

branchScript_Feed = "bash /nas/share05/tools/DQ2/CheckValidationTable.scala -feedname %s -date %s -sleeptime %s -retry 200 -rundate %s -hour %s -outPath %s " %(feedname_Feed,d_1,sleeptime,run_date,hour_run,pathcsv)
logging.info(branchScript_Feed)

def branch_Feed():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feedname2_Feed,feedname2_Feed)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript_Feed)
    x = readcsv(filepath,feedname2_Feed)
    logging.info('branch_Feed' + x)
    return x

Branching_Feed = BranchPythonOperator(
    task_id='branchid_Feed',
    python_callable=branch_Feed,
    dag=dag,
    run_as_user='daasuser',
        priority_weight=1
)

PCF_Feed = BashOperator(
    task_id='PCF_Feed',
    bash_command= pcf_command_Feed,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
        priority_weight=1
)

Dedup_Feed = BashOperator(
    task_id='Dedup_Feed',
    bash_command= dedup_command_Feed,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
        priority_weight=1
)
Dedup2_Feed = BashOperator(
    task_id='Dedup2_Feed',
    bash_command= dedup_command_Feed,
    dag=dag,
    run_as_user='daasuser',
    queue='edge01002',
        priority_weight=1
)

PCFCheck_Feed = BashOperator(
    task_id='PCFCheck_Feed',
    bash_command=PCFCheck_command_Feed,
    dag=dag,
    run_as_user='daasuser',
        queue='edge01002',
    priority_weight=1
)

faile_Feed = EmailOperator(
    task_id='faile_Feed',
    to=email,
    subject='Airflow Alert for feed %s'%(feedname2_Feed),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

delay_python_task_Feed: PythonOperator = PythonOperator(task_id="waitForFlush_Feed",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))
            

Branching_Feed >> PCF_Feed >> PCFCheck_Feed >> delay_python_task_Feed >> Dedup_Feed
Branching_Feed >> Dedup2_Feed
Branching_Feed >> faile_Feed
Branching_Feed >> Validation_End
