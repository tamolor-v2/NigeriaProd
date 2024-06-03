from __future__ import print_function
import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import datetime as dt
from dateutil.relativedelta import relativedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone
import dateutil.relativedelta

from collections import defaultdict
from os import popen
import os
import re

today =  dt.date.today()
dateRun =(today-dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m01')
#dateRun =dt.date(today.year, today.month-1, 1) #datetime.today() - timedelta(days=7)

#scripts locations
scriptPath="/nas/share05/tools/Facebook_reports/FB_EmailSender.py"
outputFile="/nas/share05/Nabil/NabilLogs/x.csv"
toolPath="/nas/share05/tools/Facebook_reports"

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': datetime(2022,6,1),
    'email': ['support@ligadata.com'],
    'email_on_failure': ['mnabeel@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup':False,
}

dag = DAG(
    dag_id='FB_Reports',
    default_args=args,
    schedule_interval="0 6 12 * * ",
    description='Facebook reports',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

def getDate(date_str):
    date = datetime.strptime(date_str[0:10], '%Y-%m-%d')
    currentdate=(date - dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m%d')
    return currentdate

getDateNode = PythonOperator(
    task_id = 'Get_Date',
    python_callable = getDate,
    priority_weight = 10,
    op_args=[popen('echo {{ execution_date }};').read()],
    dag=dag
)

dateRunStr="{{ task_instance.xcom_pull(task_ids='Get_Date') }}"

##added by Nabil to remove the data before the insertion 20221003
hdfsPath="hdfs://ngdaas/FlareData/output_8/"

dreports =[
"FB_CELL_INFO_REPORT_BASE"
,"FB_CELL_INFO_VALIDATION"
,"FB_CELL_QOS_VALIDATION"
,"FB_CELL_QOS_REPORT_BASE"
,"FB_SYSTEM_ENGAGEMENT_VALIDATION"
,"FB_SITE_ENGAGEMENT_VALIDATION"
,"FB_SITE_INFO_REPORT_BASE"
,"FB_SITE_INFO_VALIDATION"
,"FB_CELL_INFO_REPORT_BASE_2"
]

Run_templates_delete = []
for dreport in dreports:
     Run_templates_delete.append(BashOperator(
     task_id='hdfs_delete_'+dreport ,
     depends_on_past=False,
     bash_command='if [[ `hdfs dfs -ls {2}/{1}/*{0}*/` ]]; then hdfs dfs -rm {2}/{1}/*{0}*/*;else echo "No data found for {1}";fi '.format(str(dateRun).replace('-',''),dreport,hdfsPath),
     dag=dag,
     run_as_user = 'daasuser'
))
######

reports =[
"FB_CELL_INFO_REPORT_VIEW_STAGE1"
,"FB_CELL_INFO_REPORT_VIEW_STAGE2"
,"FB_CELL_INFO_REPORT_UPDATE_BASE"
,"FB_CELL_INFO_REPORT_VALIDATION"
,"FB_CELL_INFO_REPORT_FINAL_VIEW"
,"FB_SITE_INFO_REPORT_VIEW_STAGE"
,"FB_SITE_INFO_REPORT_UPDATE_BASE"
,"FB_SITE_INFO_REPORT_VALIDATION"
,"FB_SITE_INFO_REPORT_FINAL_VIEW"
,"FB_CELL_QOS_REPORT_UPDATE_BASE"
,"FB_CELL_QOS_REPORT_VALIDATION"
,"FB_CELL_QOS_REPORT_FINAL_VIEW"
,"FB_SITE_ENGAGE_REPORT_VIEW_STAGE1"
,"FB_SITE_ENGAGE_REPORT_VIEW_STAGE2"
,"FB_SITE_ENGAGE_REPORT_FINAL_VIEW"
,"FB_SYSTEM_ENGAGE_REPORT_FINAL_VIEW"
,"FB_SITE_ENGAGE_REPORT_VALIDATION"
,"FB_SYSTEM_ENGAGE_REPORT_VALIDATION"
,"FB_CELL_INFO_REPORT_BASE_2"
 ]

Run_templates = []
for report in reports:
     Run_templates.append(BashOperator(
     task_id='presto_Insert_'+report ,
     depends_on_past=False,
     bash_command='python3.9 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfigWithFacebookReports.json -qn {1} -rd {0} '.format(dateRunStr,report),
     dag=dag,
     run_as_user = 'daasuser'
))

scripts =[
"CELL_INFO_CSV_FILE"
,"SITE_INFO_CSV_FILE"
,"CELL_QOS_CSV_FILE"
,"SITE_ENGAGEMENT_CSV_FILE"
,"SYSTEM_ENGAGEMENT_CSV_FILE"
,"POPULATION_DATA_CSV_FILE"
,"TOPOLOGY_MAPS_CSV_FILES"
 ]

reportsList=[
"CELL_INFO_REPORT",
"SITE_INFO_REPORT",
"CELL_QOS_REPORT",
"SITE_ENGAGEMENT_REPORT",
"SYSTEM_ENGAGEMENT_REPORT"
]

Script_templates = []
for sc in scripts:
     Script_templates.append(BashOperator(
     task_id='generate_'+sc ,
     depends_on_past=False,
     bash_command='bash /nas/share05/tools/Facebook_reports/{0}.sh '.format(sc),
     dag=dag,
     run_as_user = 'daasuser'
))

counter=0
sendEmail=[]

for feed in reportsList:
     counter+=5
     sendEmail.append(BashOperator(
     task_id='sednEmail_'+feed ,
     depends_on_past=False,
     priority_weight = counter,
     bash_command='if [[ {0} == "SYSTEM_ENGAGEMENT_REPORT" ]]; then python3.9 {1} -o {2} -e {3}/sendingEmails_V1.sh -t VALIDATION_ONE -f {0};else python3.9 {1} -o {2} -e {3}/sendingEmails_V2.sh -t VALIDATION;fi '.format(feed,scriptPath,outputFile,toolPath),
     dag=dag,
     run_as_user = 'daasuser'
))

removeReplaceBase = BashOperator(
     task_id='removeReplaceBase' ,
     bash_command='if [[ `hdfs dfs -ls {1}/FB_CELL_INFO_REPORT_BASE/*{0}*/*` ]]; then hdfs dfs -rm {1}/FB_CELL_INFO_REPORT_BASE/*{0}/*;hdfs dfs -cp {1}/FB_CELL_INFO_REPORT_BASE_2/report_month={0}/* {1}/FB_CELL_INFO_REPORT_BASE/report_month={0}/ ;else echo "No data found for {0}";fi '.format(str(dateRun).replace('-',''),hdfsPath),
     dag=dag,
     run_as_user = 'daasuser'
)

genrateFile_CELL_INFO_REPORT = BashOperator(
     task_id='genrateFile_CELL_INFO_REPORT' ,
     bash_command='bash /nas/share05/tools/Facebook_reports/GC_DASHBOARDS.sh {0} {1} '.format(str(dateRun).replace('-',''),'FB_CELL_INFO_VALIDATION'),
     dag=dag,
     run_as_user = 'daasuser'
)

genrateFile_SITE_INFO_REPORT = BashOperator(
     task_id='genrateFile_SITE_INFO_REPORT' ,
     bash_command='bash /nas/share05/tools/Facebook_reports/GC_DASHBOARDS.sh {0} {1} '.format(str(dateRun).replace('-',''),'FB_SITE_INFO_VALIDATION'),
     dag=dag,
     run_as_user = 'daasuser'
)



Run_templates[0] << Run_templates_delete << getDateNode
Run_templates[2] << Run_templates[1] << Run_templates[0]
Run_templates[5] << Run_templates[2]
Run_templates[6] << Run_templates[5]
(Run_templates[18],Run_templates[7]) << Run_templates[8] << Run_templates[6]
(Run_templates[3],Run_templates[4]) << removeReplaceBase << Run_templates[18]
Run_templates[9] << Run_templates[4]
(Run_templates[10],Run_templates[11]) << Run_templates[9]
Run_templates[12] <<Run_templates[11]
Run_templates[13] << Run_templates[12]
Run_templates[14] << (Run_templates[12],Run_templates[13])
(Run_templates[15],Run_templates[16],Run_templates[17]) << Run_templates[14]
Script_templates[0] << Run_templates[4]
Script_templates[1] << Run_templates[8]
Script_templates[2] << Run_templates[11]
(Script_templates[3],Script_templates[5],Script_templates[6]) << Run_templates[14]
Script_templates[4] << Run_templates[15]
#sendEmail << Run_templates[15]
sendEmail[1] << Run_templates[7]
genrateFile_SITE_INFO_REPORT << Run_templates[7]
sendEmail[0] << Run_templates[3]
genrateFile_CELL_INFO_REPORT << Run_templates[3]
sendEmail[2] << Run_templates[10]
sendEmail[3] << Run_templates[16]
sendEmail[4] << Run_templates[17]