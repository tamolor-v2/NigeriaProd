from __future__ import print_function
import time
from datetime import datetime, timedelta
import datetime as dt
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone
from os import popen
import os
import re

from airflow.utils.task_group import TaskGroup
from airflow.utils.weight_rule import WeightRule
from airflow.operators.subdag_operator import SubDagOperator
today =  dt.date.today()
dateRun = dt.date(today.year, today.month, 1)

schemaName="flare_8"
hdfsPath="hdfs://ngdaas/FlareData/output_8"

#scripts locations
pythonScript="/nas/share05/tools/Facebook_reports/FB_EmailSender.py"
outputFile="/nas/share05/Nabil/NabilLogs/x.csv"
sendEmailScriptRec="/nas/share05/tools/Facebook_reports/sendingEmails_Receive.sh"
sendEmailScriptSLA="/nas/share05/tools/Facebook_reports/sendingEmails_SLA.sh"
pythonMonthlyScript="/nas/share05/tools/Facebook_reports/FB_EmailSender_Monthly.py"

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': datetime(2022,6,1),
    'email': ['m.nabeel@ligadata.com'],
    'email_on_failure': ['mnabeel@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup':False,
}

dag = DAG(
    dag_id='FB_Dashboards',
    default_args=args,
    schedule_interval="0 8 * * * ",
    description='Facebook Dashboards',
    catchup=False,
    concurrency=5,
    max_active_runs=4
)


def subdag(parent_dag_name, child_dag_name,args):
    dag_subdag = DAG(
            dag_id='%s.%s' % (parent_dag_name, child_dag_name),
            default_args=args,
            schedule_interval="@daily",
            concurrency=1,
            is_paused_upon_creation=False,
            )
    monthlyFeedsList =["CELL_INFO_2G","CELL_INFO_3G","CELL_INFO_4G","MTN_NG_GATEWAY_INFORMATION","MAPS_SITE_INFO","MTN_NG_3G_MONTHLY","MTN_NG_4G_MONTHLY","TOPOLOGY_MAP","MTNN_ASSET_EXTRACT_2G_CELLS","MTNN_ASSET_EXTRACT_3G_CELLS","MTNN_ASSET_EXTRACT_4G_CELLS","POPULATION_DS"]
    monthlyFeedsReceived=[]
    for feed in monthlyFeedsList:
        monthlyFeedsReceived.append(BashOperator(
        task_id='monthlyEmail_'+feed,
        depends_on_past=False,
        bash_command='python3.9 {0} -o {1} -e {2} -t {3} -f {4} '.format(pythonScript,outputFile,sendEmailScriptRec,'Received',feed),
        dag=dag_subdag,
        run_as_user = 'daasuser'
        ))
    return dag_subdag


def getDate(date_str):
    date = datetime.strptime(date_str[0:10], '%Y-%m-%d')
    currentdate=(date - timedelta(0)).strftime('%Y%m%d')
    return currentdate

def getDatePrev(date_str):
    from dateutil.relativedelta import relativedelta
    date = datetime.strptime(date_str[0:10], '%Y-%m-%d')
    currentdate=(date - relativedelta(months=1)).strftime('%Y%m')
    return currentdate

getDateNode = PythonOperator(
    task_id = 'Get_Date',
    python_callable = getDate,
    priority_weight = 10,
    op_args=[popen('echo {{ execution_date }};').read()],
    dag=dag
)

getDateNodePrev = PythonOperator(
    task_id = 'Get_Date_Prev',
    python_callable = getDatePrev,
    priority_weight = 10,
    op_args=[popen('echo {{ execution_date }};').read()],
    dag=dag
)


dateRunStr="{{ task_instance.xcom_pull(task_ids='Get_Date') }}"
dateRunStrPrev="{{ task_instance.xcom_pull(task_ids='Get_Date_Prev') }}"


def branch_check_first_day_of_month(**kwargs):
    from airflow.exceptions import AirflowException
    currentDate =  dt.date.today()
    firstDayOfMonth = dt.date(currentDate.year, currentDate.month, 1)
    print(str(firstDayOfMonth).replace('-',''),"---",kwargs['key1'])
    if(str(firstDayOfMonth).replace('-','') == kwargs['key1']):
        return 'checkFillDQ_monthly'
    else:
        return 'notFirstDayOfMonth'

allReports =[
"VW_MTN_NG_3G_MONTHLY_DQ_ISSUES"
,"FB_MTN_NG_3G_MONTHLY_DQ_RPT"
,"VW_MTN_NG_3G_MONTHLY_DUPLICATES"
,"VW_MTN_NG_3G_MONTHLY_REJECTED"
,"VW_MTN_NG_4G_MONTHLY_DQ_ISSUES"
,"FB_MTN_NG_4G_MONTHLY_DQ_RPT"
,"VW_MTN_NG_4G_MONTHLY_DUPLICATES"
,"VW_MTN_NG_4G_MONTHLY_REJECTED"
,"VW_MTNN_ASSET_EXTRACT_2G_CELLS_DQ_ISSUES"
,"FB_ASSET_EXTRACT_2G_DQ_RPT"
,"VW_MTNN_ASSET_EXTRACT_2G_CELLS_DUPLICATES"
,"VW_MTNN_ASSET_EXTRACT_2G_CELLS_REJECTED"
,"VW_MTNN_ASSET_EXTRACT_3G_CELLS_DQ_ISSUES"
,"FB_ASSET_EXTRACT_3G_DQ_RPT"
,"VW_MTNN_ASSET_EXTRACT_3G_CELLS_REJECTED"
,"VW_MTNN_ASSET_EXTRACT_4G_CELLS_DQ_ISSUES"
,"FB_ASSET_EXTRACT_4G_DQ_RPT"
,"VW_MTNN_ASSET_EXTRACT_4G_CELLS_DUPLICATES"
,"VW_MTNN_ASSET_EXTRACT_4G_CELLS_REJECTED"
,"VW_CELL_INFO_2G_DQ_ISSUES"
,"FB_CELL_INFO_2G_DQ_RPT"
,"VW_CELL_INFO_2G_DUPLICATES"
,"VW_CELL_INFO_2G_REJECTED"
,"VW_CELL_INFO_3G_DQ_ISSUES"
,"FB_CELL_INFO_3G_DQ_RPT"
,"VW_CELL_INFO_3G_DUPLICATES"
,"VW_CELL_INFO_3G_REJECTED"
,"VW_CELL_INFO_4G_DQ_ISSUES"
,"FB_CELL_INFO_4G_DQ_RPT"
,"VW_CELL_INFO_4G_DUPLICATES"
,"VW_CELL_INFO_4G_REJECTED"
,"VW_CELL_QOS_2G_DQ_ISSUES"
,"FB_CELL_QOS_2G_DQ_RPT"
,"VW_CELL_QOS_2G_DUPLICATES"
,"VW_CELL_QOS_2G_REJECTED"
,"VW_CELL_QOS_3G_DQ_ISSUES"
,"FB_CELL_QOS_3G_DQ_RPT"
,"VW_CELL_QOS_3G_DUPLICATES"
,"VW_CELL_QOS_3G_REJECTED"
,"VW_CELL_QOS_4G_DQ_ISSUES"
,"FB_CELL_QOS_4G_DQ_RPT"
,"VW_CELL_QOS_4G_DUPLICATES"
,"VW_CELL_QOS_4G_REJECTED"
,"VW_GATEWAY_DQ_ISSUES"
,"FB_GATEWAY_DQ_RPT"
,"VW_GATEWAY_DUPLICATES"
,"VW_GATEWAY_REJECTED"
,"VW_POPULATION_DQ_ISSUES"
,"FB_POPULATION_DQ_RPT"
,"VW_POPULATION_DUPLICATES"
,"VW_POPULATION_REJECTED"
,"VW_MAPS_SITE_INFO_DQ_ISSUES"
,"FB_MAPS_SITE_INFO_DQ_RPT"
,"VW_MAPS_SITE_INFO_DUPLICATES"
,"VW_TOPOLOGY_MAP_DQ_ISSUES"
,"FB_TOPOLOGY_MAP_DQ_RPT"
,"VW_TOPOLOGY_MAP_DUPLICATES"
,"VW_TOPOLOGY_MAP_REJECTED"
,"FB_COVERAGE_DQ_RPT"
,"FB_FEED_SLA_DQ_MONTHLY"
,"FB_COVERAGE_DASHBOARD_DQ"
,"VW_SITE_INFO_REJECTED"
,"FB_CELL_INFO_REPORT_DELIVERY_DATE"]

Run_templates_delete = []
for dreport in allReports:
     Run_templates_delete.append(BashOperator(
     task_id='hdfs_delete_'+dreport ,
     depends_on_past=False,
     bash_command='hdfs dfs -test -e {2}/{1}/*{0}*;res=`echo $?`;hdfs dfs -test -e {2}/{1}/*{3}*;res2=`echo $?`;if [[ res -eq 0 && res2 -eq 0 ]];then partionBy=`hdfs dfs -ls -d hdfs://ngdaas/FlareData/output_8/{1}/*{4}* | cut -d \'/\' -f7 | cut -d \'=\' -f1`;beeline -e "ALTER TABLE flare_8.{1} DROP PARTITION ($partionBy >= \'{5}01\',$partionBy <=\'{3}31\')";hdfs dfs -rm -r {2}/{1}/*{0}*;hdfs dfs -rm -r {2}/{1}/*{3}*;elif [[ res -eq 0 ]];then  partionBy=`hdfs dfs -ls -d hdfs://ngdaas/FlareData/output_8/{1}/*=* | cut -d \'/\' -f7 | cut -d \'=\' -f1 | head -1`;beeline -e "ALTER TABLE flare_8.{1} DROP PARTITION ($partionBy >= \'{5}01\',$partionBy <=\'{3}31\')";hdfs dfs -rm -r {2}/{1}/*{0}*;elif [[ res2 -eq 0 ]];then  partionBy=`hdfs dfs -ls -d hdfs://ngdaas/FlareData/output_8/{1}/*=* | cut -d \'/\' -f7 | cut -d \'=\' -f1 | head -1`;beeline -e "ALTER TABLE flare_8.{1} DROP PARTITION ($partionBy >= \'{5}01\',$partionBy <=\'{3}31\')";hdfs dfs -rm -r {2}/{1}/*{3}*;else echo "No data found for {1}";fi;for i in "VW_CELL_QOS_2G_DQ_ISSUES" "VW_CELL_QOS_3G_DQ_ISSUES" "VW_CELL_QOS_4G_DQ_ISSUES" "VW_CELL_INFO_2G_DQ_ISSUES" "VW_CELL_INFO_3G_DQ_ISSUES" "VW_CELL_INFO_4G_DQ_ISSUES"; do partionBy=`hdfs dfs -ls -d hdfs://ngdaas/FlareData/output_8/$i/*=* | cut -d \'/\' -f7 | cut -d \'=\' -f1 | head -1`;beeline -e "ALTER TABLE flare_8.$i DROP PARTITION ($partionBy >= \'{5}01\',$partionBy <=\'{3}31\')";done'.format(dateRunStrPrev,dreport,hdfsPath,str(dateRun).replace('-','')[0:6],str(dateRun).replace('-','').replace('\'',''),dateRunStrPrev),
     dag=dag,
     run_as_user = 'daasuser'
))


checkCompleteness = BashOperator(
    task_id='checkCompleteness',
    bash_command='echo "The delete cmd succeeded!" ',
    dag=dag,
    run_as_user='daasuser'
)

dupReports=[
"FB_3G_MONTHLY_DASHBOARD_DUPLICATES"
,"FB_4G_MONTHLY_DASHBOARD_DUPLICATES"
,"FB_ASSET_2G_DASHBOARD_DUPLICATES"
,"FB_ASSET_4G_DASHBOARD_DUPLICATES"
,"FB_CELL_INFO_2G_DASHBOARD_DUPLICATES"
,"FB_CELL_INFO_3G_DASHBOARD_DUPLICATES"
,"FB_CELL_INFO_4G_DASHBOARD_DUPLICATES"
,"FB_CELL_QOS_2G_DASHBOARD_DUPLICATES"
,"FB_CELL_QOS_3G_DASHBOARD_DUPLICATES"
,"FB_CELL_QOS_4G_DASHBOARD_DUPLICATES"
,"FB_GATEWAY_DASHBOARD_DUPLICATES"
,"FB_POPULATION_DASHBOARD_DUPLICATES"
,"FB_SITE_DASHBOARD_DUPLICATES"
,"FB_TOPOLOGY_DASHBOARD_DUPLICATES"
]

dqReports=[
"FB_3G_MONTHLY_DASHBOARD_DQ"
,"FB_4G_MONTHLY_DASHBOARD_DQ"
,"FB_ASSET_2G_DASHBOARD_DQ"
,"FB_ASSET_3G_DASHBOARD_DQ"
,"FB_ASSET_4G_DASHBOARD_DQ"
,"FB_CELL_INFO_2G_DASHBOARD_DQ"
,"FB_CELL_INFO_3G_DASHBOARD_DQ"
,"FB_CELL_INFO_4G_DASHBOARD_DQ"
,"FB_CELL_QOS_2G_DASHBOARD_DQ"
,"FB_CELL_QOS_3G_DASHBOARD_DQ"
,"FB_CELL_QOS_4G_DASHBOARD_DQ"
,"FB_COVERAGE_DASHBOARD_DQ"
,"FB_GATEWAY_DASHBOARD_DQ"
,"FB_POPULATION_DASHBOARD_DQ"
,"FB_SITE_DASHBOARD_DQ"
,"FB_TOPOLOGY_DASHBOARD_DQ"
]

dqIssueReports=[
"FB_3G_MONTHLY_DASHBOARD_DQ_ISSUE"
,"FB_4G_MONTHLY_DASHBOARD_DQ_ISSUE"
,"FB_ASSET_2G_DASHBOARD_DQ_ISSUE"
,"FB_ASSET_3G_DASHBOARD_DQ_ISSUE"
,"FB_ASSET_4G_DASHBOARD_DQ_ISSUE"
,"FB_CELL_INFO_2G_DASHBOARD_DQ_ISSUE"
,"FB_CELL_INFO_3G_DASHBOARD_DQ_ISSUE"
,"FB_CELL_INFO_4G_DASHBOARD_DQ_ISSUE"
,"FB_CELL_QOS_2G_DASHBOARD_DQ_ISSUE"
,"FB_CELL_QOS_3G_DASHBOARD_DQ_ISSUE"
,"FB_CELL_QOS_4G_DASHBOARD_DQ_ISSUE"
,"FB_GATEWAY_DASHBOARD_DQ_ISSUE"
,"FB_POPULATION_DASHBOARD_DQ_ISSUE"
,"FB_SITE_DASHBOARD_DQ_ISSUE"
,"FB_TOPOLOGY_DASHBOARD_DQ_ISSUE"
]

rejReports=[
"FB_3G_MONTHLY_DASHBOARD_REJECTED"
,"FB_4G_MONTHLY_DASHBOARD_REJECTED"
,"FB_ASSET_2G_DASHBOARD_REJECTED"
,"FB_ASSET_3G_DASHBOARD_REJECTED"
,"FB_ASSET_4G_DASHBOARD_REJECTED"
,"FB_CELL_INFO_2G_DASHBOARD_REJECTED"
,"FB_CELL_INFO_3G_DASHBOARD_REJECTED"
,"FB_CELL_INFO_4G_DASHBOARD_REJECTED"
,"FB_CELL_QOS_2G_DASHBOARD_REJECTED"
,"FB_CELL_QOS_3G_DASHBOARD_REJECTED"
,"FB_CELL_QOS_4G_DASHBOARD_REJECTED"
,"FB_GATEWAY_DASHBOARD_REJECTED"
,"FB_POPULATION_DASHBOARD_REJECTED"
,"FB_SITE_DASHBOARD_REJECTED"
,"FB_TOPOLOGY_DASHBOARD_REJECTED"
]

finalStageReports=[
"FB_3G_MONTHLY_DASHBOARD_STAGE"
,"FB_4G_MONTHLY_DASHBOARD_STAGE"
,"FB_ASSET_2G_DASHBOARD_STAGE"
,"FB_ASSET_3G_DASHBOARD_STAGE"
,"FB_ASSET_4G_DASHBOARD_STAGE"
,"FB_CELL_INFO_2G_DASHBOARD_STAGE"
,"FB_CELL_INFO_3G_DASHBOARD_STAGE"
,"FB_CELL_INFO_4G_DASHBOARD_STAGE"
,"FB_CELL_QOS_2G_DASHBOARD_STAGE"
,"FB_CELL_QOS_3G_DASHBOARD_STAGE"
,"FB_CELL_QOS_4G_DASHBOARD_STAGE"
,"FB_COVERAGE_DASHBOARD_STAGE"
,"FB_GATEWAY_DASHBOARD_STAGE"
,"FB_POPULATION_DASHBOARD_STAGE"
,"FB_SITE_DASHBOARD_STAGE"
,"FB_TOPOLOGY_DASHBOARD_STAGE"
,"FB_COVERAGE_DASHBOARD_STAGE_DETAILS"
,"FB_FEEDS_DASHBOARD_DQ_RPT"
,"FB_FEEDS_SLA_MONTHLY_DQ"
,"FB_FEEDS_SLA_MONTHLY_STAGE"
,"FB_VALIDATION_DQ_SUMMARY"
,"FB_REPORT_SLA_DQ_DETAILS"
,"FB_REPORT_SLA_DQ_MONTHLY"
,"FB_CELL_INFO_REPORT_DELIVERY_DATE"
]

tablesToDrop=[
"fb_mtn_ng_3G_monthly_dq_rpt5"
,"fb_mtn_ng_4g_monthly_dq_rpt5"
,"fb_asset_extract_2g_dq_rpt9"
,"fb_asset_extract_3g_dq_rpt8"
,"fb_asset_extract_4G_dq_rpt8"
,"fb_cell_info_2g_dq_rpt13"
,"fb_cell_info_3g_dq_rpt9"
,"fb_cell_info_4g_dq_rpt9"
,"fb_CELL_QOS_2G_dq_rpt9"
,"fb_CELL_QOS_3G_dq_rpt8"
,"fb_CELL_QOS_4G_dq_rpt8"
,"FB_COVERAGE_dq_rpt4"
,"FB_GATEWAY_dq_rpt3"
,"FB_POPULATION_dq_rpt3"
,"fb_maps_site_info_dq_rpt6"
,"fb_topology_map_dq_rpt8"
,"fb_coverage_dq_rpt_detail"
,"fb_feed_dq_rpt"
,"fb_feed_sla_dq_monthly_temp10"
,"fb_validation_dq_summary"
,"fb_report_sla_dq_detail"
,"fb_report_sla_dq_monthly"
]

Run_templates_rej = []
for report in rejReports:
     Run_templates_rej.append(BashOperator(
     task_id='presto_InsertRej_'+report ,
     depends_on_past=False,
     bash_command='python3.9 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfigWithFacebookReports.json -qn {1} -rd {0} '.format(dateRunStr,report),
     dag=dag,
     run_as_user = 'daasuser'
))

Run_templates_dq = []
for report in dqReports:
     Run_templates_dq.append(BashOperator(
     task_id='presto_InsertDQ_'+report ,
     depends_on_past=False,
     bash_command='python3.9 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfigWithFacebookReports.json -qn {1} -rd {0} '.format(dateRunStr,report),
     dag=dag,
     run_as_user = 'daasuser'
))

Run_templates_dqissue = []
for report in dqIssueReports:
     Run_templates_dqissue.append(BashOperator(
     task_id='presto_InsertDQIssues_'+report ,
     depends_on_past=False,
     bash_command='python3.9 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfigWithFacebookReports.json -qn {1} -rd {0} '.format(dateRunStr,report),
     dag=dag,
     run_as_user = 'daasuser'
))

Run_templates_dup = []
for report in dupReports:
     Run_templates_dup.append(BashOperator(
     task_id='presto_InsertDup_'+report ,
     depends_on_past=False,
     bash_command='python3.9 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfigWithFacebookReports.json -qn {1} -rd {0} '.format(dateRunStr,report),
     dag=dag,
     run_as_user = 'daasuser'
))

Run_templates_droptables = []
for report in tablesToDrop:
     Run_templates_droptables.append(BashOperator(
     task_id='presto_DropTables_'+report ,
     depends_on_past=False,
     bash_command='beeline -e "drop table {0}.{1}" '.format(schemaName,report),
     dag=dag,
     run_as_user = 'daasuser'
))

Run_templates_finalstage = []
for report in finalStageReports:
     Run_templates_finalstage.append(BashOperator(
     task_id='presto_InsertFinalStage_'+report ,
     priority_weight = 7,
     depends_on_past=False,
     bash_command='python3.9 /nas/share05/tools/TransactionsTool/runTransaction.py -cf /nas/share05/tools/TransactionsTool/runTransactionConfigWithFacebookReports.json -qn {1} -rd {0} '.format(dateRunStr,report),
     dag=dag,
     run_as_user = 'daasuser'
))

checkFillDQ_daily = BashOperator(
    task_id='checkFillDQ_daily',
    bash_command='bash /nas/share05/tools/Facebook_reports/dqFillChecker.sh {0} {1} {2}'.format(str(dateRun).replace('-',''),schemaName,'daily'),
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)

checkFillDQ_monthly = BashOperator(
    task_id='checkFillDQ_monthly',
    bash_command='bash /nas/share05/tools/Facebook_reports/dqFillChecker.sh {0} {1} {2}'.format(str(dateRun).replace('-',''),schemaName,'monthly'),
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)

notFirstDayOfMonth = BashOperator(
    task_id='notFirstDayOfMonth',
    bash_command='echo "The date {0} is not the first day of the month!" '.format(str(dateRun).replace('-','')),
    dag=dag,
    run_as_user='daasuser'
)

branch_check_data = BranchPythonOperator(
    task_id='branch_check_data',
    python_callable=branch_check_first_day_of_month,
    dag=dag,
    run_as_user='daasuser',
    op_kwargs={'key1': dateRunStr},
    priority_weight=1
)

sendEmailSLABreachDaily = BashOperator(
    task_id='sendEmailSLABreachDaily',
    bash_command='python3.9 {0} -o {1} -e {2} -t {3} '.format(pythonScript,outputFile,sendEmailScriptSLA,'SLA'),
    task_concurrency=1,
    dag=dag,
    run_as_user='daasuser'
)

sendEmailSLABreachMonthly = BashOperator(
    task_id='sendEmailSLABreachMonthly',
    bash_command='python3.9 {0} -o {1} -e {2} -t {3} '.format(pythonMonthlyScript,outputFile,sendEmailScriptSLA,'SLA'),
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)

dailyFeedsList =["CELL_QOS_2G","CELL_QOS_3G","CELL_QOS_4G"]

counter=0
dailyFeedsReceived=[]
for feed in dailyFeedsList:
     counter+=5
     dailyFeedsReceived.append(BashOperator(
     task_id='dailyEmail_'+feed,
     depends_on_past=False,
     priority_weight=counter,
     bash_command='python3.9 {0} -o {1} -e {2} -t {3} -f {4} '.format(pythonScript,outputFile,sendEmailScriptRec,'Received',feed),
     dag=dag,
     run_as_user = 'daasuser'
))

monthlyFeedsList =[
"CELL_INFO_2G"
,"CELL_INFO_3G"
,"CELL_INFO_4G"
,"MTN_NG_GATEWAY_INFORMATION"
,"MAPS_SITE_INFO"
,"MTN_NG_3G_MONTHLY"
,"MTN_NG_4G_MONTHLY"
,"TOPOLOGY_MAP"
,"MTNN_ASSET_EXTRACT_2G_CELLS"
,"MTNN_ASSET_EXTRACT_3G_CELLS"
,"MTNN_ASSET_EXTRACT_4G_CELLS"
,"POPULATION_DS"
]


generateGCFile = BashOperator(
     task_id='generateGCFile' ,
     bash_command='bash /nas/share05/tools/Facebook_reports/GC_DASHBOARDS.sh {0} {1} '.format(str(dateRun).replace('-',''),'FB_REPORT_SLA_DQ_MONTHLY'),
     dag=dag,
     run_as_user = 'daasuser'
)


subdag = SubDagOperator(
        task_id='monthlyFeedsReceived',
        subdag=subdag('FB_Dashboards','monthlyFeedsReceived',dag.default_args),
        dag=dag,
        )


#1 delete the old data -avoid duplicate-
checkCompleteness << Run_templates_delete << getDateNode << getDateNodePrev
#tg1 << getDateNode
#2 insert Rej dashboard
Run_templates_rej << checkCompleteness

#3 insert DQ issue dashboard
Run_templates_dqissue << checkCompleteness

#4 insert Dup dashboard
Run_templates_dup << checkCompleteness

#5 + #6 + #7 insert stage + DQ + feeds dashboards
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[0] << Run_templates_finalstage[0] << Run_templates_droptables[0]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[1] << Run_templates_finalstage[1] << Run_templates_droptables[1]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[2] << Run_templates_finalstage[2] << Run_templates_droptables[2]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[3] << Run_templates_finalstage[3] << Run_templates_droptables[3]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[4] << Run_templates_finalstage[4] << Run_templates_droptables[4]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[5] << Run_templates_finalstage[5] << Run_templates_droptables[5]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[6] << Run_templates_finalstage[6] << Run_templates_droptables[6]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[7] << Run_templates_finalstage[7] << Run_templates_droptables[7]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[8] << Run_templates_finalstage[8] << Run_templates_droptables[8]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[9] << Run_templates_finalstage[9] << Run_templates_droptables[9] << checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[10] <<Run_templates_finalstage[10] << Run_templates_droptables[10] << checkCompleteness
Run_templates_finalstage[16] << Run_templates_droptables [16] << Run_templates_dq[11] <<Run_templates_finalstage[11] << Run_templates_droptables[11] << checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[12] << Run_templates_finalstage[12] << Run_templates_droptables[12]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[13] << Run_templates_finalstage[13] << Run_templates_droptables[13]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[14]  << Run_templates_finalstage[14] << Run_templates_droptables[14]<< checkCompleteness
Run_templates_finalstage[17] << Run_templates_droptables[17] << Run_templates_dq[15] << Run_templates_finalstage[15] << Run_templates_droptables[15]<<checkCompleteness

#8 insert into feeds SLA monthly DQ
Run_templates_finalstage[19] << Run_templates_finalstage[18] << Run_templates_droptables[18] << (Run_templates_dq[0],Run_templates_dq[1],Run_templates_dq[2],Run_templates_dq[3],Run_templates_dq[4],Run_templates_dq[5],Run_templates_dq[6],Run_templates_dq[7],Run_templates_dq[8],Run_templates_dq[9],Run_templates_dq[10],Run_templates_dq[12],Run_templates_dq[13],Run_templates_dq[14],Run_templates_dq[15])

#9 insert into validation DQ summary
Run_templates_finalstage[20] << Run_templates_droptables[19] << (Run_templates_dq[12],Run_templates_dq[13],Run_templates_dq[15])

#10 delete the SLA DQ details then insert into it -avoid duplicate-
Run_templates_finalstage[21] << Run_templates_finalstage[23] << Run_templates_droptables[20] << (Run_templates_finalstage[19],Run_templates_finalstage[16])

#11 run the final table (report SLA DQ monthly) and generate the file + GC file
generateGCFile << Run_templates_finalstage[22] << Run_templates_droptables[21] << (Run_templates_finalstage[20],Run_templates_finalstage[21])
#generateGCFile << Run_templates_finalstage[22]

#12 check if the needed DQ tables got inserted + send an email in case of errors (Daily,Monthly)
sendEmailSLABreachDaily << checkFillDQ_daily << (Run_templates_dq[10],Run_templates_dq[9],Run_templates_dq[8])
sendEmailSLABreachMonthly << checkFillDQ_monthly << branch_check_data << (Run_templates_finalstage[19],Run_templates_finalstage[16])

#13 check if the needed DQ tables got inserted + send an email for the received feeds (Daily,Monthly)
dailyFeedsReceived << checkFillDQ_daily << (Run_templates_dq[10],Run_templates_dq[9],Run_templates_dq[8])

# check if the date is the first day of month to send the monthly emails
notFirstDayOfMonth << branch_check_data

subdag << checkFillDQ_monthly <<  branch_check_data << (Run_templates_finalstage[19],Run_templates_finalstage[16])
