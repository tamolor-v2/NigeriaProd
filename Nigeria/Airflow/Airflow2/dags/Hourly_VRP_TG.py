import airflow
from airflow.models import DAG
import re
import os
import csv
import os.path
import logging
from airflow.operators.python_operator import PythonOperator
from typing import List
from airflow.models.taskinstance import Context as context
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag import SubDagOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from runPresto import presto
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
import time
#from airflow.sensors.external_task import ExternalTaskSensor

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
#d_1 = Variable.get("rerunDate_Dev", deserialize_json=True)
d_1 = datetime.now().strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=2)
hour_run = hour_run_date.strftime('%H')
sleeptime= 2*1000*60

pathcsv = '/nas/share05/tools/DQ2/status2/'
phases = ['group_{0}.Success_{0}','group_{0}.Dedup2_{0}','group_{0}.PCF_{0}','group_{0}.faile_{0}']
email = ['y.bloukh@ligadata.com','support@ligadata.com','k.musallam@ligadata.com','t.olorunfemi@ligadata.com','bmustafa@ligadata.com']


prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'

oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)

def excQ(p,q):
    print(q)
    res = p.executeQuery(q,True)
    return res


#def readcsv(path,feedname):
#    print(feedname)
#    with open(path,'r') as csv_file:
#        readcsv = csv.reader(csv_file, delimiter = '|')
#        line = next(readcsv)
#        print('--------')
#        print("line 1" + str(line[0]))
#        print(type(feedname))
#        result = task(line[0],feedname.upper())
#    return result
#
#def task(i,feedname):
#    print('-----inside task------')
#    result = phases[3]
#    if i == '0' :
#        result = phases[0]
#    elif i == '1' :
#        result = phases[1]
#    elif i == '2' :
#        result = phases[2]
#    elif i == '3' :
#        result = phases[3]
#    return result.format(str(feedname).upper())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['y.bloukh@ligadata.com'],
    'email_on_failure': ['y.bloukh@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

with DAG('Feeds_VRTG',
    default_args=args,
    schedule_interval= "0 */12 * * * ",
    catchup=False,
    concurrency=6,
    max_active_runs=1
    ) as dag:


    def readcsv(path,feedname):
        print(feedname)
        with open(path,'r') as csv_file:
            readcsv = csv.reader(csv_file, delimiter = '|')
            line = next(readcsv)
            print('--------')
            print("line 1" + str(line[0]))
            print(type(feedname))
            print(type(line))
            result = task(line[0],feedname.upper())
        return result
    
    def task(i,feedname):
        print('-----inside task------')
        result = phases[3]
        if i == '0' :
            result = phases[0]
        elif i == '1' :
            result = phases[1]
        elif i == '2' :
            result = phases[2]
        elif i == '3' :
            result = phases[3]
        return result.format(str(feedname).upper())

    feedsList_VR_str = Variable.get("feedsList_VR",deserialize_json=False)
    feedsList_VR = re.sub('\'|\[|\]','', feedsList_VR_str).split(', ')
    print(feedsList_VR)
    feeds_all = ','.join(feedsList_VR)
    print('-------feedsall----')
    print(feeds_all)
    ValidationEndCode = '/usr/local/bin/python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d %s -f %s -c config.json ' %(d_1,feeds_all)
    Validation_End = BashOperator(
        task_id='Validation_End',
        bash_command= ValidationEndCode,
        dag=dag,
        run_as_user='daasuser',
        priority_weight=100
    )
    Validation_Start = BashOperator(
        task_id='Validation_Start',
        bash_command= ValidationEndCode,
        dag=dag,
        run_as_user='daasuser',
        priority_weight=100
    )
    fdi_edge2_str = Variable.get("fdi_edge2_feeds",deserialize_json=False)
    fdi_edge2 = re.sub('\'|\[|\]','', feedsList_VR_str).split(', ')
    feedsList_VR_str = Variable.get("feedsList_VR",deserialize_json=False)
    feedsList_VR = re.sub('\'|\[|\]','', feedsList_VR_str).split(', ')
    tgs = []
    ValidationCode = '/usr/local/bin/python3.6 /nas/share05/tools/ValidationTool_Python/bin/validationTool.py -d {d_1} -f {feed_name} -c config.json '
    dedup_command_Feed="kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM;bash /nas/share05/tools/DQ2/Dedup.sh {d_1} {feed_name} "
    pcf_command_Feed = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM;java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {d_1} -ed {d_1} -f {feed_name} --logQueries --ignoreDotFilesCheck -p 1 --move --jdbcName m1004 "
    PCFCheck_command_Feed = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM;hive --outputformat=tsv2 --showHeader=false -e 'MSCK REPAIR TABLE audit.reprocessing_summary_report;' && bash /nas/share05/tools/DQ2/CheckPCFTable.scala -feedname {feed_name} -date {run_date} -sleeptime {sleeptime} "
    branchScript_Feed = "kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM;bash /nas/share05/tools/DQ2/CheckValidationTable_new.scala -feedname {0} -date {1} -sleeptime {2} -retry 200 -rundate {3} -hour {4} -outPath {5} "
    pcf_command_edge_Feed = "ssh edge01002 java -Dlog4j2.formatMsgNoLookups=true -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml  -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps-1.0.1.190614.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto_FDI_local_edge2.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {d_1} -ed {d_1} -f {feed_name} --logQueries --ignoreDotFilesCheck -p 2 --move "
    for feed_name in feedsList_VR:
        with TaskGroup(group_id='group_{feed_name}'.format(feed_name=feed_name)) as tg1:
            
            Success = EmailOperator(
                task_id='Success_{feed_name}'.format(feed_name=feed_name.upper()),
                to=email,
                subject='Airflow Success for feed',
                html_content=""" <h3>Please check validation</h3> """,
                run_as_user='daasuser'
            )
            feedname_Feed = feed_name.upper()
            feedname2_Feed = feed_name.lower()

            def branch_Feed(feed_name):
                logging.info(os.getenv('HOSTNAME'))
                filepath = '%s/%s/%s/%s' % (pathcsv,d_1,feed_name.lower(),feed_name.lower())
                if os.path.isfile(filepath):
                    os.remove(filepath)
                    logging.info('remove file %s'%(filepath))

                print(branchScript_Feed.format(feed_name.upper(),d_1,sleeptime,run_date,hour_run,pathcsv))
                os.system(branchScript_Feed.format(feed_name.upper(),d_1,sleeptime,run_date,hour_run,pathcsv))
                x = readcsv(filepath,str(feed_name.lower()))
                logging.info('branch_Feed' + x)
                return x

            def branch_check_edge(feed_name):
                print('Check edge or normal')
                if (feed_name in fdi_edge2):
                    return "group_{feed_name}.PCF_edge_{feed_name}".format(feed_name=feed_name)
                else:
                    return "group_{feed_name}.PCF_norm_{feed_name}".format(feed_name)


            branch_pcf_Feed = BranchPythonOperator(
                task_id='PCF_{feed_name}'.format(feed_name=feed_name.upper()),
                python_callable=branch_check_edge,
                op_args=[feed_name],
                run_as_user='daasuser',
                priority_weight=1
            )

            Branching_Feed = BranchPythonOperator(
                task_id='branchid_{feed_name}'.format(feed_name=feed_name.upper()),
                python_callable=branch_Feed,
                op_args=[feed_name],
                run_as_user='daasuser',
                provide_context=True,
                priority_weight=100
            )
    
            PCF_Feed = BashOperator(
                task_id='PCF_norm_{feed_name}'.format(feed_name=feed_name.upper()),
                bash_command= pcf_command_Feed.format(d_1=d_1,feed_name=feed_name),
                run_as_user='daasuser',
                priority_weight=1
            )
            PCF_edge_Feed = BashOperator(
                task_id='PCF_edge_{feed_name}'.format(feed_name=feed_name.upper()),
                bash_command= pcf_command_edge_Feed.format(d_1=d_1,feed_name=feed_name),
                run_as_user='daasuser',
                priority_weight=1
            )
    
            Dedup_Feed = BashOperator(
                task_id='Dedup_{feed_name}'.format(feed_name=feed_name.upper()),
                bash_command= dedup_command_Feed.format(d_1=d_1,feed_name=feed_name),
                run_as_user='daasuser',
                priority_weight=1
            )
            Dedup2_Feed = BashOperator(
                task_id='Dedup2_{feed_name}'.format(feed_name=feed_name.upper()),
                bash_command= dedup_command_Feed.format(d_1=d_1,feed_name=feed_name),
                run_as_user='daasuser',
                priority_weight=1
            )
    
            PCFCheck_Feed = BashOperator(
                task_id='PCFCheck_{feed_name}'.format(feed_name=feed_name.upper()),
                bash_command=PCFCheck_command_Feed.format(feed_name=feed_name,run_date=run_date,sleeptime=sleeptime),
                run_as_user='daasuser',
                priority_weight=1,
                trigger_rule='none_failed'
            )
    
            faile_Feed = EmailOperator(
                task_id='faile_{feed_name}'.format(feed_name=feed_name.upper()),
                to=email,
                subject='Airflow Alert for feed %s'%(feedname2_Feed),
                html_content=""" <h3>Please check validation</h3> """,
                run_as_user='daasuser'
            )
    
            delay_python_task_Feed: PythonOperator = PythonOperator(task_id="waitForFlush_{feed_name}".format(feed_name=feed_name.upper()),python_callable=lambda: time.sleep(1800))
    
            Validation_Start >> Branching_Feed
            Branching_Feed >> branch_pcf_Feed >> PCF_Feed >> PCFCheck_Feed >> delay_python_task_Feed >> Dedup_Feed
            Branching_Feed >> branch_pcf_Feed >> PCF_edge_Feed >> PCFCheck_Feed >> delay_python_task_Feed >> Dedup_Feed
            Branching_Feed >> Dedup2_Feed
            Branching_Feed >> faile_Feed
            Branching_Feed >> Success
        tgs.append(tg1)
    def resetVariable():
        Variable.set("vr_flag","0")

    reset_variable = PythonOperator(
    task_id = 'reset_variable',
    depends_on_past=False,
    python_callable=resetVariable,
    trigger_rule='all_done',
    run_as_user = 'daasuser'
    )


    cnt_arr = 0
    six_arr = [[],[],[],[],[],[]]
    for task_name in range(0,len(tgs)):
#        print('---{0} is {1}----'.format(task_name,tgs[task_name]))
        if cnt_arr <= 4:
            print('----{} is less than 5 ---- '.format(cnt_arr))
            six_arr[cnt_arr].append(tgs[task_name])
            cnt_arr += 1
        else :
            print('----- {} is more than 5---'.format(cnt_arr))
            six_arr[cnt_arr].append(tgs[task_name])
            cnt_arr = 0


    print(six_arr)
 
    for tg in range(0,len(six_arr)) :
        for grp in range(0,len(six_arr[tg])):
#            if grp < len(six_arr[tg])-1:
#                six_arr[tg][grp] >> six_arr[tg][grp+1]
#            else:
#                six_arr[tg][grp] >> reset_variable
             six_arr[tg][grp] >> reset_variable
    reset_variable >> Validation_End
