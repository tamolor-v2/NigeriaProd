import airflow
import os
import csv
import os.path
import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import BranchPythonOperator

list_feed = 'MAPS_2G_CELL_D,MAPS_3G_CN_D,MAPS_3G_CELL_D,MAPS_3G_CN_D_BH,RECON,CIS_CDR,USSD_TRAFFIC_SUCCESS_RATE,SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,SMSC_TOTAL_DELIVERY_SUCCESS_RATE,SMSC_LICENSE,SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,CGW_API,IB_API,SAG_API,DUMP_SHARESELL,PAYMENT_GATEWAY_AIRTIME,BYPASS_VENDOR,XAAS_DAILY,USSD_CDR,SUBSCRIBER_TRANSACTIONS_CDR,BALANCES,DAILY_BVN_LINKING,DYA_DAILY_ACTIVATION,CANVASA,INWARD,OUTWARD,TOKEN_REPORT,TRANSACTING_AGENT,IVR_SERVICE,CC_ONLINE_ACTIVITY,FLYTXT_LATCH_DUMP'

yesterday = datetime.today() - timedelta(days=1)
today = datetime.today()
dateMonth= yesterday.strftime('%Y%m')
d_1 = yesterday.strftime('%Y%m%d')
run_date = today.strftime('%Y%m%d')
dayStr = today.strftime('%Y%m%d%H%M%S')
hour_run_date = datetime.today() - timedelta(hours=10)
hour_run = hour_run_date.strftime('%H')

phases = ['Extract_','Dedup2_','PCF_','faile_']
working_dir='/mnt/beegfs_bsl/Deployment/DEV/scripts/BslDriver'

pathStatus = Variable.get("pathStatus", deserialize_json=True)
Email = Variable.get("Email", deserialize_json=True)
sleeptime = Variable.get("sleeptime", deserialize_json=True)
ValidationCode_group23 = Variable.get("ValidationCode_group23", deserialize_json=True)
dedup_command= Variable.get("dedup_command", deserialize_json=True)
pcf_command= Variable.get("pcf_command", deserialize_json=True)
PCFCheck_command= Variable.get("PCFCheck_command", deserialize_json=True)
branchScript= Variable.get("branchScript", deserialize_json=True)
list_feed_ext = Variable.get("ExtractGroup1", deserialize_json=True)


default_args = {
    'owner': 'VR',
    'depends_on_past':False,
    'start_date': datetime(2019,12,3),
    'email': ['support@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def task(i,feedname,ExtractGroup):
    result = phases[3]+ feedname.upper()
    if i == '0' :
        result =ExtractGroup
    elif i == '1' :
        result = phases[1]+ feedname.upper()
    elif i == '2' :
        result = phases[2]+ feedname.upper()
    elif i == '3' :
        result = phases[3]+ feedname.upper()
    return result

def taskDumps(i,feedname,ExtractGroup):
    result = phases[3]+ feedname
    if i == '0' :
        result = ExtractGroup
    elif i == '1' :
        result = phases[1]+ feedname
    elif i == '2' :
        result = phases[2]+ feedname
    elif i == '3' :
        result = phases[3]+ feedname
    return result

def readcsv (path,feedname,ExtractGroup):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = task (line[0],feedname,ExtractGroup)
    return result
    
def readcsvdump (path,feedname,ExtractGroup):
    with open(path,'r') as csv_file:
        readcsv = csv.reader(csv_file, delimiter = '|')
        line = next(readcsv)
        result = taskDumps (line[0],feedname,ExtractGroup)
    return result

dag = DAG('VR_Group_23', default_args=default_args, catchup=False, schedule_interval= '0 8 * * * ')



Validation = BashOperator(
task_id='Validation',
bash_command= ValidationCode_group23.format(d_1,dayStr,list_feed),
trigger_rule='all_success',
run_as_user='daasuser',
priority_weight = 100,
dag=dag,
)


feedname_Group_2 = 'MAPS_2G_CELL_D,MAPS_3G_CN_D,MAPS_3G_CELL_D,MAPS_3G_CN_D_BH,RECON,CIS_CDR,USSD_TRAFFIC_SUCCESS_RATE,SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,SMSC_TOTAL_DELIVERY_SUCCESS_RATE,SMSC_LICENSE,SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,CGW_API,IB_API,SAG_API,DUMP_SHARESELL,PAYMENT_GATEWAY_AIRTIME,BYPASS_VENDOR,XAAS_DAILY,USSD_CDR,SUBSCRIBER_TRANSACTIONS_CDR,BALANCES,DAILY_BVN_LINKING,DYA_DAILY_ACTIVATION,CANVASA,INWARD,OUTWARD,TOKEN_REPORT,TRANSACTING_AGENT,IVR_SERVICE,CC_ONLINE_ACTIVITY,FLYTXT_LATCH_DUMP'

End = BashOperator(
    task_id='End',
    bash_command = 'echo finish',
    trigger_rule='none_failed',
    dag=dag,
    run_as_user='daasuser'
)

feedname_MAPS_2G_CELL_D = 'MAPS_2G_CELL_D'.upper()
feedname2_MAPS_2G_CELL_D = 'MAPS_2G_CELL_D'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_MAPS_2G_CELL_D():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MAPS_2G_CELL_D,feedname2_MAPS_2G_CELL_D)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MAPS_2G_CELL_D,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MAPS_2G_CELL_D, 'End')
    logging.info('branch_MAPS_2G_CELL_D' + x)
    return x

Branching_MAPS_2G_CELL_D = BranchPythonOperator(
    task_id='branchid_MAPS_2G_CELL_D',
    python_callable=branch_MAPS_2G_CELL_D,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MAPS_2G_CELL_D = BashOperator(
    task_id='PCF_MAPS_2G_CELL_D',
    bash_command= pcf_command.format(d_1,feedname_MAPS_2G_CELL_D),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_MAPS_2G_CELL_D = BashOperator(
    task_id='Dedup_MAPS_2G_CELL_D',
    bash_command= dedup_command.format(d_1, feedname_MAPS_2G_CELL_D, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MAPS_2G_CELL_D = BashOperator(
    task_id='Dedup2_MAPS_2G_CELL_D',
    bash_command= dedup_command.format(d_1, feedname_MAPS_2G_CELL_D, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MAPS_2G_CELL_D = BashOperator(
    task_id='PCFCheck_MAPS_2G_CELL_D',
    bash_command=PCFCheck_command.format(feedname_MAPS_2G_CELL_D,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MAPS_2G_CELL_D = EmailOperator(
    task_id='faile_MAPS_2G_CELL_D',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS_2G_CELL_D),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MAPS_2G_CELL_D >> PCF_MAPS_2G_CELL_D >> PCFCheck_MAPS_2G_CELL_D  >> Dedup_MAPS_2G_CELL_D >> End
Validation >> Branching_MAPS_2G_CELL_D >> End
Validation >> Branching_MAPS_2G_CELL_D >> Dedup2_MAPS_2G_CELL_D >> End
Validation >> Branching_MAPS_2G_CELL_D >> faile_MAPS_2G_CELL_D


feedname_MAPS_3G_CN_D = 'MAPS_3G_CN_D'.upper()
feedname2_MAPS_3G_CN_D = 'MAPS_3G_CN_D'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_MAPS_3G_CN_D():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MAPS_3G_CN_D,feedname2_MAPS_3G_CN_D)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MAPS_3G_CN_D,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MAPS_3G_CN_D, 'End')
    logging.info('branch_MAPS_3G_CN_D' + x)
    return x

Branching_MAPS_3G_CN_D = BranchPythonOperator(
    task_id='branchid_MAPS_3G_CN_D',
    python_callable=branch_MAPS_3G_CN_D,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MAPS_3G_CN_D = BashOperator(
    task_id='PCF_MAPS_3G_CN_D',
    bash_command= pcf_command.format(d_1,feedname_MAPS_3G_CN_D),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_MAPS_3G_CN_D = BashOperator(
    task_id='Dedup_MAPS_3G_CN_D',
    bash_command= dedup_command.format(d_1, feedname_MAPS_3G_CN_D, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MAPS_3G_CN_D = BashOperator(
    task_id='Dedup2_MAPS_3G_CN_D',
    bash_command= dedup_command.format(d_1, feedname_MAPS_3G_CN_D, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MAPS_3G_CN_D = BashOperator(
    task_id='PCFCheck_MAPS_3G_CN_D',
    bash_command=PCFCheck_command.format(feedname_MAPS_3G_CN_D,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MAPS_3G_CN_D = EmailOperator(
    task_id='faile_MAPS_3G_CN_D',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS_3G_CN_D),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MAPS_3G_CN_D >> PCF_MAPS_3G_CN_D >> PCFCheck_MAPS_3G_CN_D  >> Dedup_MAPS_3G_CN_D >> End
Validation >> Branching_MAPS_3G_CN_D >> End
Validation >> Branching_MAPS_3G_CN_D >> Dedup2_MAPS_3G_CN_D >> End
Validation >> Branching_MAPS_3G_CN_D >> faile_MAPS_3G_CN_D


feedname_MAPS_3G_CELL_D = 'MAPS_3G_CELL_D'.upper()
feedname2_MAPS_3G_CELL_D = 'MAPS_3G_CELL_D'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_MAPS_3G_CELL_D():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MAPS_3G_CELL_D,feedname2_MAPS_3G_CELL_D)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MAPS_3G_CELL_D,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MAPS_3G_CELL_D, 'End')
    logging.info('branch_MAPS_3G_CELL_D' + x)
    return x

Branching_MAPS_3G_CELL_D = BranchPythonOperator(
    task_id='branchid_MAPS_3G_CELL_D',
    python_callable=branch_MAPS_3G_CELL_D,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MAPS_3G_CELL_D = BashOperator(
    task_id='PCF_MAPS_3G_CELL_D',
    bash_command= pcf_command.format(d_1,feedname_MAPS_3G_CELL_D),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_MAPS_3G_CELL_D = BashOperator(
    task_id='Dedup_MAPS_3G_CELL_D',
    bash_command= dedup_command.format(d_1, feedname_MAPS_3G_CELL_D, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MAPS_3G_CELL_D = BashOperator(
    task_id='Dedup2_MAPS_3G_CELL_D',
    bash_command= dedup_command.format(d_1, feedname_MAPS_3G_CELL_D, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MAPS_3G_CELL_D = BashOperator(
    task_id='PCFCheck_MAPS_3G_CELL_D',
    bash_command=PCFCheck_command.format(feedname_MAPS_3G_CELL_D,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MAPS_3G_CELL_D = EmailOperator(
    task_id='faile_MAPS_3G_CELL_D',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS_3G_CELL_D),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MAPS_3G_CELL_D >> PCF_MAPS_3G_CELL_D >> PCFCheck_MAPS_3G_CELL_D  >> Dedup_MAPS_3G_CELL_D >> End
Validation >> Branching_MAPS_3G_CELL_D >> End
Validation >> Branching_MAPS_3G_CELL_D >> Dedup2_MAPS_3G_CELL_D >> End
Validation >> Branching_MAPS_3G_CELL_D >> faile_MAPS_3G_CELL_D


feedname_MAPS_3G_CN_D_BH = 'MAPS_3G_CN_D_BH'.upper()
feedname2_MAPS_3G_CN_D_BH = 'MAPS_3G_CN_D_BH'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_MAPS_3G_CN_D_BH():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_MAPS_3G_CN_D_BH,feedname2_MAPS_3G_CN_D_BH)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_MAPS_3G_CN_D_BH,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_MAPS_3G_CN_D_BH, 'End')
    logging.info('branch_MAPS_3G_CN_D_BH' + x)
    return x

Branching_MAPS_3G_CN_D_BH = BranchPythonOperator(
    task_id='branchid_MAPS_3G_CN_D_BH',
    python_callable=branch_MAPS_3G_CN_D_BH,
    dag=dag,
    run_as_user='daasuser'
)

PCF_MAPS_3G_CN_D_BH = BashOperator(
    task_id='PCF_MAPS_3G_CN_D_BH',
    bash_command= pcf_command.format(d_1,feedname_MAPS_3G_CN_D_BH),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_MAPS_3G_CN_D_BH = BashOperator(
    task_id='Dedup_MAPS_3G_CN_D_BH',
    bash_command= dedup_command.format(d_1, feedname_MAPS_3G_CN_D_BH, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_MAPS_3G_CN_D_BH = BashOperator(
    task_id='Dedup2_MAPS_3G_CN_D_BH',
    bash_command= dedup_command.format(d_1, feedname_MAPS_3G_CN_D_BH, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_MAPS_3G_CN_D_BH = BashOperator(
    task_id='PCFCheck_MAPS_3G_CN_D_BH',
    bash_command=PCFCheck_command.format(feedname_MAPS_3G_CN_D_BH,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_MAPS_3G_CN_D_BH = EmailOperator(
    task_id='faile_MAPS_3G_CN_D_BH',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_MAPS_3G_CN_D_BH),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_MAPS_3G_CN_D_BH >> PCF_MAPS_3G_CN_D_BH >> PCFCheck_MAPS_3G_CN_D_BH  >> Dedup_MAPS_3G_CN_D_BH >> End
Validation >> Branching_MAPS_3G_CN_D_BH >> End
Validation >> Branching_MAPS_3G_CN_D_BH >> Dedup2_MAPS_3G_CN_D_BH >> End
Validation >> Branching_MAPS_3G_CN_D_BH >> faile_MAPS_3G_CN_D_BH


feedname_RECON = 'RECON'.upper()
feedname2_RECON = 'RECON'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_RECON():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_RECON,feedname2_RECON)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_RECON,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_RECON, 'End')
    logging.info('branch_RECON' + x)
    return x

Branching_RECON = BranchPythonOperator(
    task_id='branchid_RECON',
    python_callable=branch_RECON,
    dag=dag,
    run_as_user='daasuser'
)

PCF_RECON = BashOperator(
    task_id='PCF_RECON',
    bash_command= pcf_command.format(d_1,feedname_RECON),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_RECON = BashOperator(
    task_id='Dedup_RECON',
    bash_command= dedup_command.format(d_1, feedname_RECON, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_RECON = BashOperator(
    task_id='Dedup2_RECON',
    bash_command= dedup_command.format(d_1, feedname_RECON, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_RECON = BashOperator(
    task_id='PCFCheck_RECON',
    bash_command=PCFCheck_command.format(feedname_RECON,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_RECON = EmailOperator(
    task_id='faile_RECON',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_RECON),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_RECON >> PCF_RECON >> PCFCheck_RECON  >> Dedup_RECON >> End
Validation >> Branching_RECON >> End
Validation >> Branching_RECON >> Dedup2_RECON >> End
Validation >> Branching_RECON >> faile_RECON


feedname_CIS_CDR = 'CIS_CDR'.upper()
feedname2_CIS_CDR = 'CIS_CDR'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_CIS_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CIS_CDR,feedname2_CIS_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CIS_CDR,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CIS_CDR, 'End')
    logging.info('branch_CIS_CDR' + x)
    return x

Branching_CIS_CDR = BranchPythonOperator(
    task_id='branchid_CIS_CDR',
    python_callable=branch_CIS_CDR,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CIS_CDR = BashOperator(
    task_id='PCF_CIS_CDR',
    bash_command= pcf_command.format(d_1,feedname_CIS_CDR),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_CIS_CDR = BashOperator(
    task_id='Dedup_CIS_CDR',
    bash_command= dedup_command.format(d_1, feedname_CIS_CDR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CIS_CDR = BashOperator(
    task_id='Dedup2_CIS_CDR',
    bash_command= dedup_command.format(d_1, feedname_CIS_CDR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CIS_CDR = BashOperator(
    task_id='PCFCheck_CIS_CDR',
    bash_command=PCFCheck_command.format(feedname_CIS_CDR,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CIS_CDR = EmailOperator(
    task_id='faile_CIS_CDR',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CIS_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CIS_CDR >> PCF_CIS_CDR >> PCFCheck_CIS_CDR  >> Dedup_CIS_CDR >> End
Validation >> Branching_CIS_CDR >> End
Validation >> Branching_CIS_CDR >> Dedup2_CIS_CDR >> End
Validation >> Branching_CIS_CDR >> faile_CIS_CDR


feedname_USSD_TRAFFIC_SUCCESS_RATE = 'USSD_TRAFFIC_SUCCESS_RATE'.upper()
feedname2_USSD_TRAFFIC_SUCCESS_RATE = 'USSD_TRAFFIC_SUCCESS_RATE'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_USSD_TRAFFIC_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_USSD_TRAFFIC_SUCCESS_RATE,feedname2_USSD_TRAFFIC_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_USSD_TRAFFIC_SUCCESS_RATE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_USSD_TRAFFIC_SUCCESS_RATE, 'End')
    logging.info('branch_USSD_TRAFFIC_SUCCESS_RATE' + x)
    return x

Branching_USSD_TRAFFIC_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_USSD_TRAFFIC_SUCCESS_RATE',
    python_callable=branch_USSD_TRAFFIC_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_USSD_TRAFFIC_SUCCESS_RATE = BashOperator(
    task_id='PCF_USSD_TRAFFIC_SUCCESS_RATE',
    bash_command= pcf_command.format(d_1,feedname_USSD_TRAFFIC_SUCCESS_RATE),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_USSD_TRAFFIC_SUCCESS_RATE = BashOperator(
    task_id='Dedup_USSD_TRAFFIC_SUCCESS_RATE',
    bash_command= dedup_command.format(d_1, feedname_USSD_TRAFFIC_SUCCESS_RATE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_USSD_TRAFFIC_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_USSD_TRAFFIC_SUCCESS_RATE',
    bash_command= dedup_command.format(d_1, feedname_USSD_TRAFFIC_SUCCESS_RATE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_USSD_TRAFFIC_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_USSD_TRAFFIC_SUCCESS_RATE',
    bash_command=PCFCheck_command.format(feedname_USSD_TRAFFIC_SUCCESS_RATE,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_USSD_TRAFFIC_SUCCESS_RATE = EmailOperator(
    task_id='faile_USSD_TRAFFIC_SUCCESS_RATE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_USSD_TRAFFIC_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_USSD_TRAFFIC_SUCCESS_RATE >> PCF_USSD_TRAFFIC_SUCCESS_RATE >> PCFCheck_USSD_TRAFFIC_SUCCESS_RATE  >> Dedup_USSD_TRAFFIC_SUCCESS_RATE >> End
Validation >> Branching_USSD_TRAFFIC_SUCCESS_RATE >> End
Validation >> Branching_USSD_TRAFFIC_SUCCESS_RATE >> Dedup2_USSD_TRAFFIC_SUCCESS_RATE >> End
Validation >> Branching_USSD_TRAFFIC_SUCCESS_RATE >> faile_USSD_TRAFFIC_SUCCESS_RATE


feedname_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = 'SMSC_ERROR_BREAKDOWN_PER_ACCOUNT'.upper()
feedname2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = 'SMSC_ERROR_BREAKDOWN_PER_ACCOUNT'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,feedname2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT, 'End')
    logging.info('branch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT' + x)
    return x

Branching_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = BranchPythonOperator(
    task_id='branchid_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT',
    python_callable=branch_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = BashOperator(
    task_id='PCF_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT',
    bash_command= pcf_command.format(d_1,feedname_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = BashOperator(
    task_id='Dedup_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT',
    bash_command= dedup_command.format(d_1, feedname_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = BashOperator(
    task_id='Dedup2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT',
    bash_command= dedup_command.format(d_1, feedname_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = BashOperator(
    task_id='PCFCheck_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT',
    bash_command=PCFCheck_command.format(feedname_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT = EmailOperator(
    task_id='faile_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> PCF_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> PCFCheck_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT  >> Dedup_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> End
Validation >> Branching_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> End
Validation >> Branching_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> Dedup2_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> End
Validation >> Branching_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT >> faile_SMSC_ERROR_BREAKDOWN_PER_ACCOUNT


feedname_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = 'SMSC_TOTAL_DELIVERY_SUCCESS_RATE'.upper()
feedname2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = 'SMSC_TOTAL_DELIVERY_SUCCESS_RATE'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE,feedname2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMSC_TOTAL_DELIVERY_SUCCESS_RATE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE, 'End')
    logging.info('branch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE' + x)
    return x

Branching_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_SMSC_TOTAL_DELIVERY_SUCCESS_RATE',
    python_callable=branch_SMSC_TOTAL_DELIVERY_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='PCF_SMSC_TOTAL_DELIVERY_SUCCESS_RATE',
    bash_command= pcf_command.format(d_1,feedname_SMSC_TOTAL_DELIVERY_SUCCESS_RATE),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='Dedup_SMSC_TOTAL_DELIVERY_SUCCESS_RATE',
    bash_command= dedup_command.format(d_1, feedname_SMSC_TOTAL_DELIVERY_SUCCESS_RATE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE',
    bash_command= dedup_command.format(d_1, feedname_SMSC_TOTAL_DELIVERY_SUCCESS_RATE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_SMSC_TOTAL_DELIVERY_SUCCESS_RATE',
    bash_command=PCFCheck_command.format(feedname_SMSC_TOTAL_DELIVERY_SUCCESS_RATE,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMSC_TOTAL_DELIVERY_SUCCESS_RATE = EmailOperator(
    task_id='faile_SMSC_TOTAL_DELIVERY_SUCCESS_RATE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> PCF_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> PCFCheck_SMSC_TOTAL_DELIVERY_SUCCESS_RATE  >> Dedup_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> End
Validation >> Branching_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> End
Validation >> Branching_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> Dedup2_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> End
Validation >> Branching_SMSC_TOTAL_DELIVERY_SUCCESS_RATE >> faile_SMSC_TOTAL_DELIVERY_SUCCESS_RATE


feedname_SMSC_LICENSE = 'SMSC_LICENSE'.upper()
feedname2_SMSC_LICENSE = 'SMSC_LICENSE'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_SMSC_LICENSE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMSC_LICENSE,feedname2_SMSC_LICENSE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMSC_LICENSE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMSC_LICENSE, 'End')
    logging.info('branch_SMSC_LICENSE' + x)
    return x

Branching_SMSC_LICENSE = BranchPythonOperator(
    task_id='branchid_SMSC_LICENSE',
    python_callable=branch_SMSC_LICENSE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMSC_LICENSE = BashOperator(
    task_id='PCF_SMSC_LICENSE',
    bash_command= pcf_command.format(d_1,feedname_SMSC_LICENSE),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_SMSC_LICENSE = BashOperator(
    task_id='Dedup_SMSC_LICENSE',
    bash_command= dedup_command.format(d_1, feedname_SMSC_LICENSE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMSC_LICENSE = BashOperator(
    task_id='Dedup2_SMSC_LICENSE',
    bash_command= dedup_command.format(d_1, feedname_SMSC_LICENSE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMSC_LICENSE = BashOperator(
    task_id='PCFCheck_SMSC_LICENSE',
    bash_command=PCFCheck_command.format(feedname_SMSC_LICENSE,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMSC_LICENSE = EmailOperator(
    task_id='faile_SMSC_LICENSE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMSC_LICENSE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMSC_LICENSE >> PCF_SMSC_LICENSE >> PCFCheck_SMSC_LICENSE  >> Dedup_SMSC_LICENSE >> End
Validation >> Branching_SMSC_LICENSE >> End
Validation >> Branching_SMSC_LICENSE >> Dedup2_SMSC_LICENSE >> End
Validation >> Branching_SMSC_LICENSE >> faile_SMSC_LICENSE


feedname_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = 'SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE'.upper()
feedname2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = 'SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,feedname2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE, 'End')
    logging.info('branch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE' + x)
    return x

Branching_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = BranchPythonOperator(
    task_id='branchid_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE',
    python_callable=branch_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = BashOperator(
    task_id='PCF_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE',
    bash_command= pcf_command.format(d_1,feedname_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = BashOperator(
    task_id='Dedup_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE',
    bash_command= dedup_command.format(d_1, feedname_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = BashOperator(
    task_id='Dedup2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE',
    bash_command= dedup_command.format(d_1, feedname_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = BashOperator(
    task_id='PCFCheck_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE',
    bash_command=PCFCheck_command.format(feedname_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE = EmailOperator(
    task_id='faile_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> PCF_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> PCFCheck_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE  >> Dedup_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> End
Validation >> Branching_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> End
Validation >> Branching_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> Dedup2_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> End
Validation >> Branching_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE >> faile_SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE


feedname_CGW_API = 'CGW_API'.upper()
feedname2_CGW_API = 'CGW_API'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_CGW_API():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CGW_API,feedname2_CGW_API)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CGW_API,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CGW_API, 'End')
    logging.info('branch_CGW_API' + x)
    return x

Branching_CGW_API = BranchPythonOperator(
    task_id='branchid_CGW_API',
    python_callable=branch_CGW_API,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CGW_API = BashOperator(
    task_id='PCF_CGW_API',
    bash_command= pcf_command.format(d_1,feedname_CGW_API),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_CGW_API = BashOperator(
    task_id='Dedup_CGW_API',
    bash_command= dedup_command.format(d_1, feedname_CGW_API, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CGW_API = BashOperator(
    task_id='Dedup2_CGW_API',
    bash_command= dedup_command.format(d_1, feedname_CGW_API, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CGW_API = BashOperator(
    task_id='PCFCheck_CGW_API',
    bash_command=PCFCheck_command.format(feedname_CGW_API,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CGW_API = EmailOperator(
    task_id='faile_CGW_API',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CGW_API),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CGW_API >> PCF_CGW_API >> PCFCheck_CGW_API  >> Dedup_CGW_API >> End
Validation >> Branching_CGW_API >> End
Validation >> Branching_CGW_API >> Dedup2_CGW_API >> End
Validation >> Branching_CGW_API >> faile_CGW_API


feedname_IB_API = 'IB_API'.upper()
feedname2_IB_API = 'IB_API'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_IB_API():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_IB_API,feedname2_IB_API)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_IB_API,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_IB_API, 'End')
    logging.info('branch_IB_API' + x)
    return x

Branching_IB_API = BranchPythonOperator(
    task_id='branchid_IB_API',
    python_callable=branch_IB_API,
    dag=dag,
    run_as_user='daasuser'
)

PCF_IB_API = BashOperator(
    task_id='PCF_IB_API',
    bash_command= pcf_command.format(d_1,feedname_IB_API),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_IB_API = BashOperator(
    task_id='Dedup_IB_API',
    bash_command= dedup_command.format(d_1, feedname_IB_API, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_IB_API = BashOperator(
    task_id='Dedup2_IB_API',
    bash_command= dedup_command.format(d_1, feedname_IB_API, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_IB_API = BashOperator(
    task_id='PCFCheck_IB_API',
    bash_command=PCFCheck_command.format(feedname_IB_API,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_IB_API = EmailOperator(
    task_id='faile_IB_API',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_IB_API),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_IB_API >> PCF_IB_API >> PCFCheck_IB_API  >> Dedup_IB_API >> End
Validation >> Branching_IB_API >> End
Validation >> Branching_IB_API >> Dedup2_IB_API >> End
Validation >> Branching_IB_API >> faile_IB_API


feedname_SAG_API = 'SAG_API'.upper()
feedname2_SAG_API = 'SAG_API'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_SAG_API():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SAG_API,feedname2_SAG_API)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SAG_API,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SAG_API, 'End')
    logging.info('branch_SAG_API' + x)
    return x

Branching_SAG_API = BranchPythonOperator(
    task_id='branchid_SAG_API',
    python_callable=branch_SAG_API,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SAG_API = BashOperator(
    task_id='PCF_SAG_API',
    bash_command= pcf_command.format(d_1,feedname_SAG_API),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_SAG_API = BashOperator(
    task_id='Dedup_SAG_API',
    bash_command= dedup_command.format(d_1, feedname_SAG_API, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SAG_API = BashOperator(
    task_id='Dedup2_SAG_API',
    bash_command= dedup_command.format(d_1, feedname_SAG_API, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SAG_API = BashOperator(
    task_id='PCFCheck_SAG_API',
    bash_command=PCFCheck_command.format(feedname_SAG_API,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SAG_API = EmailOperator(
    task_id='faile_SAG_API',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SAG_API),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SAG_API >> PCF_SAG_API >> PCFCheck_SAG_API  >> Dedup_SAG_API >> End
Validation >> Branching_SAG_API >> End
Validation >> Branching_SAG_API >> Dedup2_SAG_API >> End
Validation >> Branching_SAG_API >> faile_SAG_API


feedname_DUMP_SHARESELL = 'DUMP_SHARESELL'.upper()
feedname2_DUMP_SHARESELL = 'DUMP_SHARESELL'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_DUMP_SHARESELL():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_DUMP_SHARESELL,feedname2_DUMP_SHARESELL)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_DUMP_SHARESELL,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_DUMP_SHARESELL, 'End')
    logging.info('branch_DUMP_SHARESELL' + x)
    return x

Branching_DUMP_SHARESELL = BranchPythonOperator(
    task_id='branchid_DUMP_SHARESELL',
    python_callable=branch_DUMP_SHARESELL,
    dag=dag,
    run_as_user='daasuser'
)

PCF_DUMP_SHARESELL = BashOperator(
    task_id='PCF_DUMP_SHARESELL',
    bash_command= pcf_command.format(d_1,feedname_DUMP_SHARESELL),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_DUMP_SHARESELL = BashOperator(
    task_id='Dedup_DUMP_SHARESELL',
    bash_command= dedup_command.format(d_1, feedname_DUMP_SHARESELL, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_DUMP_SHARESELL = BashOperator(
    task_id='Dedup2_DUMP_SHARESELL',
    bash_command= dedup_command.format(d_1, feedname_DUMP_SHARESELL, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_DUMP_SHARESELL = BashOperator(
    task_id='PCFCheck_DUMP_SHARESELL',
    bash_command=PCFCheck_command.format(feedname_DUMP_SHARESELL,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_DUMP_SHARESELL = EmailOperator(
    task_id='faile_DUMP_SHARESELL',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_DUMP_SHARESELL),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_DUMP_SHARESELL >> PCF_DUMP_SHARESELL >> PCFCheck_DUMP_SHARESELL  >> Dedup_DUMP_SHARESELL >> End
Validation >> Branching_DUMP_SHARESELL >> End
Validation >> Branching_DUMP_SHARESELL >> Dedup2_DUMP_SHARESELL >> End
Validation >> Branching_DUMP_SHARESELL >> faile_DUMP_SHARESELL


feedname_PAYMENT_GATEWAY_AIRTIME = 'PAYMENT_GATEWAY_AIRTIME'.upper()
feedname2_PAYMENT_GATEWAY_AIRTIME = 'PAYMENT_GATEWAY_AIRTIME'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_PAYMENT_GATEWAY_AIRTIME():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_PAYMENT_GATEWAY_AIRTIME,feedname2_PAYMENT_GATEWAY_AIRTIME)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_PAYMENT_GATEWAY_AIRTIME,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_PAYMENT_GATEWAY_AIRTIME, 'End')
    logging.info('branch_PAYMENT_GATEWAY_AIRTIME' + x)
    return x

Branching_PAYMENT_GATEWAY_AIRTIME = BranchPythonOperator(
    task_id='branchid_PAYMENT_GATEWAY_AIRTIME',
    python_callable=branch_PAYMENT_GATEWAY_AIRTIME,
    dag=dag,
    run_as_user='daasuser'
)

PCF_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='PCF_PAYMENT_GATEWAY_AIRTIME',
    bash_command= pcf_command.format(d_1,feedname_PAYMENT_GATEWAY_AIRTIME),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='Dedup_PAYMENT_GATEWAY_AIRTIME',
    bash_command= dedup_command.format(d_1, feedname_PAYMENT_GATEWAY_AIRTIME, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='Dedup2_PAYMENT_GATEWAY_AIRTIME',
    bash_command= dedup_command.format(d_1, feedname_PAYMENT_GATEWAY_AIRTIME, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_PAYMENT_GATEWAY_AIRTIME = BashOperator(
    task_id='PCFCheck_PAYMENT_GATEWAY_AIRTIME',
    bash_command=PCFCheck_command.format(feedname_PAYMENT_GATEWAY_AIRTIME,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_PAYMENT_GATEWAY_AIRTIME = EmailOperator(
    task_id='faile_PAYMENT_GATEWAY_AIRTIME',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_PAYMENT_GATEWAY_AIRTIME),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_PAYMENT_GATEWAY_AIRTIME >> PCF_PAYMENT_GATEWAY_AIRTIME >> PCFCheck_PAYMENT_GATEWAY_AIRTIME  >> Dedup_PAYMENT_GATEWAY_AIRTIME >> End
Validation >> Branching_PAYMENT_GATEWAY_AIRTIME >> End
Validation >> Branching_PAYMENT_GATEWAY_AIRTIME >> Dedup2_PAYMENT_GATEWAY_AIRTIME >> End
Validation >> Branching_PAYMENT_GATEWAY_AIRTIME >> faile_PAYMENT_GATEWAY_AIRTIME


feedname_BYPASS_VENDOR = 'BYPASS_VENDOR'.upper()
feedname2_BYPASS_VENDOR = 'BYPASS_VENDOR'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_BYPASS_VENDOR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_BYPASS_VENDOR,feedname2_BYPASS_VENDOR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_BYPASS_VENDOR,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_BYPASS_VENDOR, 'End')
    logging.info('branch_BYPASS_VENDOR' + x)
    return x

Branching_BYPASS_VENDOR = BranchPythonOperator(
    task_id='branchid_BYPASS_VENDOR',
    python_callable=branch_BYPASS_VENDOR,
    dag=dag,
    run_as_user='daasuser'
)

PCF_BYPASS_VENDOR = BashOperator(
    task_id='PCF_BYPASS_VENDOR',
    bash_command= pcf_command.format(d_1,feedname_BYPASS_VENDOR),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_BYPASS_VENDOR = BashOperator(
    task_id='Dedup_BYPASS_VENDOR',
    bash_command= dedup_command.format(d_1, feedname_BYPASS_VENDOR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_BYPASS_VENDOR = BashOperator(
    task_id='Dedup2_BYPASS_VENDOR',
    bash_command= dedup_command.format(d_1, feedname_BYPASS_VENDOR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_BYPASS_VENDOR = BashOperator(
    task_id='PCFCheck_BYPASS_VENDOR',
    bash_command=PCFCheck_command.format(feedname_BYPASS_VENDOR,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_BYPASS_VENDOR = EmailOperator(
    task_id='faile_BYPASS_VENDOR',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_BYPASS_VENDOR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_BYPASS_VENDOR >> PCF_BYPASS_VENDOR >> PCFCheck_BYPASS_VENDOR  >> Dedup_BYPASS_VENDOR >> End
Validation >> Branching_BYPASS_VENDOR >> End
Validation >> Branching_BYPASS_VENDOR >> Dedup2_BYPASS_VENDOR >> End
Validation >> Branching_BYPASS_VENDOR >> faile_BYPASS_VENDOR


feedname_XAAS_DAILY = 'XAAS_DAILY'.upper()
feedname2_XAAS_DAILY = 'XAAS_DAILY'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_XAAS_DAILY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_XAAS_DAILY,feedname2_XAAS_DAILY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_XAAS_DAILY,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_XAAS_DAILY, 'End')
    logging.info('branch_XAAS_DAILY' + x)
    return x

Branching_XAAS_DAILY = BranchPythonOperator(
    task_id='branchid_XAAS_DAILY',
    python_callable=branch_XAAS_DAILY,
    dag=dag,
    run_as_user='daasuser'
)

PCF_XAAS_DAILY = BashOperator(
    task_id='PCF_XAAS_DAILY',
    bash_command= pcf_command.format(d_1,feedname_XAAS_DAILY),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_XAAS_DAILY = BashOperator(
    task_id='Dedup_XAAS_DAILY',
    bash_command= dedup_command.format(d_1, feedname_XAAS_DAILY, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_XAAS_DAILY = BashOperator(
    task_id='Dedup2_XAAS_DAILY',
    bash_command= dedup_command.format(d_1, feedname_XAAS_DAILY, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_XAAS_DAILY = BashOperator(
    task_id='PCFCheck_XAAS_DAILY',
    bash_command=PCFCheck_command.format(feedname_XAAS_DAILY,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_XAAS_DAILY = EmailOperator(
    task_id='faile_XAAS_DAILY',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_XAAS_DAILY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_XAAS_DAILY >> PCF_XAAS_DAILY >> PCFCheck_XAAS_DAILY  >> Dedup_XAAS_DAILY >> End
Validation >> Branching_XAAS_DAILY >> End
Validation >> Branching_XAAS_DAILY >> Dedup2_XAAS_DAILY >> End
Validation >> Branching_XAAS_DAILY >> faile_XAAS_DAILY


feedname_USSD_CDR = 'USSD_CDR'.upper()
feedname2_USSD_CDR = 'USSD_CDR'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_USSD_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_USSD_CDR,feedname2_USSD_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_USSD_CDR,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_USSD_CDR, 'End')
    logging.info('branch_USSD_CDR' + x)
    return x

Branching_USSD_CDR = BranchPythonOperator(
    task_id='branchid_USSD_CDR',
    python_callable=branch_USSD_CDR,
    dag=dag,
    run_as_user='daasuser'
)

PCF_USSD_CDR = BashOperator(
    task_id='PCF_USSD_CDR',
    bash_command= pcf_command.format(d_1,feedname_USSD_CDR),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_USSD_CDR = BashOperator(
    task_id='Dedup_USSD_CDR',
    bash_command= dedup_command.format(d_1, feedname_USSD_CDR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_USSD_CDR = BashOperator(
    task_id='Dedup2_USSD_CDR',
    bash_command= dedup_command.format(d_1, feedname_USSD_CDR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_USSD_CDR = BashOperator(
    task_id='PCFCheck_USSD_CDR',
    bash_command=PCFCheck_command.format(feedname_USSD_CDR,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_USSD_CDR = EmailOperator(
    task_id='faile_USSD_CDR',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_USSD_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_USSD_CDR >> PCF_USSD_CDR >> PCFCheck_USSD_CDR  >> Dedup_USSD_CDR >> End
Validation >> Branching_USSD_CDR >> End
Validation >> Branching_USSD_CDR >> Dedup2_USSD_CDR >> End
Validation >> Branching_USSD_CDR >> faile_USSD_CDR


feedname_SUBSCRIBER_TRANSACTIONS_CDR = 'SUBSCRIBER_TRANSACTIONS_CDR'.upper()
feedname2_SUBSCRIBER_TRANSACTIONS_CDR = 'SUBSCRIBER_TRANSACTIONS_CDR'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_SUBSCRIBER_TRANSACTIONS_CDR():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_SUBSCRIBER_TRANSACTIONS_CDR,feedname2_SUBSCRIBER_TRANSACTIONS_CDR)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_SUBSCRIBER_TRANSACTIONS_CDR,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_SUBSCRIBER_TRANSACTIONS_CDR, 'End')
    logging.info('branch_SUBSCRIBER_TRANSACTIONS_CDR' + x)
    return x

Branching_SUBSCRIBER_TRANSACTIONS_CDR = BranchPythonOperator(
    task_id='branchid_SUBSCRIBER_TRANSACTIONS_CDR',
    python_callable=branch_SUBSCRIBER_TRANSACTIONS_CDR,
    dag=dag,
    run_as_user='daasuser'
)

PCF_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='PCF_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command= pcf_command.format(d_1,feedname_SUBSCRIBER_TRANSACTIONS_CDR),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='Dedup_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command= dedup_command.format(d_1, feedname_SUBSCRIBER_TRANSACTIONS_CDR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='Dedup2_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command= dedup_command.format(d_1, feedname_SUBSCRIBER_TRANSACTIONS_CDR, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_SUBSCRIBER_TRANSACTIONS_CDR = BashOperator(
    task_id='PCFCheck_SUBSCRIBER_TRANSACTIONS_CDR',
    bash_command=PCFCheck_command.format(feedname_SUBSCRIBER_TRANSACTIONS_CDR,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_SUBSCRIBER_TRANSACTIONS_CDR = EmailOperator(
    task_id='faile_SUBSCRIBER_TRANSACTIONS_CDR',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_SUBSCRIBER_TRANSACTIONS_CDR),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_SUBSCRIBER_TRANSACTIONS_CDR >> PCF_SUBSCRIBER_TRANSACTIONS_CDR >> PCFCheck_SUBSCRIBER_TRANSACTIONS_CDR  >> Dedup_SUBSCRIBER_TRANSACTIONS_CDR >> End
Validation >> Branching_SUBSCRIBER_TRANSACTIONS_CDR >> End
Validation >> Branching_SUBSCRIBER_TRANSACTIONS_CDR >> Dedup2_SUBSCRIBER_TRANSACTIONS_CDR >> End
Validation >> Branching_SUBSCRIBER_TRANSACTIONS_CDR >> faile_SUBSCRIBER_TRANSACTIONS_CDR


feedname_BALANCES = 'BALANCES'.upper()
feedname2_BALANCES = 'BALANCES'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_BALANCES():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_BALANCES,feedname2_BALANCES)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_BALANCES,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_BALANCES, 'End')
    logging.info('branch_BALANCES' + x)
    return x

Branching_BALANCES = BranchPythonOperator(
    task_id='branchid_BALANCES',
    python_callable=branch_BALANCES,
    dag=dag,
    run_as_user='daasuser'
)

PCF_BALANCES = BashOperator(
    task_id='PCF_BALANCES',
    bash_command= pcf_command.format(d_1,feedname_BALANCES),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_BALANCES = BashOperator(
    task_id='Dedup_BALANCES',
    bash_command= dedup_command.format(d_1, feedname_BALANCES, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_BALANCES = BashOperator(
    task_id='Dedup2_BALANCES',
    bash_command= dedup_command.format(d_1, feedname_BALANCES, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_BALANCES = BashOperator(
    task_id='PCFCheck_BALANCES',
    bash_command=PCFCheck_command.format(feedname_BALANCES,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_BALANCES = EmailOperator(
    task_id='faile_BALANCES',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_BALANCES),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_BALANCES >> PCF_BALANCES >> PCFCheck_BALANCES  >> Dedup_BALANCES >> End
Validation >> Branching_BALANCES >> End
Validation >> Branching_BALANCES >> Dedup2_BALANCES >> End
Validation >> Branching_BALANCES >> faile_BALANCES


feedname_DAILY_BVN_LINKING = 'DAILY_BVN_LINKING'.upper()
feedname2_DAILY_BVN_LINKING = 'DAILY_BVN_LINKING'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_DAILY_BVN_LINKING():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_DAILY_BVN_LINKING,feedname2_DAILY_BVN_LINKING)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_DAILY_BVN_LINKING,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_DAILY_BVN_LINKING, 'End')
    logging.info('branch_DAILY_BVN_LINKING' + x)
    return x

Branching_DAILY_BVN_LINKING = BranchPythonOperator(
    task_id='branchid_DAILY_BVN_LINKING',
    python_callable=branch_DAILY_BVN_LINKING,
    dag=dag,
    run_as_user='daasuser'
)

PCF_DAILY_BVN_LINKING = BashOperator(
    task_id='PCF_DAILY_BVN_LINKING',
    bash_command= pcf_command.format(d_1,feedname_DAILY_BVN_LINKING),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_DAILY_BVN_LINKING = BashOperator(
    task_id='Dedup_DAILY_BVN_LINKING',
    bash_command= dedup_command.format(d_1, feedname_DAILY_BVN_LINKING, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_DAILY_BVN_LINKING = BashOperator(
    task_id='Dedup2_DAILY_BVN_LINKING',
    bash_command= dedup_command.format(d_1, feedname_DAILY_BVN_LINKING, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_DAILY_BVN_LINKING = BashOperator(
    task_id='PCFCheck_DAILY_BVN_LINKING',
    bash_command=PCFCheck_command.format(feedname_DAILY_BVN_LINKING,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_DAILY_BVN_LINKING = EmailOperator(
    task_id='faile_DAILY_BVN_LINKING',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_DAILY_BVN_LINKING),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_DAILY_BVN_LINKING >> PCF_DAILY_BVN_LINKING >> PCFCheck_DAILY_BVN_LINKING  >> Dedup_DAILY_BVN_LINKING >> End
Validation >> Branching_DAILY_BVN_LINKING >> End
Validation >> Branching_DAILY_BVN_LINKING >> Dedup2_DAILY_BVN_LINKING >> End
Validation >> Branching_DAILY_BVN_LINKING >> faile_DAILY_BVN_LINKING


feedname_DYA_DAILY_ACTIVATION = 'DYA_DAILY_ACTIVATION'.upper()
feedname2_DYA_DAILY_ACTIVATION = 'DYA_DAILY_ACTIVATION'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_DYA_DAILY_ACTIVATION():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_DYA_DAILY_ACTIVATION,feedname2_DYA_DAILY_ACTIVATION)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_DYA_DAILY_ACTIVATION,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_DYA_DAILY_ACTIVATION, 'End')
    logging.info('branch_DYA_DAILY_ACTIVATION' + x)
    return x

Branching_DYA_DAILY_ACTIVATION = BranchPythonOperator(
    task_id='branchid_DYA_DAILY_ACTIVATION',
    python_callable=branch_DYA_DAILY_ACTIVATION,
    dag=dag,
    run_as_user='daasuser'
)

PCF_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='PCF_DYA_DAILY_ACTIVATION',
    bash_command= pcf_command.format(d_1,feedname_DYA_DAILY_ACTIVATION),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='Dedup_DYA_DAILY_ACTIVATION',
    bash_command= dedup_command.format(d_1, feedname_DYA_DAILY_ACTIVATION, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='Dedup2_DYA_DAILY_ACTIVATION',
    bash_command= dedup_command.format(d_1, feedname_DYA_DAILY_ACTIVATION, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_DYA_DAILY_ACTIVATION = BashOperator(
    task_id='PCFCheck_DYA_DAILY_ACTIVATION',
    bash_command=PCFCheck_command.format(feedname_DYA_DAILY_ACTIVATION,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_DYA_DAILY_ACTIVATION = EmailOperator(
    task_id='faile_DYA_DAILY_ACTIVATION',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_DYA_DAILY_ACTIVATION),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_DYA_DAILY_ACTIVATION >> PCF_DYA_DAILY_ACTIVATION >> PCFCheck_DYA_DAILY_ACTIVATION  >> Dedup_DYA_DAILY_ACTIVATION >> End
Validation >> Branching_DYA_DAILY_ACTIVATION >> End
Validation >> Branching_DYA_DAILY_ACTIVATION >> Dedup2_DYA_DAILY_ACTIVATION >> End
Validation >> Branching_DYA_DAILY_ACTIVATION >> faile_DYA_DAILY_ACTIVATION


feedname_CANVASA = 'CANVASA'.upper()
feedname2_CANVASA = 'CANVASA'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_CANVASA():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CANVASA,feedname2_CANVASA)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CANVASA,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CANVASA, 'End')
    logging.info('branch_CANVASA' + x)
    return x

Branching_CANVASA = BranchPythonOperator(
    task_id='branchid_CANVASA',
    python_callable=branch_CANVASA,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CANVASA = BashOperator(
    task_id='PCF_CANVASA',
    bash_command= pcf_command.format(d_1,feedname_CANVASA),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_CANVASA = BashOperator(
    task_id='Dedup_CANVASA',
    bash_command= dedup_command.format(d_1, feedname_CANVASA, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CANVASA = BashOperator(
    task_id='Dedup2_CANVASA',
    bash_command= dedup_command.format(d_1, feedname_CANVASA, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CANVASA = BashOperator(
    task_id='PCFCheck_CANVASA',
    bash_command=PCFCheck_command.format(feedname_CANVASA,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CANVASA = EmailOperator(
    task_id='faile_CANVASA',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CANVASA),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CANVASA >> PCF_CANVASA >> PCFCheck_CANVASA  >> Dedup_CANVASA >> End
Validation >> Branching_CANVASA >> End
Validation >> Branching_CANVASA >> Dedup2_CANVASA >> End
Validation >> Branching_CANVASA >> faile_CANVASA


feedname_INWARD = 'INWARD'.upper()
feedname2_INWARD = 'INWARD'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_INWARD():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_INWARD,feedname2_INWARD)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_INWARD,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_INWARD, 'End')
    logging.info('branch_INWARD' + x)
    return x

Branching_INWARD = BranchPythonOperator(
    task_id='branchid_INWARD',
    python_callable=branch_INWARD,
    dag=dag,
    run_as_user='daasuser'
)

PCF_INWARD = BashOperator(
    task_id='PCF_INWARD',
    bash_command= pcf_command.format(d_1,feedname_INWARD),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_INWARD = BashOperator(
    task_id='Dedup_INWARD',
    bash_command= dedup_command.format(d_1, feedname_INWARD, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_INWARD = BashOperator(
    task_id='Dedup2_INWARD',
    bash_command= dedup_command.format(d_1, feedname_INWARD, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_INWARD = BashOperator(
    task_id='PCFCheck_INWARD',
    bash_command=PCFCheck_command.format(feedname_INWARD,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_INWARD = EmailOperator(
    task_id='faile_INWARD',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_INWARD),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_INWARD >> PCF_INWARD >> PCFCheck_INWARD  >> Dedup_INWARD >> End
Validation >> Branching_INWARD >> End
Validation >> Branching_INWARD >> Dedup2_INWARD >> End
Validation >> Branching_INWARD >> faile_INWARD


feedname_OUTWARD = 'OUTWARD'.upper()
feedname2_OUTWARD = 'OUTWARD'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_OUTWARD():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_OUTWARD,feedname2_OUTWARD)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_OUTWARD,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_OUTWARD, 'End')
    logging.info('branch_OUTWARD' + x)
    return x

Branching_OUTWARD = BranchPythonOperator(
    task_id='branchid_OUTWARD',
    python_callable=branch_OUTWARD,
    dag=dag,
    run_as_user='daasuser'
)

PCF_OUTWARD = BashOperator(
    task_id='PCF_OUTWARD',
    bash_command= pcf_command.format(d_1,feedname_OUTWARD),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_OUTWARD = BashOperator(
    task_id='Dedup_OUTWARD',
    bash_command= dedup_command.format(d_1, feedname_OUTWARD, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_OUTWARD = BashOperator(
    task_id='Dedup2_OUTWARD',
    bash_command= dedup_command.format(d_1, feedname_OUTWARD, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_OUTWARD = BashOperator(
    task_id='PCFCheck_OUTWARD',
    bash_command=PCFCheck_command.format(feedname_OUTWARD,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_OUTWARD = EmailOperator(
    task_id='faile_OUTWARD',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_OUTWARD),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_OUTWARD >> PCF_OUTWARD >> PCFCheck_OUTWARD  >> Dedup_OUTWARD >> End
Validation >> Branching_OUTWARD >> End
Validation >> Branching_OUTWARD >> Dedup2_OUTWARD >> End
Validation >> Branching_OUTWARD >> faile_OUTWARD


feedname_TOKEN_REPORT = 'TOKEN_REPORT'.upper()
feedname2_TOKEN_REPORT = 'TOKEN_REPORT'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_TOKEN_REPORT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_TOKEN_REPORT,feedname2_TOKEN_REPORT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_TOKEN_REPORT,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_TOKEN_REPORT, 'End')
    logging.info('branch_TOKEN_REPORT' + x)
    return x

Branching_TOKEN_REPORT = BranchPythonOperator(
    task_id='branchid_TOKEN_REPORT',
    python_callable=branch_TOKEN_REPORT,
    dag=dag,
    run_as_user='daasuser'
)

PCF_TOKEN_REPORT = BashOperator(
    task_id='PCF_TOKEN_REPORT',
    bash_command= pcf_command.format(d_1,feedname_TOKEN_REPORT),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_TOKEN_REPORT = BashOperator(
    task_id='Dedup_TOKEN_REPORT',
    bash_command= dedup_command.format(d_1, feedname_TOKEN_REPORT, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_TOKEN_REPORT = BashOperator(
    task_id='Dedup2_TOKEN_REPORT',
    bash_command= dedup_command.format(d_1, feedname_TOKEN_REPORT, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_TOKEN_REPORT = BashOperator(
    task_id='PCFCheck_TOKEN_REPORT',
    bash_command=PCFCheck_command.format(feedname_TOKEN_REPORT,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_TOKEN_REPORT = EmailOperator(
    task_id='faile_TOKEN_REPORT',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_TOKEN_REPORT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_TOKEN_REPORT >> PCF_TOKEN_REPORT >> PCFCheck_TOKEN_REPORT  >> Dedup_TOKEN_REPORT >> End
Validation >> Branching_TOKEN_REPORT >> End
Validation >> Branching_TOKEN_REPORT >> Dedup2_TOKEN_REPORT >> End
Validation >> Branching_TOKEN_REPORT >> faile_TOKEN_REPORT


feedname_TRANSACTING_AGENT = 'TRANSACTING_AGENT'.upper()
feedname2_TRANSACTING_AGENT = 'TRANSACTING_AGENT'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_TRANSACTING_AGENT():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_TRANSACTING_AGENT,feedname2_TRANSACTING_AGENT)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_TRANSACTING_AGENT,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_TRANSACTING_AGENT, 'End')
    logging.info('branch_TRANSACTING_AGENT' + x)
    return x

Branching_TRANSACTING_AGENT = BranchPythonOperator(
    task_id='branchid_TRANSACTING_AGENT',
    python_callable=branch_TRANSACTING_AGENT,
    dag=dag,
    run_as_user='daasuser'
)

PCF_TRANSACTING_AGENT = BashOperator(
    task_id='PCF_TRANSACTING_AGENT',
    bash_command= pcf_command.format(d_1,feedname_TRANSACTING_AGENT),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_TRANSACTING_AGENT = BashOperator(
    task_id='Dedup_TRANSACTING_AGENT',
    bash_command= dedup_command.format(d_1, feedname_TRANSACTING_AGENT, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_TRANSACTING_AGENT = BashOperator(
    task_id='Dedup2_TRANSACTING_AGENT',
    bash_command= dedup_command.format(d_1, feedname_TRANSACTING_AGENT, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_TRANSACTING_AGENT = BashOperator(
    task_id='PCFCheck_TRANSACTING_AGENT',
    bash_command=PCFCheck_command.format(feedname_TRANSACTING_AGENT,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_TRANSACTING_AGENT = EmailOperator(
    task_id='faile_TRANSACTING_AGENT',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_TRANSACTING_AGENT),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_TRANSACTING_AGENT >> PCF_TRANSACTING_AGENT >> PCFCheck_TRANSACTING_AGENT  >> Dedup_TRANSACTING_AGENT >> End
Validation >> Branching_TRANSACTING_AGENT >> End
Validation >> Branching_TRANSACTING_AGENT >> Dedup2_TRANSACTING_AGENT >> End
Validation >> Branching_TRANSACTING_AGENT >> faile_TRANSACTING_AGENT


feedname_IVR_SERVICE = 'IVR_SERVICE'.upper()
feedname2_IVR_SERVICE = 'IVR_SERVICE'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_IVR_SERVICE():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_IVR_SERVICE,feedname2_IVR_SERVICE)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_IVR_SERVICE,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_IVR_SERVICE, 'End')
    logging.info('branch_IVR_SERVICE' + x)
    return x

Branching_IVR_SERVICE = BranchPythonOperator(
    task_id='branchid_IVR_SERVICE',
    python_callable=branch_IVR_SERVICE,
    dag=dag,
    run_as_user='daasuser'
)

PCF_IVR_SERVICE = BashOperator(
    task_id='PCF_IVR_SERVICE',
    bash_command= pcf_command.format(d_1,feedname_IVR_SERVICE),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_IVR_SERVICE = BashOperator(
    task_id='Dedup_IVR_SERVICE',
    bash_command= dedup_command.format(d_1, feedname_IVR_SERVICE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_IVR_SERVICE = BashOperator(
    task_id='Dedup2_IVR_SERVICE',
    bash_command= dedup_command.format(d_1, feedname_IVR_SERVICE, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_IVR_SERVICE = BashOperator(
    task_id='PCFCheck_IVR_SERVICE',
    bash_command=PCFCheck_command.format(feedname_IVR_SERVICE,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_IVR_SERVICE = EmailOperator(
    task_id='faile_IVR_SERVICE',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_IVR_SERVICE),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_IVR_SERVICE >> PCF_IVR_SERVICE >> PCFCheck_IVR_SERVICE  >> Dedup_IVR_SERVICE >> End
Validation >> Branching_IVR_SERVICE >> End
Validation >> Branching_IVR_SERVICE >> Dedup2_IVR_SERVICE >> End
Validation >> Branching_IVR_SERVICE >> faile_IVR_SERVICE


feedname_CC_ONLINE_ACTIVITY = 'CC_ONLINE_ACTIVITY'.upper()
feedname2_CC_ONLINE_ACTIVITY = 'CC_ONLINE_ACTIVITY'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_CC_ONLINE_ACTIVITY():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_CC_ONLINE_ACTIVITY,feedname2_CC_ONLINE_ACTIVITY)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_CC_ONLINE_ACTIVITY,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_CC_ONLINE_ACTIVITY, 'End')
    logging.info('branch_CC_ONLINE_ACTIVITY' + x)
    return x

Branching_CC_ONLINE_ACTIVITY = BranchPythonOperator(
    task_id='branchid_CC_ONLINE_ACTIVITY',
    python_callable=branch_CC_ONLINE_ACTIVITY,
    dag=dag,
    run_as_user='daasuser'
)

PCF_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='PCF_CC_ONLINE_ACTIVITY',
    bash_command= pcf_command.format(d_1,feedname_CC_ONLINE_ACTIVITY),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='Dedup_CC_ONLINE_ACTIVITY',
    bash_command= dedup_command.format(d_1, feedname_CC_ONLINE_ACTIVITY, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='Dedup2_CC_ONLINE_ACTIVITY',
    bash_command= dedup_command.format(d_1, feedname_CC_ONLINE_ACTIVITY, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_CC_ONLINE_ACTIVITY = BashOperator(
    task_id='PCFCheck_CC_ONLINE_ACTIVITY',
    bash_command=PCFCheck_command.format(feedname_CC_ONLINE_ACTIVITY,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_CC_ONLINE_ACTIVITY = EmailOperator(
    task_id='faile_CC_ONLINE_ACTIVITY',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_CC_ONLINE_ACTIVITY),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_CC_ONLINE_ACTIVITY >> PCF_CC_ONLINE_ACTIVITY >> PCFCheck_CC_ONLINE_ACTIVITY  >> Dedup_CC_ONLINE_ACTIVITY >> End
Validation >> Branching_CC_ONLINE_ACTIVITY >> End
Validation >> Branching_CC_ONLINE_ACTIVITY >> Dedup2_CC_ONLINE_ACTIVITY >> End
Validation >> Branching_CC_ONLINE_ACTIVITY >> faile_CC_ONLINE_ACTIVITY


feedname_FLYTXT_LATCH_DUMP = 'FLYTXT_LATCH_DUMP'.upper()
feedname2_FLYTXT_LATCH_DUMP = 'FLYTXT_LATCH_DUMP'.lower()
ExtractGroup_Group_2 = 'Group_2'


def branch_FLYTXT_LATCH_DUMP():
    logging.info(os.getenv('HOSTNAME'))
    filepath = '%s/%s/%s/%s' % (pathStatus,d_1,feedname2_FLYTXT_LATCH_DUMP,feedname2_FLYTXT_LATCH_DUMP)
    if os.path.isfile(filepath):
        os.remove(filepath)
        logging.info('remove file %s'%(filepath))
        
    os.system(branchScript.format(feedname_FLYTXT_LATCH_DUMP,d_1,sleeptime,pathStatus,run_date,hour_run))
    x = readcsv(filepath,feedname2_FLYTXT_LATCH_DUMP, 'End')
    logging.info('branch_FLYTXT_LATCH_DUMP' + x)
    return x

Branching_FLYTXT_LATCH_DUMP = BranchPythonOperator(
    task_id='branchid_FLYTXT_LATCH_DUMP',
    python_callable=branch_FLYTXT_LATCH_DUMP,
    dag=dag,
    run_as_user='daasuser'
)

PCF_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='PCF_FLYTXT_LATCH_DUMP',
    bash_command= pcf_command.format(d_1,feedname_FLYTXT_LATCH_DUMP),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

Dedup_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='Dedup_FLYTXT_LATCH_DUMP',
    bash_command= dedup_command.format(d_1, feedname_FLYTXT_LATCH_DUMP, 'false'),
    dag=dag,
    run_as_user='daasuser'
)
Dedup2_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='Dedup2_FLYTXT_LATCH_DUMP',
    bash_command= dedup_command.format(d_1, feedname_FLYTXT_LATCH_DUMP, 'false'),
    dag=dag,
    run_as_user='daasuser'
)

PCFCheck_FLYTXT_LATCH_DUMP = BashOperator(
    task_id='PCFCheck_FLYTXT_LATCH_DUMP',
    bash_command=PCFCheck_command.format(feedname_FLYTXT_LATCH_DUMP,d_1,sleeptime),
    dag=dag,
    queue='edge01002',
    run_as_user='daasuser'
)

faile_FLYTXT_LATCH_DUMP = EmailOperator(
    task_id='faile_FLYTXT_LATCH_DUMP',
    to=Email,
    subject='Airflow Alert for feed %s'%(feedname2_FLYTXT_LATCH_DUMP),
    html_content=""" <h3>Please check validation</h3> """,
    dag=dag,
    run_as_user='daasuser'
)

Validation >> Branching_FLYTXT_LATCH_DUMP >> PCF_FLYTXT_LATCH_DUMP >> PCFCheck_FLYTXT_LATCH_DUMP  >> Dedup_FLYTXT_LATCH_DUMP >> End
Validation >> Branching_FLYTXT_LATCH_DUMP >> End
Validation >> Branching_FLYTXT_LATCH_DUMP >> Dedup2_FLYTXT_LATCH_DUMP >> End
Validation >> Branching_FLYTXT_LATCH_DUMP >> faile_FLYTXT_LATCH_DUMP
