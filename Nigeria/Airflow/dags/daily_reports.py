from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': True,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

date_param = (datetime.now() - timedelta(days=0)).strftime('%Y-%m-%d') 
 
dag = DAG(
    dag_id='Daily_Reports_a',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

#gsm_status = BashOperator(
#     task_id='GSM_Status_report',
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p GSM_STS',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

#act_sum_rep = BashOperator(
#     task_id='Account_Summary_Report',
#     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p ACC_SUMM_REP',
#     dag=dag,
#     run_as_user = 'daasuser'
#)

VisaFone_AOD = BashOperator(
     task_id='VisaFone_AOD_Summary', 
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p aod',
     dag=dag,
     run_as_user = 'daasuser'
)

call_to_180 = BashOperator(
     task_id='Call_To_180',
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p 180',
     dag=dag,
     run_as_user = 'daasuser'
)

CDR_Summary = BashOperator(
     task_id='CDR_Summary',
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p CDR',
     dag=dag,
     run_as_user = 'daasuser'
)

cust_ord_invo = BashOperator(
     task_id='cust_ord_invo',
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p CUST_ORD',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

SIM_ACTIVATION = BashOperator(
     task_id='SIM_ACTIVATION',
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p SIM_ACTIVATION',
     dag=dag,
     run_as_user = 'daasuser'
)

Epost_Paid_GPRS = BashOperator(
     task_id='Epost-Paid_GPRS',
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p EPOST_GPRS',
     dag=dag,
     run_as_user = 'daasuser'
)

epostpaid_mou_voice = BashOperator(
     task_id='epostpaid_mou_voice',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p EPOSTPAID_MOU_VOICE -l 3',
     dag=dag,
     run_as_user = 'daasuser'
)
    
epostpaid_mou_sms = BashOperator(
     task_id='epostpaid_mou_sms',
     bash_command='/nas/share05/ops/mtnops/OT_summary.py -p EPOSTPAID_MOU_SMS -l 3',
     run_as_user = 'daasuser',
     dag=dag
)

Hynet_Summary = BashOperator(
     task_id='Hynet_Summary',
     depends_on_past=True,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p hynet',
     dag=dag,
     run_as_user = 'daasuser'
)

ISP_Login = BashOperator(
     task_id='ISP_Login',
     depends_on_past=True,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p ISP_LOGIN ',
     dag=dag,
     run_as_user = 'daasuser'
)


Momo_Summary = BashOperator(
     task_id='Momo_Summary',
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p momo ',
     dag=dag,
     run_as_user = 'daasuser'
)

BANK_TRANSACTIONS = BashOperator(
     task_id='BANK_TRANSACTIONS',
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p MOD_TRANS ',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

Neon_Summary = BashOperator(
     depends_on_past=True,
     task_id='Neon_Summary',
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p neon ',
     dag=dag,
     run_as_user = 'daasuser'
)

infraction_late = BashOperator(
     task_id='infraction_late' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p INFRACTION -s {0} -l 8'.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

SMS_ACTIVATION_REQ = BashOperator(
     task_id='SMS_ACTIVATION_REQ' ,
     bash_command='/nas/share05/ops/mtnops/data_ingestion.py -p B_SMS_ACTIVATION_REQUEST -l 1 -s `date --date="-1 days" +%Y-%m-%d`',
     dag=dag,
     run_as_user = 'daasuser'
)


#No_Mgmt_Summary = BashOperator(
#     task_id='No_Mgmt_Summary',
#     depends_on_past=True,
#     bash_command='python3.6 /nas/share05/ops/mtnops/daily_summaries.py -p NOMGMT ',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


infraction_late >> SMS_ACTIVATION_REQ >> VisaFone_AOD >> call_to_180 >> CDR_Summary >> cust_ord_invo >> SIM_ACTIVATION >> Epost_Paid_GPRS >> epostpaid_mou_sms >> epostpaid_mou_voice >> Hynet_Summary >> ISP_Login >> Momo_Summary >> BANK_TRANSACTIONS >> Neon_Summary 
