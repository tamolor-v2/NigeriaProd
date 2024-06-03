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
    #run_as_user = 'daasuser'
}
 
dag = DAG(
    dag_id='Daily_reports_b',
    default_args=args,
    schedule_interval='0 6 * * *',
    catchup=True,
    concurrency=1,
    max_active_runs=1
)

Short_Code_Summary = BashOperator(
     task_id='Short_Code_Summary' ,
     depends_on_past=True,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p SHORT_CODE ',
     dag=dag,
     run_as_user = 'daasuser'
)

Smart_Apps_CDR = BashOperator(
     task_id='Smart_Apps_CDR' ,
     depends_on_past=True,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p SMART_APPS ',
     dag=dag,
     run_as_user = 'daasuser'
)



#RUN_REV = BashOperator(
#     task_id='run_rev' ,
#     bash_command='bash /nas/share05/scripts/revenue/run_rev.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


#Top1000 = BashOperator(
#     task_id='Top1000' ,
#     bash_command='bash /nas/share05/ops/scripts/top1000/run_top1000.sh `date --date="-3 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`	',
#     dag=dag,
#     run_as_user = 'daasuser'
#)
    
#top1000_log = BashOperator(
#     task_id='log' ,
#     bash_command='tail -30 /nas/share05/ops/scripts/log/top1000_spender.out	',
#     dag=dag,
#     run_as_user = 'daasuser'
#)


vtu_channel_sumd = BashOperator(
     task_id='vtu_channel_sumd' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p VTU_SUMD -l 3 ',
     dag=dag,
     run_as_user = 'daasuser'
)

RATTYPE = BashOperator(
     task_id='RATTYPE' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p RAT_TYPE -l 15',
     dag=dag,
     run_as_user = 'daasuser'
)

SDP_Bal_SC = BashOperator(
     task_id='SDP_Bal_SC' ,
     bash_command='/nas/share05/ops/mtnops/month_end_reports.py -p SDP_Bal_SC -l 5',
     depends_on_past=True,
     dag=dag,
     run_as_user = 'daasuser'
)

Service_Invoice = BashOperator(
     task_id='Service_Invoice' ,
     bash_command='/nas/share05/ops/mtnops/EDW_Service_Invoice.py `date --date="-1 days" +%Y%m%d`',
     dag=dag,
     run_as_user = 'daasuser'
)

RECH_BY_VOUCHER = BashOperator(
     task_id='rech_by_voucher' ,
     bash_command='perl /nas/share05/ops/monthly/rech_by_voucher.pl `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m%d`  ',
     dag=dag,
     run_as_user = 'daasuser'
)

MODTRANSACTIONS_ACT = BashOperator(
     task_id='modtransactions_act' ,
     bash_command='/nas/share05/ops/mtnops/mod_transaction.py -p MODTRANSACTIONS_ACT -s `date --date="-3 days" +%Y-%m-%d` -l 3 ',
     dag=dag,
     run_as_user = 'daasuser'
)

SIM_REG_PBI = BashOperator(
     task_id='sim_reg_pbi' ,
     bash_command='/nas/share05/ops/mtnops/daily_summaries.py -p SIM_REG_PBI -l 3 ',
     dag=dag,
     run_as_user = 'daasuser'
)

PORT_OUT_BALANCE = BashOperator(
     task_id='port_out_balance' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/port_out_balance.py ',
     dag=dag,
     run_as_user = 'daasuser'
)        

SIM_REG_PBI >> RECH_BY_VOUCHER >> Short_Code_Summary >> Smart_Apps_CDR >> vtu_channel_sumd >> SDP_Bal_SC >> Service_Invoice >>  MODTRANSACTIONS_ACT >>  RATTYPE >> PORT_OUT_BALANCE
