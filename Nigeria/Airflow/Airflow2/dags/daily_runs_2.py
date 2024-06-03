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
    'email': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['j.fadare@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=-1)).strftime('%Y-%m-%d')
date_param_d1 = (datetime.now() - timedelta(days=-1)).strftime('%Y%m%d')




dag = DAG(
    dag_id='ALL_DAILY_RUN_2',
    default_args=args,
    schedule_interval=None,
    description='D-1 reporting',
    catchup=False,
    concurrency=10,
    max_active_runs=10
)


VOICE = BashOperator(
     task_id='VOICE' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summary.py -s {0} -p VOICE -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)

VAS = BashOperator(
     task_id='VAS' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summary.py -s {0} -p VAS -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


SDP = BashOperator(
     task_id='SDP' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summary.py -s {0} -p SDP -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)

SMS = BashOperator(
     task_id='SMS' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summaryBSL.py -s {0} -p SMS -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)

GPRS = BashOperator(
     task_id='GPRS' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summaryBSL.py -s {0} -p GPRS -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)

OTHER_VAS = BashOperator(
     task_id='OTHER_VAS' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summaryBSL.py -s {0} -p OTHER_VAS -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


PAYMENTS = BashOperator(
     task_id='PAYMENTS' ,
     bash_command='python3.6 /nas/share05/ops/mtnops/usage_summaryBSL.py -s {0} -p PAYMENTS -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)



RUN_SDP_CHECK = BashOperator(
     task_id='RUN_SDP_CHECK' ,
     bash_command='/nas/share05/ops/mtnops/run_sdp.sh ',
     run_as_user='daasuser',
     dag=dag,
)


prepaid_activation = BashOperator(
     task_id='prepaid_activation' ,
     bash_command='perl /nas/share05/ops/daily/prepaid_activation.pl  1 1 ',
     run_as_user='daasuser',
     dag=dag,
)


RUN_POOL_CHECK = BashOperator(
     task_id='RUN_POOL_CHECK' ,
     bash_command='/nas/share05/ops/mtnops/run_pool.sh ',
     run_as_user='daasuser',
     dag=dag,
)

GROSS_ACT_REGION = BashOperator(
     task_id='GROSS_ACT_REGION' ,
     bash_command='/nas/share05/ops/daily/daily_reports.py -s {0} -p GROSS_ACT_REGION -l 1'.format(date_param),
     run_as_user='daasuser',
     dag=dag,
)


SMS_GROSS_ACT = BashOperator(
     task_id='SMS_GROSS_ACT' ,
     bash_command='perl /nas/share05/ops/sms_alert/sms_gross_act.pl 5 1 ',
     run_as_user='daasuser',
     dag=dag,
)


devd = BashOperator(
     task_id='devd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_devd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

simd = BashOperator(
     task_id='simd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_simd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

devm = BashOperator(
     task_id='devm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_devm daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

devmm = BashOperator(
     task_id='devmm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_devmm daily 9999  '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


dev_val = BashOperator(
     task_id='dev_val' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_dev daily 9999  '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


dmc_check = BashOperator(
     task_id='dmc_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh dmc_dump_all {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

lte_hss = BashOperator(
     task_id='lte_hss' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh lte_hss {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

dev_and_sim_d_done = BashOperator(
     task_id='dev_and_sim_d_done' ,
     bash_command='echo done  ',
     run_as_user='daasuser',
     dag=dag,
)



rchd = BashOperator(
     task_id='rchd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

rchm = BashOperator(
     task_id='rchm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchm monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

rchaggrd = BashOperator(
     task_id='rchaggrd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchaggrd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

rchaggrm = BashOperator(
     task_id='rchaggrm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_rchaggrm monthly 9999  '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


rch_val = BashOperator(
     task_id='rch_val' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_rch daily 9999  '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


mk_share_phase_one = BashOperator(
    task_id='mk_share_stage1',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_1'.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_phase_two = BashOperator(
    task_id = 'mk_share_stage2',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_2 '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_phase_three = BashOperator(
    task_id = 'mk_share_stage3',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_3 '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_phase_four = BashOperator(
    task_id='mk_share_stage4',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_4 '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_mk_share_phase_fourb = BashOperator(
    task_id='mk_share_stage4b',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_B4 '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_phase_fourc = BashOperator(
    task_id='mk_share_stage4c',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_C4 '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_phase_five = BashOperator(
    task_id='mk_share_stage5',
    bash_command='/nas/share05/ops/mtnops/NB_summary.py -l 1 -s {0} -p MKSHARE_5 '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

mk_share_phase_six = BashOperator(
    task_id='mk_share_stage6',
    bash_command='/nas/share05/ops/mtnops/mshare_unknown.py -l 1 -s {0} -p MSHARE_UNKNOWN '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')
mk_share_phase_seven = BashOperator(
    task_id='mk_share_stage7',
    bash_command='/nas/share05/ops/mtnops/mshare_ops_dashboard.py -l 1 -s {0} -p MSHARE_VALIDATION '.format(date_param),
    dag=dag,
    run_as_user = 'daasuser')

join = DummyOperator(
    task_id='join',
    trigger_rule='all_success',      
    dag=dag,
    run_as_user = 'daasuser')

end_dag = BashOperator(
    task_id='end_dag',
    bash_command='echo end_dag ',      
    dag=dag,
    run_as_user = 'daasuser')


post_usage_summary = BashOperator(
     task_id='post_usage_summary' ,
     bash_command='/nas/share05/ops/mtnops/post_usage_summary.sh ',
     run_as_user='daasuser',
     dag=dag,
)

revd = BashOperator(
     task_id='revd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revdd = BashOperator(
     task_id='revdd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revdd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revm = BashOperator(
     task_id='revm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revm monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revmm = BashOperator(
     task_id='revmm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revmm monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revaggrscd = BashOperator(
     task_id='revaggrscd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revaggrscm = BashOperator(
     task_id='revaggrscm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscm monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revaggrscdd = BashOperator(
     task_id='revaggrscdd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscdd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revaggrscmm = BashOperator(
     task_id='revaggrscmm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrscmm monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

revaggrd = BashOperator(
     task_id='revaggrd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrd daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


revaggrm = BashOperator(
     task_id='revaggrm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_revaggrm monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


rev_evtd = BashOperator(
     task_id='rev_evtd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_evtd monthly 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

rev_val = BashOperator(
     task_id='rev_val' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_rev daily 9999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


usgd = BashOperator(
     task_id='usgd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgd daily 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgdd = BashOperator(
     task_id='usgdd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgdd daily 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgm = BashOperator(
     task_id='usgm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgm monthly 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgmm = BashOperator(
     task_id='usgmm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgmm monthly 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgaggrscd = BashOperator(
     task_id='usgaggrscd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscd daily 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgaggrscm = BashOperator(
     task_id='usgaggrscm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscm monthly 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgaggrscdd = BashOperator(
     task_id='usgaggrscdd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscdd daily 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgaggrscmm = BashOperator(
     task_id='usgaggrscmm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrscmm monthly 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

usgaggrd = BashOperator(
     task_id='usgaggrd' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrd daily 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


usgaggrm = BashOperator(
     task_id='usgaggrm' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_usgaggrm monthly 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)



usg_val = BashOperator(
     task_id='usg_val' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_usg daily 8999   '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


run_subpred_1 = BashOperator(
     task_id='run_subpred_1' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subpred1 daily 8668'.format(date_param_d1) ,
     dag=dag,
     run_as_user = 'daasuser'


)

run_subpred_2 = BashOperator(
     task_id='run_subpred_2' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subpred2 daily 8668'.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)


subpred = BashOperator(
     task_id='subpred' ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subpred daily 8668'.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)


subpred_check = BashOperator(
     task_id='subpred_check' ,
     bash_command='/nas/share05/ops/mtnops/subpred_check.sh  ',
     run_as_user='daasuser',
     dag=dag,
)


sub_wait = BashOperator(
     task_id='sub_wait' ,
     bash_command='echo moonthly aggr wait  ',
     run_as_user='daasuser',
     dag=dag,
)

cs_check = BashOperator(
     task_id='cs_check' ,
     bash_command='/nas/share05/ops/mtnops/cs_check.sh {0} ',
     run_as_user='daasuser',
     dag=dag,
)

cs_check_trend = BashOperator(
     task_id='cs_check_trend' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh customersubject {0} '.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)


dropsubbase = BashOperator(
     task_id='dropsubbase',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp99"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubgeo = BashOperator(
     task_id='dropsubgeo',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp98"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubusim = BashOperator(
     task_id='dropsubusim',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp97"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubrevrank = BashOperator(
     task_id='dropsubrevrank',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp96"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubrev = BashOperator(
     task_id='dropsubrev',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp95"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubusg = BashOperator(
     task_id='dropsubusg',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp93"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubsmartdev = BashOperator(
     task_id='dropsubsmartdev',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp92"' ,      
     dag=dag,
 run_as_user = 'daasuser')

dropsubsmartdevnew = BashOperator(
     task_id='dropsubsmartdevnew',
     bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp91"' ,      
     dag=dag,
 run_as_user = 'daasuser')

subbase = BashOperator(
     task_id='subbase'
     ,
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subbase daily 8668'.format(date_param_d1) ,      
     dag=dag,
     run_as_user = 'daasuser')

subgeo = BashOperator(
     task_id='subgeo',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subgeo daily 8668'.format(date_param_d1) ,      
     dag=dag,
     run_as_user = 'daasuser')

subusim = BashOperator(
     task_id='subusim',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subusim daily 8668'.format(date_param_d1) ,      
     dag=dag,
     run_as_user = 'daasuser')

subrevrank = BashOperator(
     task_id='subrevrank',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subrevrank daily 8668'.format(date_param_d1) ,      
     dag=dag,
     run_as_user = 'daasuser')

subrev = BashOperator(
     task_id='subrev',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subrev daily 8668'.format(date_param_d1) ,      
     dag=dag,
     run_as_user = 'daasuser')

subusg = BashOperator(
     task_id='subusg',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subusg daily 8668'.format(date_param_d1) ,      
     dag=dag,
     run_as_user = 'daasuser')

subsmartdev = BashOperator(
     task_id='subsmartdev',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subsmartdev daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')

subsmartdevnew = BashOperator(
     task_id='subsmartdevnew',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subsmartdevnew daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')

subfinal = BashOperator(
     task_id='subfinal',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subfinal daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')


subaggrd = BashOperator(
     task_id='subaggrd',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subaggrd daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')

subaggrscd = BashOperator(
     task_id='subaggrscd',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subaggrscd daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')

validation_sub = BashOperator(
     task_id='validation_sub',
     bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_sub daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')


sub_mon = BashOperator(
     task_id='sub_mon',
     bash_command='/nas/share05/scripts/segment5b5/run_segment5b5.sh {0} {0} segment5b5_mon daily 8668'.format(date_param_d1) ,      
     dag=dag,
 run_as_user = 'daasuser')



hsdp_sumd = BashOperator(
     task_id='hsdp_sumd' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HSDP_SUMD -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


hynet_offers = BashOperator(
     task_id='hynet_offers' ,
     bash_command='/nas/share05/ops/mtnops/NB_summary.py -p HYNET_OFFERS -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

vas_revenue = BashOperator(
     task_id='vas_revenue' ,
     bash_command='perl /nas/share05/ops/daily/mkt_ent_vas_revenue_metrics_v8.pl ',
     dag=dag,
     run_as_user = 'daasuser'
)


data_revenue = BashOperator(
     task_id='data_revenue' ,
     bash_command='perl /nas/share05/ops/daily/mkt_data_revenue_metrics_v9_p.pl  ',
     dag=dag,
     run_as_user = 'daasuser'
)


pbi_data_report_kpis = BashOperator(
     task_id='pbi_data_report_kpis' ,
     bash_command='/nas/share05/ops/mtnops/power_bi_refresh.py -l 1 -p pbi_data_report_kpis -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)



uniq_purchased_bundle = BashOperator(
     task_id='uniq_purchased_bundle' ,
     bash_command='/nas/share05/ops/mtnops/power_bi_refresh.py -l 1 -p uniq_purchased_bundle -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


UNIQ_PURCHASED_ALL = BashOperator(
     task_id='UNIQ_PURCHASED_ALL' ,
     bash_command='/nas/share05/ops/mtnops/power_bi_refresh.py -l 1 -p UNIQ_PURCHASED_ALL -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


UNIQ_PURCHASED_OOB_DAILY = BashOperator(
     task_id='UNIQ_PURCHASED_OOB_DAILY' ,
     bash_command='/nas/share05/ops/mtnops/power_bi_refresh.py -l 1 -p UNIQ_PURCHASED_OOB_DAILY -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


AUTO_RENEWAL_MTD = BashOperator(
     task_id='AUTO_RENEWAL_MTD' ,
     bash_command='/nas/share05/ops/mtnops/power_bi_refresh.py -l 1 -p AUTO_RENEWAL_MTD -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)


recharge_subscribers = BashOperator(
     task_id='recharge_subscribers' ,
     bash_command='perl /nas/share05/ops/mtnops/recharge_subscribers_count.pl {0} {0} '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


#sub_metrics_check = BashOperator(
#     task_id='sub_metrics_check' ,
#     bash_command='/nas/share05/ops/mtnops/sub_metrics_check.sh  ',
#     run_as_user='daasuser',
#     dag=dag,
#)


FULL_BARRED = BashOperator(
     task_id='FULL_BARRED' ,
     bash_command='/nas/share05/ops/mtnops/barred_disconn.py -p FULL_BARRED -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

BARRING_METRICS = BashOperator(
     task_id='BARRING_METRICS' ,
     bash_command='/nas/share05/ops/mtnops/barred_disconn.py -p BARRING_METRICS -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

MSISDN_BARRED = BashOperator(
     task_id='MSISDN_BARRED' ,
     bash_command='/nas/share05/ops/mtnops/barred_disconn.py -p MSISDN_BARRED -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

BARRED_MSISDN_GC_BASE = BashOperator(
     task_id='BARRED_MSISDN_GC_BASE' ,
     bash_command='/nas/share05/ops/mtnops/barred_disconn.py -p BARRED_MSISDN_GC_BASE -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

BARRED_MSISDN_CHURN_BASE = BashOperator(
     task_id='BARRED_MSISDN_CHURN_BASE' ,
     bash_command='/nas/share05/ops/mtnops/barred_disconn.py -p BARRED_MSISDN_CHURN_BASE -l 1 -s {0} '.format(date_param),
     dag=dag,
     run_as_user = 'daasuser'
)

voice_ma_check = BashOperator(
     task_id='voice_ma_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh cs5_ccn_voice_ma {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


refill_ma_check = BashOperator(
     task_id='refill_ma_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh cs5_air_refill_ma {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

gprs_ma_check = BashOperator(
     task_id='gprs_ma_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh cs5_ccn_gprs_ma {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

adj_ma_check = BashOperator(
     task_id='adj_ma_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh cs5_sdp_acc_adj_ma {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

vtu_dump_check = BashOperator(
     task_id='vtu_dump_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh cs5_vtu_dump {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

ers_vend_check = BashOperator(
     task_id='ers_vend_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh ers_vend_new {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

sms_ma_check = BashOperator(
     task_id='sms_ma_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh cs5_ccn_sms_ma {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

ngvs_check = BashOperator(
     task_id='ngvs_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh ngvs_cdr {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


bundle4u_gprs_check = BashOperator(
     task_id='bundle4u_gprs_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh bundle4u_gprs {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

bundle4u_voice_check = BashOperator(
     task_id='bundle4u_voice_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh bundle4u_voice {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

ncc_events = BashOperator(
     task_id='ncc_events' ,
     bash_command='perl /nas/share05/ops/daily/ncc_events.pl {0} {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


ncc_bsl = BashOperator(
     task_id='ncc_bsl' ,
     bash_command='perl /nas/share05/ops/daily/ncc_bsl.pl {0} {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)

ncc_customersubject_consolidated = BashOperator(
     task_id='ncc_customersubject_consolidated' ,
     bash_command='perl /nas/share05/ops/daily/ncc_customersubject_consolidated.pl {0} {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


dim_handset = BashOperator(
     task_id='dim_handset',
     bash_command=' bash /nas/share05/tools/IMEI_report/airflow_dim_handset.sh ',
     dag=dag,

     run_as_user='daasuser'
)
imei_tracker = BashOperator(
     task_id='imei_tracker',
     bash_command=' bash /nas/share05/tools/IMEI_report/airflow_imei_tracker.sh ',
     dag=dag,     
     run_as_user='daasuser'
)


gross_connection = BashOperator(
     task_id='gross_connection' ,
     bash_command='perl /nas/share05/ops/daily/daily_rgs_conn.pl 1 1 ',
     run_as_user='daasuser',
     dag=dag,
)

kitchen_activations = BashOperator(
     task_id='kitchen_activations' ,
     bash_command='bash /nas/share05/scripts/kitchen_activations/run_kitchen_activations.sh {0} {0} '.format(date_param_d1),
     run_as_user='daasuser',
     dag=dag,
)


sub_metrics = BashOperator(
     task_id='sub_metrics' ,
     bash_command='perl /nas/share05/ops/daily/daily_sub_metrics.pl 1 1 ',
     run_as_user='daasuser',
     dag=dag,
)


metrics = BashOperator(
     task_id='metrics' ,
     bash_command='/nas/share05/ops/daily/daily_metrics_new.pl 1 1 ',
     run_as_user='daasuser',
     dag=dag,
)


gross_conn_check = BashOperator(
     task_id='gross_conn_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh gross_conn_check {0} '.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)

metrics_check = BashOperator(
     task_id='metrics_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh metrics_check {0} '.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)


sub_metrics_check = BashOperator(
     task_id='sub_metrics_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh sub_metrics_check {0} '.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)

rev_daily2_dups_check = BashOperator(
     task_id='rev_daily2_dups_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh rev_daily2_dups_check {0} '.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)


usg_daily2_dups_check = BashOperator(
     task_id='usg_daily2_dups_check' ,
     bash_command='/nas/share05/ops/mtnops/feed_check.sh usg_daily2_dups_check {0} '.format(date_param_d1) ,
     run_as_user='daasuser',
     dag=dag,
)

checks_done = BashOperator(task_id='checks_done',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
D_1_cleared = BashOperator(task_id='D_1_cleared',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
CHECK_LIVE_FEEDS = BashOperator(task_id='CHECK_LIVE_FEEDS',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
REFRESH_CONTAINER = BashOperator(task_id='REFRESH_CONTAINER',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
PCF = BashOperator(task_id='PCF',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
DEDUP = BashOperator(task_id='DEDUP',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
BSL_EXTRACTION = BashOperator(task_id='BSL_EXTRACTION',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
BSL_CALCULATION = BashOperator(task_id='BSL_CALCULATION',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
POST_BSL = BashOperator(task_id='POST_BSL',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
GEOGRAPHY = BashOperator(task_id='GEOGRAPHY',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
FLYTXT = BashOperator(task_id='FLYTXT',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
#SUB_METRICS = BashOperator(task_id='SUB_METRICS',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
#RGS_CONN = BashOperator(task_id='RGS_CONN',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
#KITCHEN_ACTIVATION = BashOperator(task_id='KITCHEN_ACTIVATION',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
#METRICS = BashOperator(task_id='METRICS',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
RGS_SMS = BashOperator(task_id='RGS_SMS',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
release_revenue_sms = BashOperator(task_id='release_revenue_sms',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
release_recharges_sms = BashOperator(task_id='release_recharges_sms',bash_command='echo end',dag=dag, run_as_user = 'daasuser')
Digital_vas = BashOperator(task_id='Digital_vas',bash_command='echo end',dag=dag, run_as_user = 'daasuser')

[mk_share_phase_one,mk_share_phase_two] >> join >> mk_share_phase_three >> [mk_share_mk_share_phase_fourb,mk_share_phase_four,mk_share_phase_fourc] >> cs_check >> mk_share_phase_five >> mk_share_phase_six >> mk_share_phase_seven >> subsmartdevnew >> end_dag

RUN_SDP_CHECK >> [SDP,RUN_POOL_CHECK]
RUN_POOL_CHECK>> prepaid_activation >> GROSS_ACT_REGION >> SMS_GROSS_ACT

dmc_check >> devd >> dev_and_sim_d_done
lte_hss  >> simd  >> dev_and_sim_d_done

dmc_check >> [dim_handset , imei_tracker]

dev_and_sim_d_done >> devm >> devmm >> sub_wait 

devmm >> dev_val

post_usage_summary >> run_subpred_1 >> run_subpred_2 >> subpred >> subpred_check >> sub_wait


post_usage_summary >> rchd >> rchm >> sub_wait

rchm >> rchaggrd >> rchaggrm >> rch_val

post_usage_summary >> revd >> revdd >> rev_daily2_dups_check >> revm >> revmm >> sub_wait 

revmm  >> revaggrscd >> revaggrscm >> revaggrscdd >> revaggrscmm >> revaggrd >> revaggrm >> rev_evtd >> rev_val

post_usage_summary >> usgd >> usgdd >> usg_daily2_dups_check >> usgm >> usgmm >> sub_wait 
 
usgmm >> usgaggrscd >> usgaggrscm >> usgaggrscdd >> usgaggrscmm >> usgaggrd >> usgaggrm  >> usg_val 

sub_wait >> cs_check >> cs_check_trend >> dropsubbase >> subbase >> dropsubgeo >> subgeo >> dropsubusim >> subusim >> dropsubrevrank >> subrevrank >> dropsubrev >> subrev >> dropsubusg  >> subusg >> dropsubsmartdev >> subsmartdev >> dropsubsmartdevnew >> subsmartdevnew >> subfinal >> subaggrd >>subaggrscd >> validation_sub >> sub_mon

sub_metrics_check >> [FULL_BARRED, BARRING_METRICS , MSISDN_BARRED, BARRED_MSISDN_GC_BASE, BARRED_MSISDN_CHURN_BASE]

checks_done >> VOICE >> post_usage_summary
checks_done >> VAS >> post_usage_summary
checks_done >> SMS >> post_usage_summary
checks_done >> GPRS >> post_usage_summary
checks_done >> OTHER_VAS >> post_usage_summary
checks_done >> PAYMENTS >> post_usage_summary

vas_revenue >> subsmartdevnew
post_usage_summary >> ncc_events >> ncc_bsl >> ncc_customersubject_consolidated >> subsmartdevnew
BSL_CALCULATION >> cs_check_trend >>  gross_connection >> gross_conn_check >> [kitchen_activations , sub_metrics ] >> sub_metrics_check  >> metrics >> metrics_check >> RGS_SMS
cs_check_trend >> GEOGRAPHY

post_usage_summary >> hsdp_sumd >> hynet_offers >> vas_revenue
post_usage_summary >> data_revenue >> [pbi_data_report_kpis,uniq_purchased_bundle, UNIQ_PURCHASED_ALL, UNIQ_PURCHASED_OOB_DAILY, AUTO_RENEWAL_MTD]


post_usage_summary >> recharge_subscribers

DEDUP >> voice_ma_check >> checks_done
DEDUP >> gprs_ma_check >> checks_done
DEDUP >> refill_ma_check >> checks_done
DEDUP >> adj_ma_check >> checks_done
DEDUP >> vtu_dump_check >> checks_done
DEDUP >> ers_vend_check >> checks_done
DEDUP >> bundle4u_gprs_check >> checks_done
DEDUP >> bundle4u_voice_check >> checks_done
DEDUP >> sms_ma_check >> checks_done
DEDUP >> ngvs_check >> checks_done


D_1_cleared >> [CHECK_LIVE_FEEDS, REFRESH_CONTAINER] 
CHECK_LIVE_FEEDS >> PCF >> DEDUP >> [checks_done,BSL_EXTRACTION]

DEDUP >> RUN_SDP_CHECK

BSL_EXTRACTION >> BSL_CALCULATION >> POST_BSL >> FLYTXT
BSL_CALCULATION >> GEOGRAPHY >> subsmartdevnew


post_usage_summary >> [release_revenue_sms, release_recharges_sms]

post_usage_summary >> Digital_vas >> subsmartdevnew 

GEOGRAPHY >> subsmartdevnew

