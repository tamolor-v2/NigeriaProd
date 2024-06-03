import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                  datetime.min.time())

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

# instantiate dag
dag = DAG(dag_id='Flytxt_monitor',
          default_args=args,
          schedule_interval='0 12 * * *')

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

def kick_off_task(date_param=date_param):
	presto_sql = """SET SESSION query_max_stage_count=2000;
delete from nigeria.flytxt_offer_attachment where start_dt = {0};
insert into nigeria.flytxt_offer_attachment (start_dt,OFFER_ID,MSISDN_COUNT)
SELECT try_cast(date_format(from_unixtime(adjustmenttimestamp/1000),'%Y%m%d') as bigint) start_dt,r_off1_offer_id as OFFER_ID,COUNT(distinct msisdn_key) MSISDN_COUNT FROM flare_8.CS5_SDP_ACC_ADJ_MA 
WHERE tbl_dt = {1}
AND r_off1_offer_id in ('5043', '60084', '60088', '60099', '1196','20600','11012', '5095', '16029', '16019', '16025','5701','5700')
AND date_parse(date_format(from_unixtime(adjustmenttimestamp/1000),'%Y%m%d'),'%Y%m%d') = date_parse(try_cast({2} as varchar),'%Y%m%d')
AND UPPER(ORIGINNODEID) = 'NEON'
GROUP BY date_format(from_unixtime(adjustmenttimestamp/1000),'%Y%m%d'),r_off1_offer_id;
delete from nigeria.flytxt_da_crediting where tbl_dt = {3};
insert into nigeria.flytxt_da_crediting (tbl_dt,DA, DA_amt,MSISDN_COUNT)
SELECT TBL_DT,SPLIT(DAID,',')[1] DA,SUM(try_cast(SPLIT(DAID,',')[4] as decimal)) DA_amt,COUNT(distinct ACCOUNTNUMBER) MSISDN_COUNT FROM flare_8.CS5_SDP_ACC_ADJ_MA 
WHERE tbl_dt  = {4}
AND UPPER(ORIGINNODEID) = 'NEON'
--AND SPLIT(DAID,',')[1] in ('140','8065')
AND TRY_CAST(ADJUSTMENTACTION AS BIGINT) = 1
GROUP BY SPLIT(DAID,',')[1],TBL_DT
ORDER BY TBL_DT;
delete from nigeria.flytxt_smsc_tracker where tbl_dt = {5};
insert into nigeria.flytxt_smsc_tracker(tbl_dt,locussm_status,msisdn,distinct_msisdn)
SELECT TBL_DT,LOCUSSM_STATUS, COUNT(DEST_ADDR) MSISDN,count(distinct DEST_ADDR) DISTINCT_MSISDN
FROM FLARE_8.SMSC where tbl_dt  = {6}
AND ORGACCOUNT = 'FLYTXT_SMPP'
GROUP BY TBL_DT,LOCUSSM_STATUS
ORDER by tbl_dt;""".format(date_param,date_param,date_param,date_param,date_param,date_param,date_param).replace("'", "'\\''")
	presto_cmd = "presto --server master01004:8099 --catalog hive5 --schema nigeria --output-format CSV --client-tags oadeniyi --execute '{0}'".format(presto_sql)
	os.system(presto_cmd)

def latch_performance(date_param=date_param):
	presto_sql = """SET SESSION query_max_stage_count=2000;
delete from nigeria.latch_performance where tbl_dt = {0};
insert into nigeria.latch_performance(tbl_dt,perc_0_to_5secs,perc_0_to_10secs,perc_0_to_30secs,perc_0_to_60secs,perc_0_to_2mins,perc_0_to_5mins,perc_above5mins)
select cast (tbl_dt as integer)  tbl_dt,
cast((cast(cumm_0_to_5secs as double)/cast(total_sms as double))*100 as double) perc_0_to_5secs,
cast((cast(cumm_0_to_10secs as double)/cast(total_sms as double))*100 as double)  perc_0_to_10secs,
cast((cast(cumm_0_to_30secs as double)/cast(total_sms as double))*100 as double)  perc_0_to_30secs,
cast((cast(cumm_0_to_60secs as double)/cast(total_sms as double))*100 as double)  perc_0_to_60secs,
cast((cast(cumm_0_to_2mins as double)/cast(total_sms as double))*100 as double)  perc_0_to_2mins,
cast((cast(cumm_0_5mins as double)/cast(total_sms as double))*100 as double)  perc_0_to_5mins,
cast((cast(time_above_5mins as double)/cast(total_sms as double))*100 as double)  perc_above5mins
from
(
select  tbl_dt,count(msisdn) TOTAL_SMS ,count(case when cast(split(time_diff,' ')[1] as integer) <=5  then msisdn end) cumm_0_to_5secs,
count(case when cast(split(time_diff,' ')[1] as integer) <= 10  then msisdn end) cumm_0_to_10secs,
count(case when cast(split(time_diff,' ')[1] as integer) <= 30  then msisdn end) cumm_0_to_30secs,
count(case when cast(split(time_diff,' ')[1] as integer) <= 60  then msisdn end) cumm_0_to_60secs,
count(case when cast(split(time_diff,' ')[1] as integer) <= 120 then msisdn end) cumm_0_to_2mins,
count(case when cast(split(time_diff,' ')[1] as integer) <= 300  then msisdn end) cumm_0_5mins,
count(case when cast(split(time_diff,' ')[1] as integer) > 300 then msisdn end) time_Above_5mins
 from
flare_8.FLYTXT_LATCH_DUMP where tbl_dt = {1} group by tbl_dt);""".format(date_param,date_param).replace("'", "'\\''")
	presto_cmd = "presto --server master01004:8099 --catalog hive5 --schema nigeria --output-format CSV --client-tags oadeniyi --execute '{0}'".format(presto_sql)
	os.system(presto_cmd)


with dag:
    kick_off_dag = PythonOperator(task_id='kick_off_dag',python_callable=kick_off_task)
    latch_perf = PythonOperator(task_id='flyxt_latch_performance',python_callable=latch_performance,run_as_user = 'daasuser')


    # Set the dependencies for both possibilities
    kick_off_dag >> latch_perf
