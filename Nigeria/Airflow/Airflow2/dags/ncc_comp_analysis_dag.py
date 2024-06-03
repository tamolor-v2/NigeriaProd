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
    'depends_on_past':False,
    'start_date': seven_days_ago,
    'email': 'olorunsegun.adeniyi@mtn.com',
    'email_on_failure': 'olorunsegun.adeniyi@mtn.com',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':True,
}

# instantiate dag
dag = DAG(dag_id='mtnn_ncc_compliance_analysis',
          default_args=args,
#          schedule_interval='0 10 * * *'
          schedule_interval=None
)

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

def kick_off_task(date_p=date_param):
    presto_sql ="""start transaction;
    SET SESSION query_max_stage_count=2000;
    delete from nigeria.simreg_analysis_outcome where tbl_dt = try_cast({0} as bigint);
    commit;""".format(date_p)
    CLI_CMD="""presto --server 10.1.197.146:8099 --catalog hive5 --schema flare_8 --output-format CSV --client-tags oladimo --execute '{0}'""".format(presto_sql)
    
    print(CLI_CMD)
    os.system(CLI_CMD)



with dag:
    kick_off_dag = PythonOperator(task_id='kick_off_dag',python_callable=kick_off_task)

    SDP_BABSE = BashOperator(task_id='SDP_BABSE',bash_command='python3.6 /home/oadeniyi/myScripts/src_simreg_base.py `date --date="-1 days" +%Y%m%d`',     dag=dag,
     run_as_user = 'daasuser')

    PROV_SOURCE = BashOperator(task_id = 'PROV_SOURCE',bash_command='python3.6 /home/oadeniyi/myScripts/prov_recycle.py ',     dag=dag,
     run_as_user = 'daasuser')

    PARENT_CHILD = BashOperator(task_id = 'PARENT_CHILD',bash_command='python3.6 /home/oadeniyi/myScripts/gds_parent_child.py ',     dag=dag,
     run_as_user = 'daasuser')

    SEAMFIX_SMS_ACT = BashOperator(task_id='SEAMFIX_SMS_ACTVN',bash_command='python3.6 /home/oadeniyi/myScripts/seamfix_sms_act.py `date --date="-1 days" +%Y%m%d`',     dag=dag,
     run_as_user = 'daasuser')

    BASE_FROM_CUSTSUBJ = BashOperator(task_id = 'BASE_FROM_CUSTSUBJ',bash_command='python3.6 /home/oadeniyi/myScripts/base_vs_subs_snapd.py `date --date="-1 days" +%Y%m%d`',     dag=dag,
     run_as_user = 'daasuser')

    SUBS_FROM_SDPOFFER = BashOperator(task_id = 'SUBS_FROM_SDPOFFER',bash_command='python3.6 /home/oadeniyi/myScripts/barring_offerlist.py `date --date="-1 days" +%Y%m%d`',     dag=dag,
     run_as_user = 'daasuser')

    join = DummyOperator(task_id='join',trigger_rule='all_success')

    join2 = DummyOperator(task_id='join2',trigger_rule='all_success')

    FINAL_SUMMARY =  BashOperator(task_id = 'FINAL_SUMMARY',bash_command='python3.6 /home/oadeniyi/myScripts/final_analysis.py `date --date="-1 days" +%Y%m%d`',     dag=dag,
     run_as_user = 'daasuser')


    # Set the dependencies for both possibilities
    kick_off_dag >> SDP_BABSE
    kick_off_dag >> PROV_SOURCE
    SDP_BABSE >> join
    PROV_SOURCE >> join
    join >> SEAMFIX_SMS_ACT
    join >> BASE_FROM_CUSTSUBJ
    join >> SUBS_FROM_SDPOFFER
    join >> PARENT_CHILD
    SEAMFIX_SMS_ACT >> join2
    BASE_FROM_CUSTSUBJ >> join2
    SUBS_FROM_SDPOFFER >> join2
    PARENT_CHILD >> join2
    join2 >> FINAL_SUMMARY

