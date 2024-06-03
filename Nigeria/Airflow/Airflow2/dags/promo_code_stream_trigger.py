from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta
import calendar
from datetime import date


import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_failure': ['support@ligadata.com','m.shekha@ligadata.com'],
    'email_on_retry': False,
    'retries': 5,
     'retry_delay': timedelta(minutes=5),
    'catchup':True,
    'depends_on_past': False,
    #'start_date': datetime(2020, 10, 9),
}



#date_param = (monthdelta(date.today(), -6)).strftime('%Y%m%d')


dag = DAG(
    dag_id='promo_code_stream_trigger',
    default_args=args,
    schedule_interval='0 * * * *',
    description='stream trigger code to flytxt through Kafka',    
    catchup=False,
    concurrency=1,
    max_active_runs=1
)


refresh_state = BashOperator(
    task_id='refresh_state',
    bash_command= 'bash /nas/share05/scripts/promo_code/cust_data_bundle_purchased_state/promo_trigger_run_helper.sh refresh_state',
    dag=dag,
    run_as_user='daasuser')
prepare_next_events_stream = BashOperator(
    task_id='prepare_next_events_stream',
    bash_command= 'bash /nas/share05/scripts/promo_code/cust_data_bundle_purchased_state/promo_trigger_run_helper.sh prepare_next_events_stream',
    dag=dag,
    run_as_user='daasuser')
recreate_topic_incase_deleted = BashOperator(
    task_id='recreate_topic_incase_deleted',
    bash_command= 'bash /nas/share05/scripts/promo_code/cust_data_bundle_purchased_state/promo_trigger_run_helper.sh recreate_topic_incase_deleted',
    dag=dag,
    run_as_user='daasuser')
stream_events_to_kafka_topic = BashOperator(
    task_id='stream_events_to_kafka_topic',
    bash_command= 'bash /nas/share05/scripts/promo_code/cust_data_bundle_purchased_state/promo_trigger_run_helper.sh stream_events_to_kafka_topic',
    dag=dag,
    run_as_user='daasuser')
add_wave_to_history_pool = BashOperator(
    task_id='add_wave_to_history_pool',
    bash_command= 'bash /nas/share05/scripts/promo_code/cust_data_bundle_purchased_state/promo_trigger_run_helper.sh add_wave_to_history_pool',
    dag=dag,
    run_as_user='daasuser')
refresh_state >> prepare_next_events_stream >> recreate_topic_incase_deleted >> stream_events_to_kafka_topic >> add_wave_to_history_pool

