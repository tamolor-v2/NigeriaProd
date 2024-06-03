import airflow
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'split',
    'depends_on_past': False,
    'start_date': datetime(2019,10,27),
    'email': ['support@ligadata.com','a.olabamidele@logadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG('vr_group_50',
          default_args=default_args,
          schedule_interval=' 0 09 * * *  ',
          catchup=False,
          concurrency=1,
          max_active_runs=1
          )

Command = BashOperator(
    task_id='run_vr_group_50',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_final_new_presto.sh `date --date="-1 days" +%Y%m%d` ALL group12.conf true 2>&1 | tee /nas/share05/tools/ValidationTool_Presto_final/logs/Group50_VR_$(date +%Y%m%d%H%M%S).txt ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
Command = BashOperator(
    task_id='Command2',
    bash_command='echo "Started Processing for `date --date="-1 days" +%Y%m%d` " ',
    trigger_rule='all_success',
    dag=dag,
    run_as_user='daasuser'
)

