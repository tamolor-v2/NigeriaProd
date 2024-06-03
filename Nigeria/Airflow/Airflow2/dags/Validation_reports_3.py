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


dag = DAG('validation_report_3',
          default_args=default_args,
          schedule_interval=' 0 10 * * *  ',
          catchup=False,
          concurrency=2,
          max_active_runs=1
          )

ValidationTool_BSL= BashOperator(
    task_id='ValidationTool_BSL',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_BSL.sh `date --date="-1 days" +%Y%m%d` all  ValidationTool_BSL.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

AVAYA= BashOperator(
    task_id='AVAYA',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_ADT.sh `date --date="-1 days" +%Y%m%d` all AVAYA.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group5= BashOperator(
    task_id='group5',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh `date --date="-1 days" +%Y%m%d` all group5.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group19= BashOperator(
    task_id='group19',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh `date --date="-1 days" +%Y%m%d` all group19.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

group31= BashOperator(
    task_id='group31',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh `date --date="-1 days" +%Y%m%d` all group31.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
AGILITY= BashOperator(
    task_id='AGILITY',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation_BSL.sh `date --date="-1 days" +%Y%m%d` all AGILITY.conf true   2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
sponsored_data= BashOperator(
    task_id='sponsored_data',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh `date --date="-1 days" +%Y%m%d` all sponsored_data.conf true  2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)

new_sponsored= BashOperator(
    task_id='new_sponsored',
    bash_command='bash /nas/share05/tools/ValidationTool_Presto_final/bin/InvokeValidation.sh `date --date="-1 days" +%Y%m%d` all new_sponsored.conf true   2>&1 | tee  /nas/share05/tools/ValidationTool_Presto_final/logs/validation_report_log_run_$(date +%Y%m%d%H%M%S).txt ',
    dag=dag,
    run_as_user='daasuser',retry_delay=timedelta(minutes=5),retries=3
)
ValidationTool_BSL>>AVAYA>>group5>>group19
group31>>AGILITY>>sponsored_data>>new_sponsored

