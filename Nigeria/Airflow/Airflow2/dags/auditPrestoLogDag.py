import airflow
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 23),
    'email': ['j.adeleke@ligadata.com', 't.olorunfemi@ligadata.com'],
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='load_presto_audit_logs',
    default_args=args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

load_presto_pipeline_logs = BashOperator(
     task_id='load_presto_pipeline_audit_logs',
     bash_command='python3.6 /nas/share03/presto_audit/auditPrestoLog.py -lp /nas/share03/presto_audit -cids presto_adt presto_bsl presto_pipeline presto_prod ',
     dag=dag,
     run_as_user = 'daasuser'
)

retention = BashOperator(
     task_id='retention',
     bash_command='python /nas/share03/presto_audit/retentionAuditTable.py -n 12',
     dag=dag,
     run_as_user = 'daasuser'
)

retention >> load_presto_pipeline_logs
