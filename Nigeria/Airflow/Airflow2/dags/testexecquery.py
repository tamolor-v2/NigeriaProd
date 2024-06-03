import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from runPresto import presto



args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['y.bloukh@ligadata.com'],
    'email_on_failure': ['y.bloukh@ligadata.com'],
    'email_on_retry': False,
    'retries': 50,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG('a_test_query_parse',
    default_args=args,
    schedule_interval= "0 */12 * * * ",
    catchup=False,
    concurrency=1,
    max_active_runs=1)


prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'
query = 'select 12345'

oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)

def executeQ(query):
    qOutput = oPresto.executeQuery
    if str(qOutput[0][0]) != '0':
        print(qOutput[0][0])

runAndParse=PythonOperator(
    task_id = 'runAndParse',
    python_callable=executeQ,
    op_args = [query],
    dag=dag,
    run_as_user = 'daasuser'
)
