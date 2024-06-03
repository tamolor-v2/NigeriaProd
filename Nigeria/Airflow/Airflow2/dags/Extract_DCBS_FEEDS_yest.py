from datetime import datetime, timedelta
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone
from os import popen

args = {
    'owner': 'MTN Nigeria',
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['n.najjar@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['n.najjar@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='Extract_DCBS_FEEDS_yest',
    default_args=args,
    schedule_interval='0 5 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

extract_09_DCBS_COLLECTIONS_COL_CUSTOMER_TRANSACTION_yest = BashOperator(
     task_id='extract_09_DCBS_COLLECTIONS_COL_CUSTOMER_TRANSACTION_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_09_DCBS_COLLECTIONS_COL_CUSTOMER_TRANSACTION_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_10_DCBS_COLLECTIONS_COL_LEDGER_TRANSACTIONS_yest = BashOperator(
     task_id='extract_10_DCBS_COLLECTIONS_COL_LEDGER_TRANSACTIONS_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_10_DCBS_COLLECTIONS_COL_LEDGER_TRANSACTIONS_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_11_DCBS_COLLECTIONS_COL_ADJUSTED_DETAILS_yest = BashOperator(
     task_id='extract_11_DCBS_COLLECTIONS_COL_ADJUSTED_DETAILS_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_11_DCBS_COLLECTIONS_COL_ADJUSTED_DETAILS_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_12_DCBS_COLLECTIONS_COL_TRANSACTION_CORRELATION_yest = BashOperator(
     task_id='extract_12_DCBS_COLLECTIONS_COL_TRANSACTION_CORRELATION_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_12_DCBS_COLLECTIONS_COL_TRANSACTION_CORRELATION_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_05_DCBS_REPORTS_MTN_EXCESS_PAYMENT_TRANSFER_VW_yest = BashOperator(
     task_id='extract_05_DCBS_REPORTS_MTN_EXCESS_PAYMENT_TRANSFER_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_05_DCBS_REPORTS_MTN_EXCESS_PAYMENT_TRANSFER_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_06_DCBS_REPORTS_MTN_EXCESS_PAYMENT_VW_yest = BashOperator(
     task_id='extract_06_DCBS_REPORTS_MTN_EXCESS_PAYMENT_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_06_DCBS_REPORTS_MTN_EXCESS_PAYMENT_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_DCBS_REPORTS_MTN_SERV_BALANCE_VW_yest = BashOperator(
     task_id='extract_DCBS_REPORTS_MTN_SERV_BALANCE_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_DCBS_REPORTS_MTN_SERV_BALANCE_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_DCBS_REPORTS_MTN_SERV_ALL_PAYMENT_VW_yest = BashOperator(
     task_id='extract_DCBS_REPORTS_MTN_SERV_ALL_PAYMENT_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_DCBS_REPORTS_MTN_SERV_ALL_PAYMENT_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_13_DCBS_COLLECTIONS_PROFILES_yest = BashOperator(
     task_id='extract_13_DCBS_COLLECTIONS_PROFILES_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_13_DCBS_COLLECTIONS_PROFILES_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_14_DCBS_COLLECTIONS_BILLING_ACCOUNTS_yest = BashOperator(
     task_id='extract_14_DCBS_COLLECTIONS_BILLING_ACCOUNTS_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_14_DCBS_COLLECTIONS_BILLING_ACCOUNTS_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_15_DCBS_COLLECTIONS_SERVICE_ACCOUNTS_yest = BashOperator(
     task_id='extract_15_DCBS_COLLECTIONS_SERVICE_ACCOUNTS_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_15_DCBS_COLLECTIONS_SERVICE_ACCOUNTS_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_04_DCBS_REPORTS_MTN_ADJUSTMENT_VW_yest = BashOperator(
     task_id='extract_04_DCBS_REPORTS_MTN_ADJUSTMENT_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_04_DCBS_REPORTS_MTN_ADJUSTMENT_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_16_DCBS_REPORTS_MTN_FIN_REVERSAL_RECEIPT_VW_yest = BashOperator(
     task_id='extract_16_DCBS_REPORTS_MTN_FIN_REVERSAL_RECEIPT_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_16_DCBS_REPORTS_MTN_FIN_REVERSAL_RECEIPT_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_07_DCBS_REPORTS_MTN_ACCOUNT_OPEN_TRANS_VW_yest = BashOperator(
     task_id='extract_07_DCBS_REPORTS_MTN_ACCOUNT_OPEN_TRANS_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_07_DCBS_REPORTS_MTN_ACCOUNT_OPEN_TRANS_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_08_DCBS_REPORTS_MTN_INV_SPECIFIC_PAYMENTS_VW_yest = BashOperator(
     task_id='extract_08_DCBS_REPORTS_MTN_INV_SPECIFIC_PAYMENTS_VW_yest' ,
     bash_command='bash /nas/share05/tools/FeedsExtractor/Scripts/Airflow_DAG/DCBS_Feeds/extract_08_DCBS_REPORTS_MTN_INV_SPECIFIC_PAYMENTS_VW_yest.sh ',
     dag=dag,
     run_as_user = 'daasuser'
)

extract_09_DCBS_COLLECTIONS_COL_CUSTOMER_TRANSACTION_yest >> extract_10_DCBS_COLLECTIONS_COL_LEDGER_TRANSACTIONS_yest >>  extract_11_DCBS_COLLECTIONS_COL_ADJUSTED_DETAILS_yest >> extract_12_DCBS_COLLECTIONS_COL_TRANSACTION_CORRELATION_yest >> extract_05_DCBS_REPORTS_MTN_EXCESS_PAYMENT_TRANSFER_VW_yest >> extract_06_DCBS_REPORTS_MTN_EXCESS_PAYMENT_VW_yest >> extract_DCBS_REPORTS_MTN_SERV_BALANCE_VW_yest >> extract_DCBS_REPORTS_MTN_SERV_ALL_PAYMENT_VW_yest >> extract_13_DCBS_COLLECTIONS_PROFILES_yest >> extract_14_DCBS_COLLECTIONS_BILLING_ACCOUNTS_yest >> extract_15_DCBS_COLLECTIONS_SERVICE_ACCOUNTS_yest >> extract_16_DCBS_REPORTS_MTN_FIN_REVERSAL_RECEIPT_VW_yest >> extract_04_DCBS_REPORTS_MTN_ADJUSTMENT_VW_yest >> extract_07_DCBS_REPORTS_MTN_ACCOUNT_OPEN_TRANS_VW_yest >> extract_08_DCBS_REPORTS_MTN_INV_SPECIFIC_PAYMENTS_VW_yest
