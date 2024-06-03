from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime,timedelta
import sys


date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def getSubDag_mon5b5(parent_dag_name, child_dag_name, args):
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)


    mon= BashOperator(task_id='mon',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_mon monthly 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')

    validation_mon= BashOperator(task_id='validation_mon',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_validation_mon daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')

     
    mon >> validation_mon
     
    return dag



