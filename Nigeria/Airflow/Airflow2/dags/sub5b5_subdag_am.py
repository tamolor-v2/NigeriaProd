from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime,timedelta
import sys


date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')


def getSubDag_sub5b5_am(parent_dag_name, child_dag_name, args):
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)


    subbase = BashOperator(task_id='subbase',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subbase daily 8668'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')

    subgeo = BashOperator(task_id='subgeo',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subgeo daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subusim = BashOperator(task_id='subusim',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subusim daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subrevrank = BashOperator(task_id='subrevrank',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subrevrank daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subrev = BashOperator(task_id='subrev',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subrev daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subusg = BashOperator(task_id='subusg',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subusg daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subsmartdev = BashOperator(task_id='subsmartdev',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subsmartdev daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subsmartdevnew = BashOperator(task_id='subsmartdevnew',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subsmartdevnew daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
    subfinal = BashOperator(task_id='subfinal',bash_command='/nas/share05/scripts/segment5b5/run_this.sh {0} {0} segment5b5_subfinal daily 9999'.format(date_param_d1) ,      dag=dag,
     run_as_user = 'daasuser')
     
     
    dropsubbase = BashOperator(task_id='dropsubbase',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp99"' ,      dag=dag,
     run_as_user = 'daasuser')

    dropsubgeo = BashOperator(task_id='dropsubgeo',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp98"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubusim = BashOperator(task_id='dropsubusim',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp97"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubrevrank = BashOperator(task_id='dropsubrevrank',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp96"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubrev = BashOperator(task_id='dropsubrev',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp95"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubusg = BashOperator(task_id='dropsubusg',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp93"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubsmartdev = BashOperator(task_id='dropsubsmartdev',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp92"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubsmartdevnew = BashOperator(task_id='dropsubsmartdevnew',bash_command='bash /nas/share05/scripts/segment5b5/drop_this.sh "nigeria.segment5b5_sub_tmp91"' ,      dag=dag,
     run_as_user = 'daasuser')
     
    dropsubbase >> subbase >> dropsubgeo >> subgeo >> dropsubusim >> subusim >> dropsubrevrank >> subrevrank >> dropsubrev >> subrev >> dropsubusg  >> subusg >> dropsubsmartdev >> subsmartdev >> dropsubsmartdevnew >> subsmartdevnew >> subfinal
     
    return dag



