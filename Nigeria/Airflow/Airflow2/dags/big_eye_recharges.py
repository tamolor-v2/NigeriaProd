from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from runPresto import presto
import logging

prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'

def readQ(path):
    file = open(path,mode='r')
    readQuery = file.read()
    return readQuery.format(yyyymmdd = datetime.now().strftime('%Y%m%d'))

rchhQuery = 'select 1'
rchdQuery = 'select 1'
rchdCubeQuery = 'select 1'
rchhCubeQuery = 'select 1'
rechargeCheckCheckQuery = 'select 1'

oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)


def getSubDag_BigEyeRecharges(parent_dag_name, child_dag_name, args):
    global oPresto
    global rchhQuery
    global rchdQuery
    global rchdCubeQuery
    global rchhCubeQuery
    global rechargeCheckCheckQuery
    escDateKey=True
    dag = DAG("{0}.{1}".format(parent_dag_name,child_dag_name), default_args=args, catchup=False, max_active_runs=1)
    rchd = PythonOperator(task_id='rchd', python_callable=oPresto.executeTransaction,op_args=[rchdQuery], dag=dag, run_as_user = 'daasuser')
    rchh = PythonOperator(task_id='rchh', python_callable=oPresto.executeTransaction,op_args=[rchhQuery], dag=dag, run_as_user = 'daasuser')
    rchdCube = PythonOperator(task_id='rchdCube', python_callable=oPresto.executeTransaction,op_args=[rchdCubeQuery], dag=dag, run_as_user = 'daasuser')
    rchhCube = PythonOperator(task_id='rchhCube', python_callable=oPresto.executeTransaction,op_args=[rchhCubeQuery], dag=dag, run_as_user = 'daasuser')
    rechargeCheck = PythonOperator(task_id='rechargeCheck',python_callable=oPresto.executeQuery,op_args=[rechargeCheckCheckQuery,escDateKey],dag=dag,run_as_user = 'daasuser')
    rchd >> rchh
    rchh >> rchdCube
    rchh >> rchhCube
    [rchdCube,rchhCube] >> rechargeCheck
    return dag

