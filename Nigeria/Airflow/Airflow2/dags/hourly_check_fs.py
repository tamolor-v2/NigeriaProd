import airflow
import logging
from airflow.models import Variable
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from runPresto import presto

default_args = {
    'owner': 'daasuser',
    'depends_on_past': False,
    'start_date': datetime(2019,12,8),
    'email': ['y.bloukh@ligadata.com','support@ligadata.com','k.musallam@ligadata.com','t.olorunfemi@ligadata.com','bmustafa@ligadata.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('Query_Filestats', default_args=default_args, catchup=False,schedule_interval='15 * * * *')


query_filestats = "select b.feedname from dim.dim_feed_catalogue a inner join (select distinct(split(fsys_msgtype,'.')[4]) feedname from flare_8.file_stats where tbl_dt={dt} and from_unixtime(endtimets/1000) >= date_parse('{hour_minus_one}','%Y-%m-%d %H:%i:%S.%f')) b on (a.feed_name = b.feedname)".format(dt=datetime.now().strftime("%Y%m%d"), hour_minus_one=datetime.now()-timedelta(hours=1))

#Start Test part for generation

prestoHost='10.1.197.145'
prestoPort='8999'
prestoCatalog='hive5'
prestojarPath='/nas/share05/tools/TransactionsTool/presto-jdbc-350.jar'

oPresto=presto(host=prestoHost,port=prestoPort,catalog=prestoCatalog,username='daasuser',jarPath=prestojarPath)
a = Variable.get("vr_flag",deserialize_json=False)
print(a)
def excQ(p,q):
    if Variable.get("vr_flag",deserialize_json=False) == "1":
        return -1
    else: 
        print(q)
        res = p.executeQuery(q,True)
        return res


def parseQ(oPresto,query_filestats):
    q_output = []
    outpt = excQ(oPresto,query_filestats)
    if outpt == -1:
        return -1
    else:
        for i in outpt:
            q_output.append(i[0])
        Variable.set("vr_flag","1")
        Variable.set("feedsList_VR",q_output)
        return 0

getFeeds = PythonOperator(
    task_id = 'getFeeds',
    depends_on_past=False,
    python_callable=parseQ,
    op_args=[oPresto,query_filestats],
    dag=dag,
    run_as_user = 'daasuser'
    )


