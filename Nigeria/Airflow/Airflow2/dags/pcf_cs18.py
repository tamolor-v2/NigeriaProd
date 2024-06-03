from __future__ import print_function

import time
from builtins import range
from pprint import pprint
from datetime import datetime, timedelta


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
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

date_param = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
date_param_d1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')




dag = DAG(
    dag_id='CS18_PCF',
    default_args=args,
    schedule_interval='0 7 * * *',
    description='cs18 D-2 pcf run',
    catchup=False,
    concurrency=10,
    max_active_runs=10
)

CCN_CDR_GPRS = BashOperator(
     task_id='CCN_CDR_GPRS' ,
     bash_command='java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -Dlog4j2.formatMsgNoLookups=true -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f CCN_CDR_GPRS --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName 10.1.197.146  2>&1 | tee /nas/share05/tools/Reprocessing/logs/FromConsole/PCF_$(date +%Y%m%d_%s).txt  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

CCN_CDR_SMS = BashOperator(
     task_id='CCN_CDR_SMS' ,
     bash_command='java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -Dlog4j2.formatMsgNoLookups=true -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f CCN_CDR_SMS --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName 10.1.197.146 2>&1 | tee /nas/share05/tools/Reprocessing/logs/FromConsole/PCF_$(date +%Y%m%d_%s).txt  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


CCN_CDR_VOICE = BashOperator(
     task_id='CCN_CDR_VOICE' ,
     bash_command='java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -Dlog4j2.formatMsgNoLookups=true -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f CCN_CDR_VOICE --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName 10.1.197.146 2>&1 | tee /nas/share05/tools/Reprocessing/logs/FromConsole/PCF_$(date +%Y%m%d_%s).txt  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


CS6_AIR_CDR = BashOperator(
     task_id='CS6_AIR_CDR' ,
     bash_command='java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -Dlog4j2.formatMsgNoLookups=true -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f CS6_AIR_CDR --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName 10.1.197.146 2>&1 | tee /nas/share05/tools/Reprocessing/logs/FromConsole/PCF_$(date +%Y%m%d_%s).txt  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)

CS6_SDP_CDR = BashOperator(
     task_id='CS6_SDP_CDR' ,
     bash_command='java -Dlog4j.configurationFile=/nas/share05/tools/Reprocessing/conf/log4j2.xml -Dlog4j2.formatMsgNoLookups=true -cp /nas/share05/tools/Reprocessing/lib/ReprocessingTool-1.0.7.jar:/nas/share05/tools/Reprocessing/lib/FileOps_2.11-0.1-SNAPSHOT_hdfs.jar:/usr/hdp/current/hadoop-client/conf com.ligadata.reprocessing.App --cfgPath /nas/share05/tools/Reprocessing/conf/config_new_presto.json -mfq /nas/share05/tools/Reprocessing/conf/MissingFilesDetectionQuery.sql -wd /tmp/ -sd {0} -ed {0} -f CS6_SDP_CDR --logQueries --ignoreDotFilesCheck -p 2 --move --jdbcName 10.1.197.146  2>&1 | tee /nas/share05/tools/Reprocessing/logs/FromConsole/PCF_$(date +%Y%m%d_%s).txt  '.format(date_param_d1),
     dag=dag,
     run_as_user = 'daasuser'
)


CS6_SDP_CDR >> CS6_AIR_CDR >> CCN_CDR_VOICE >> CCN_CDR_SMS >> CCN_CDR_GPRS

