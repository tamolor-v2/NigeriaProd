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
    'depends_on_past':False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_failure': ['o.olanipekun@ligadata.com','support@ligadata.com'],
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}
 
date_param = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
date_param_1 = (datetime.now() - timedelta(days=1)).strftime('%Y%m')
 
 
dag = DAG(
    dag_id='SPONSORED_MAIL',
    default_args=args,
    schedule_interval='0 8 * * *',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

SPONSORED_MAIL = BashOperator(
     task_id='SPONSORED_MAIL' ,
     bash_command=' bash /nas/share05/dataOps_prod/sponsor_data/operamini.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/paycom_2.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/telvida.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/AYOBA.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/betpawa.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/rootbridge.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/itscope.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/mquid.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/alphagram.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/isignet.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/fuo.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/elizade.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/hilltoptv.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/randj.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/svgaming.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/edclearn.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/greenlotto.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/melbet.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/steadymart.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/mtnmarketing.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/pfizer.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/moyinnet.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/edusko.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/echub.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` && bash /nas/share05/dataOps_prod/sponsor_data/smooth.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m` ',
     dag=dag,
)

#&& bash /nas/share05/dataOps_prod/sponsor_data/sochitel.sh `date --date="-1 days" +%Y%m%d` `date --date="-1 days" +%Y%m`
SPONSORED_MAIL
