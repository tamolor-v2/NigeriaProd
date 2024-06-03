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
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False,
}

dag = DAG(
    dag_id='MOVE_TO_ECS_GROUP_3',
    default_args=args,
    schedule_interval='0 8 * * *',
    catchup=False,  
    concurrency=5,
    max_active_runs=5

)

START = BashOperator(
     task_id='start' ,
     bash_command='echo "start"	',
     run_as_user = 'daasuser',
     dag=dag,
)

IFSAPP_BUDGET_COMM_COST_VAR =BashOperator(
     task_id='IFSAPP_BUDGET_COMM_COST_VAR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_BUDGET_COMM_COST_VAR	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_DETAILS =BashOperator(
     task_id='CUSTOMER_DETAILS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_DETAILS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DBA_ROLE_PRIVS =BashOperator(
     task_id='DBA_ROLE_PRIVS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DBA_ROLE_PRIVS	',
     run_as_user = 'daasuser',
     dag=dag,
)
IDENTITY_INVOICE_INFO =BashOperator(
     task_id='IDENTITY_INVOICE_INFO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IDENTITY_INVOICE_INFO	',
     run_as_user = 'daasuser',
     dag=dag,
)
DYA_DAILY_ACTIVATION =BashOperator(
     task_id='DYA_DAILY_ACTIVATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DYA_DAILY_ACTIVATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFS_SERVICE_CENTRE_SALES =BashOperator(
     task_id='IFS_SERVICE_CENTRE_SALES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFS_SERVICE_CENTRE_SALES	',
     run_as_user = 'daasuser',
     dag=dag,
)
STATE =BashOperator(
     task_id='STATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh STATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
VALID_USER =BashOperator(
     task_id='VALID_USER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh VALID_USER	',
     run_as_user = 'daasuser',
     dag=dag,
)
BYPASS_VENDOR =BashOperator(
     task_id='BYPASS_VENDOR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BYPASS_VENDOR	',
     run_as_user = 'daasuser',
     dag=dag,
)
OUTWARD =BashOperator(
     task_id='OUTWARD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh OUTWARD	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_PRIMARY_SALE =BashOperator(
     task_id='TAS_PRIMARY_SALE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAS_PRIMARY_SALE	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_ACCOUNT =BashOperator(
     task_id='IFSAPP_ACCOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_ACCOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_BIB_PAYMENT_REVERSAL_VW =BashOperator(
     task_id='MSO_BIB_PAYMENT_REVERSAL_VW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSO_BIB_PAYMENT_REVERSAL_VW	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_CREDIT_INFO =BashOperator(
     task_id='CUSTOMER_CREDIT_INFO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_CREDIT_INFO	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_PLAN_AUTH =BashOperator(
     task_id='PAYMENT_PLAN_AUTH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_PLAN_AUTH	',
     run_as_user = 'daasuser',
     dag=dag,
)
PRICE_LIST =BashOperator(
     task_id='PRICE_LIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PRICE_LIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_SWAP =BashOperator(
     task_id='SIM_SWAP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIM_SWAP	',
     run_as_user = 'daasuser',
     dag=dag,
)
INACTIVE_DEVICES =BashOperator(
     task_id='INACTIVE_DEVICES' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INACTIVE_DEVICES	',
     run_as_user = 'daasuser',
     dag=dag,
)
CODE_F =BashOperator(
     task_id='CODE_F' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CODE_F	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_INFO_COMM_METHOD =BashOperator(
     task_id='CUSTOMER_INFO_COMM_METHOD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_INFO_COMM_METHOD	',
     run_as_user = 'daasuser',
     dag=dag,
)
ENROLLMENT_REF =BashOperator(
     task_id='ENROLLMENT_REF' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ENROLLMENT_REF	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_CONNECT_USER_DETAILS =BashOperator(
     task_id='MTN_CONNECT_USER_DETAILS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTN_CONNECT_USER_DETAILS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_PRODUCT_MASTER =BashOperator(
     task_id='TAS_PRODUCT_MASTER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAS_PRODUCT_MASTER	',
     run_as_user = 'daasuser',
     dag=dag,
)
KYC_DEALER =BashOperator(
     task_id='KYC_DEALER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh KYC_DEALER	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_KPI_TARGET_VS_ACHIEVEMENT =BashOperator(
     task_id='TAS_KPI_TARGET_VS_ACHIEVEMENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAS_KPI_TARGET_VS_ACHIEVEMENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CODE_G =BashOperator(
     task_id='CODE_G' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CODE_G	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_GATEWAY_AIRTIME =BashOperator(
     task_id='PAYMENT_GATEWAY_AIRTIME' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_GATEWAY_AIRTIME	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_GATEWAY =BashOperator(
     task_id='PAYMENT_GATEWAY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_GATEWAY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_CUST_AVAILABLE_CREDIT =BashOperator(
     task_id='MTN_CUST_AVAILABLE_CREDIT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTN_CUST_AVAILABLE_CREDIT	',
     run_as_user = 'daasuser',
     dag=dag,
)
INWARD =BashOperator(
     task_id='INWARD' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INWARD	',
     run_as_user = 'daasuser',
     dag=dag,
)
CLM_WBO =BashOperator(
     task_id='CLM_WBO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CLM_WBO	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_BUDGET_YEAR_AMOUNT_UNION =BashOperator(
     task_id='IFSAPP_BUDGET_YEAR_AMOUNT_UNION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_BUDGET_YEAR_AMOUNT_UNION	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAS_CLOSING_STOCK_BALANCE =BashOperator(
     task_id='TAS_CLOSING_STOCK_BALANCE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAS_CLOSING_STOCK_BALANCE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MNP_LOCATION =BashOperator(
     task_id='MNP_LOCATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MNP_LOCATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
RETURN_MATERIAL_LINE =BashOperator(
     task_id='RETURN_MATERIAL_LINE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RETURN_MATERIAL_LINE	',
     run_as_user = 'daasuser',
     dag=dag,
)
QMATIC =BashOperator(
     task_id='QMATIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh QMATIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
PBM_PROBLEM_INVESTIGATION =BashOperator(
     task_id='PBM_PROBLEM_INVESTIGATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PBM_PROBLEM_INVESTIGATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
VAT_PERC =BashOperator(
     task_id='VAT_PERC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh VAT_PERC	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUST_ORD_CUSTOMER_ENT =BashOperator(
     task_id='CUST_ORD_CUSTOMER_ENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUST_ORD_CUSTOMER_ENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SUPPLIER_INFO =BashOperator(
     task_id='SUPPLIER_INFO' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SUPPLIER_INFO	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_TERM =BashOperator(
     task_id='PAYMENT_TERM' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_TERM	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_GROUP =BashOperator(
     task_id='CUSTOMER_GROUP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_GROUP	',
     run_as_user = 'daasuser',
     dag=dag,
)
FND_USER =BashOperator(
     task_id='FND_USER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FND_USER	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYMENT_ADDRESS_GENERAL =BashOperator(
     task_id='PAYMENT_ADDRESS_GENERAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYMENT_ADDRESS_GENERAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
SALES_PRICE_LIST_PART =BashOperator(
     task_id='SALES_PRICE_LIST_PART' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SALES_PRICE_LIST_PART	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_RETAIL_OUTLETS =BashOperator(
     task_id='CB_RETAIL_OUTLETS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_RETAIL_OUTLETS	',
     run_as_user = 'daasuser',
     dag=dag,
)
CHANGE_REPORT_MTN =BashOperator(
     task_id='CHANGE_REPORT_MTN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CHANGE_REPORT_MTN	',
     run_as_user = 'daasuser',
     dag=dag,
)
MTN_LOCATION_REGION =BashOperator(
     task_id='MTN_LOCATION_REGION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MTN_LOCATION_REGION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MASTER_DATA_VIEW =BashOperator(
     task_id='MASTER_DATA_VIEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MASTER_DATA_VIEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
DEALER_TYPE =BashOperator(
     task_id='DEALER_TYPE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DEALER_TYPE	',
     run_as_user = 'daasuser',
     dag=dag,
)
CORPORATE_FORM =BashOperator(
     task_id='CORPORATE_FORM' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CORPORATE_FORM	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_SIMSWAP_METRIC =BashOperator(
     task_id='ESM_SIMSWAP_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_SIMSWAP_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
RETAIL_SHOP =BashOperator(
     task_id='RETAIL_SHOP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RETAIL_SHOP	',
     run_as_user = 'daasuser',
     dag=dag,
)
VTU_KPI_METRICS =BashOperator(
     task_id='VTU_KPI_METRICS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh VTU_KPI_METRICS	',
     run_as_user = 'daasuser',
     dag=dag,
)
SPON_PROVIDER_DTLS =BashOperator(
     task_id='SPON_PROVIDER_DTLS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SPON_PROVIDER_DTLS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_PGW_DAILY =BashOperator(
     task_id='MAPS_CORE_PGW_DAILY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_PGW_DAILY	',
     run_as_user = 'daasuser',
     dag=dag,
)
PURCHASE_ORDER =BashOperator(
     task_id='PURCHASE_ORDER' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PURCHASE_ORDER	',
     run_as_user = 'daasuser',
     dag=dag,
)
SUPPLIER_INFO_ADDRESS =BashOperator(
     task_id='SUPPLIER_INFO_ADDRESS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SUPPLIER_INFO_ADDRESS	',
     run_as_user = 'daasuser',
     dag=dag,
)
CUSTOMER_INFO_TAB =BashOperator(
     task_id='CUSTOMER_INFO_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CUSTOMER_INFO_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_CUSTOMER_INFO_TAB =BashOperator(
     task_id='IFSAPP_CUSTOMER_INFO_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_CUSTOMER_INFO_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_CITY_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_CITY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_CITY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
CC_ONLINE_ACTIVITY =BashOperator(
     task_id='CC_ONLINE_ACTIVITY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CC_ONLINE_ACTIVITY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CC_SURVEY =BashOperator(
     task_id='CC_SURVEY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CC_SURVEY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CC_AGENT_ACTIVITY =BashOperator(
     task_id='CC_AGENT_ACTIVITY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CC_AGENT_ACTIVITY	',
     run_as_user = 'daasuser',
     dag=dag,
)
CANVASA =BashOperator(
     task_id='CANVASA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CANVASA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_PGW_WEEKLY =BashOperator(
     task_id='MAPS_CORE_PGW_WEEKLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_PGW_WEEKLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_W_BH =BashOperator(
     task_id='MAPS_3G_CN_W_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CN_W_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HSS_W =BashOperator(
     task_id='MAPS_CORE_HSS_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_HSS_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_BOARD_W =BashOperator(
     task_id='MAPS_4G_BOARD_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_BOARD_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MGW_W =BashOperator(
     task_id='MAPS_CORE_MGW_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MGW_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HLR_W =BashOperator(
     task_id='MAPS_CORE_HLR_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_HLR_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MSC_W =BashOperator(
     task_id='MAPS_CORE_MSC_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MSC_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_W =BashOperator(
     task_id='MAPS_3G_CN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
GUEST_PASS =BashOperator(
     task_id='GUEST_PASS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh GUEST_PASS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_W_BH =BashOperator(
     task_id='MAPS_2G_CN_W_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CN_W_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_ROUTE_W =BashOperator(
     task_id='MAPS_CORE_ROUTE_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_ROUTE_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MME_W =BashOperator(
     task_id='MAPS_CORE_MME_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MME_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_W_BH =BashOperator(
     task_id='MAPS_2G_CELL_W_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CELL_W_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_CN_W =BashOperator(
     task_id='MAPS_CORE_CN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_CN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_SWAP_SUCCESS_COUNT =BashOperator(
     task_id='SIM_SWAP_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIM_SWAP_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIM_REG_SUCCESS_COUNT =BashOperator(
     task_id='SIM_REG_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIM_REG_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIB_CREDIT_CONTROL_REPORT =BashOperator(
     task_id='BIB_CREDIT_CONTROL_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BIB_CREDIT_CONTROL_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
BULK_SMS =BashOperator(
     task_id='BULK_SMS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BULK_SMS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_COUNTRY_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_COUNTRY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_COUNTRY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_COUNTRY_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_COUNTRY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_COUNTRY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_LGA_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_LGA_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_LGA_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_LICENSE =BashOperator(
     task_id='SMSC_LICENSE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMSC_LICENSE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_TERRITORY_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_TERRITORY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_TERRITORY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_STATE_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_STATE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_STATE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_STATE_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_STATE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_STATE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_CLUSTER_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_CLUSTER_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_CLUSTER_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_CLUSTER_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_CLUSTER_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_CLUSTER_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_TERRITORY_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_TERRITORY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_TERRITORY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_CITY_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_CITY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_CITY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE =BashOperator(
     task_id='SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
USSD_ERROR_CODE_BREAKDOWN =BashOperator(
     task_id='USSD_ERROR_CODE_BREAKDOWN' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh USSD_ERROR_CODE_BREAKDOWN	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_SITE_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_SITE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_SITE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_LGA_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_LGA_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_LGA_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_DAILY_REGION_AH_REPORT =BashOperator(
     task_id='MKT_DAILY_REGION_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_DAILY_REGION_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_WEEKLY_REGION_AH_REPORT =BashOperator(
     task_id='MKT_WEEKLY_REGION_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_WEEKLY_REGION_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
COMPENSATION =BashOperator(
     task_id='COMPENSATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh COMPENSATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
EMM_DELIVERY_KPI =BashOperator(
     task_id='EMM_DELIVERY_KPI' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh EMM_DELIVERY_KPI	',
     run_as_user = 'daasuser',
     dag=dag,
)
APILOG =BashOperator(
     task_id='APILOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh APILOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_TOTAL_SUBMIT_SUCCESS_RATE =BashOperator(
     task_id='SMSC_TOTAL_SUBMIT_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMSC_TOTAL_SUBMIT_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
USSD_TRAFFIC_SUCCESS_RATE =BashOperator(
     task_id='USSD_TRAFFIC_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh USSD_TRAFFIC_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
NG_USIM_STG =BashOperator(
     task_id='NG_USIM_STG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh NG_USIM_STG	',
     run_as_user = 'daasuser',
     dag=dag,
)
D_CONNECT_ACTIVATION_YYYYMM_LIVE =BashOperator(
     task_id='D_CONNECT_ACTIVATION_YYYYMM_LIVE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh D_CONNECT_ACTIVATION_YYYYMM_LIVE	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_SITE_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_SITE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_SITE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_REGION_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_REGION_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_REGION_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_LGA_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_LGA_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_LGA_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_COUNTRY_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_COUNTRY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_COUNTRY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_CLUSTER_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_CLUSTER_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_CLUSTER_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
WBS_REPORT =BashOperator(
     task_id='WBS_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh WBS_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_W =BashOperator(
     task_id='MAPS_4G_CELL_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_CELL_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_D =BashOperator(
     task_id='MAPS_4G_CELL_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_CELL_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MNP_PORTING_BROADCAST =BashOperator(
     task_id='MNP_PORTING_BROADCAST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MNP_PORTING_BROADCAST	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_GGSN_W =BashOperator(
     task_id='MAPS_CORE_GGSN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_GGSN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_W =BashOperator(
     task_id='MAPS_CORE_SGSN_W' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_SGSN_W	',
     run_as_user = 'daasuser',
     dag=dag,
)
DSA_SUCCESS_COUNT =BashOperator(
     task_id='DSA_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DSA_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_TERRITORY_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_TERRITORY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_TERRITORY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
TARIFF_TYPE =BashOperator(
     task_id='TARIFF_TYPE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TARIFF_TYPE	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMSC_ERROR_BREAKDOWN_PER_ACCOUNT =BashOperator(
     task_id='SMSC_ERROR_BREAKDOWN_PER_ACCOUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMSC_ERROR_BREAKDOWN_PER_ACCOUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MME_D =BashOperator(
     task_id='MAPS_CORE_MME_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MME_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MSC_D =BashOperator(
     task_id='MAPS_CORE_MSC_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MSC_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HSS_D =BashOperator(
     task_id='MAPS_CORE_HSS_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_HSS_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_GGSN_D =BashOperator(
     task_id='MAPS_CORE_GGSN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_GGSN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_D =BashOperator(
     task_id='MAPS_CORE_SGSN_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_SGSN_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MGW_D =BashOperator(
     task_id='MAPS_CORE_MGW_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MGW_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_ROUTE_D =BashOperator(
     task_id='MAPS_CORE_ROUTE_D' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_ROUTE_D	',
     run_as_user = 'daasuser',
     dag=dag,
)
PAYLOAD_DATA_AT_AREA_AND_BTS_ID =BashOperator(
     task_id='PAYLOAD_DATA_AT_AREA_AND_BTS_ID' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PAYLOAD_DATA_AT_AREA_AND_BTS_ID	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_CITY_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_CITY_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_CITY_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PROVISIONING_LOG =BashOperator(
     task_id='PROVISIONING_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PROVISIONING_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
DPI_CDR =BashOperator(
     task_id='DPI_CDR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DPI_CDR	',
     run_as_user = 'daasuser',
     dag=dag,
)
SHOP_LOCATOR =BashOperator(
     task_id='SHOP_LOCATOR' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SHOP_LOCATOR	',
     run_as_user = 'daasuser',
     dag=dag,
)
COSTS_FOR_PROFITABILITY =BashOperator(
     task_id='COSTS_FOR_PROFITABILITY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh COSTS_FOR_PROFITABILITY	',
     run_as_user = 'daasuser',
     dag=dag,
)
BILL_RUN_STATISTICS_TAB =BashOperator(
     task_id='BILL_RUN_STATISTICS_TAB' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BILL_RUN_STATISTICS_TAB	',
     run_as_user = 'daasuser',
     dag=dag,
)
MRKT_SIZING_NEXT_10_YRS =BashOperator(
     task_id='MRKT_SIZING_NEXT_10_YRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MRKT_SIZING_NEXT_10_YRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
COUNTRY_WIDE_PARAMETERS =BashOperator(
     task_id='COUNTRY_WIDE_PARAMETERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh COUNTRY_WIDE_PARAMETERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DIM_PACKAGE_NG =BashOperator(
     task_id='DIM_PACKAGE_NG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DIM_PACKAGE_NG	',
     run_as_user = 'daasuser',
     dag=dag,
)
ERM_10YRS =BashOperator(
     task_id='ERM_10YRS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ERM_10YRS	',
     run_as_user = 'daasuser',
     dag=dag,
)
TECHNOLOGY_CELL_SPECIFICATION =BashOperator(
     task_id='TECHNOLOGY_CELL_SPECIFICATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TECHNOLOGY_CELL_SPECIFICATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
AREA_WIDE_PARAMETERS =BashOperator(
     task_id='AREA_WIDE_PARAMETERS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AREA_WIDE_PARAMETERS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DATA_QUALITY_CHECK_LOGS =BashOperator(
     task_id='DATA_QUALITY_CHECK_LOGS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DATA_QUALITY_CHECK_LOGS	',
     run_as_user = 'daasuser',
     dag=dag,
)
RECHARGE_SUCCESS_RATE =BashOperator(
     task_id='RECHARGE_SUCCESS_RATE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RECHARGE_SUCCESS_RATE	',
     run_as_user = 'daasuser',
     dag=dag,
)
INTERNATIONAL_ICX =BashOperator(
     task_id='INTERNATIONAL_ICX' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INTERNATIONAL_ICX	',
     run_as_user = 'daasuser',
     dag=dag,
)
AA_AD_BONUS =BashOperator(
     task_id='AA_AD_BONUS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AA_AD_BONUS	',
     run_as_user = 'daasuser',
     dag=dag,
)
AA_AD_BUNDLE =BashOperator(
     task_id='AA_AD_BUNDLE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AA_AD_BUNDLE	',
     run_as_user = 'daasuser',
     dag=dag,
)
AA_TARIFF_DATA =BashOperator(
     task_id='AA_TARIFF_DATA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh AA_TARIFF_DATA	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_ACCURACY_STATISTICS =BashOperator(
     task_id='MSO_ACCURACY_STATISTICS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSO_ACCURACY_STATISTICS	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_ZONE_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_ZONE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_ZONE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_M_DATA_BH =BashOperator(
     task_id='MAPS_2G_CN_M_DATA_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CN_M_DATA_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MKT_MONTHLY_STATE_AH_REPORT =BashOperator(
     task_id='MKT_MONTHLY_STATE_AH_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MKT_MONTHLY_STATE_AH_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_3G_CN_M =BashOperator(
     task_id='MAPS_3G_CN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_3G_CN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
QERROR_NG =BashOperator(
     task_id='QERROR_NG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh QERROR_NG	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_DYA_METRIC =BashOperator(
     task_id='ESM_DYA_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_DYA_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
RBT_SUCCESS_COUNT =BashOperator(
     task_id='RBT_SUCCESS_COUNT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh RBT_SUCCESS_COUNT	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_GPRS_FINAL =BashOperator(
     task_id='TAPOUT_GPRS_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPOUT_GPRS_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
TAPOUT_VOICE_FINAL =BashOperator(
     task_id='TAPOUT_VOICE_FINAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TAPOUT_VOICE_FINAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
QRIOUS =BashOperator(
     task_id='QRIOUS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh QRIOUS	',
     run_as_user = 'daasuser',
     dag=dag,
)
DIRECT_CONNECT_CCTP =BashOperator(
     task_id='DIRECT_CONNECT_CCTP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DIRECT_CONNECT_CCTP	',
     run_as_user = 'daasuser',
     dag=dag,
)
ESM_VTU_VENDING_METRIC =BashOperator(
     task_id='ESM_VTU_VENDING_METRIC' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ESM_VTU_VENDING_METRIC	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HLR_M =BashOperator(
     task_id='MAPS_CORE_HLR_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_HLR_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CELL_MONTHLY_BH =BashOperator(
     task_id='MAPS_2G_CELL_MONTHLY_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CELL_MONTHLY_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_HSS_M =BashOperator(
     task_id='MAPS_CORE_HSS_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_HSS_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION =BashOperator(
     task_id='MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_ROUTE_M =BashOperator(
     task_id='MAPS_CORE_ROUTE_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_ROUTE_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_M =BashOperator(
     task_id='MAPS_4G_CELL_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_CELL_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_M_BH =BashOperator(
     task_id='MAPS_CORE_SGSN_M_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_SGSN_M_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_CELL_M_BH =BashOperator(
     task_id='MAPS_4G_CELL_M_BH' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_CELL_M_BH	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_2G_CN_M =BashOperator(
     task_id='MAPS_2G_CN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_2G_CN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_4G_BOARD_M =BashOperator(
     task_id='MAPS_4G_BOARD_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_4G_BOARD_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_PGW_MONTHLY =BashOperator(
     task_id='MAPS_CORE_PGW_MONTHLY' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_PGW_MONTHLY	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MME_M =BashOperator(
     task_id='MAPS_CORE_MME_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MME_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_CN_M =BashOperator(
     task_id='MAPS_CORE_CN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_CN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_SGSN_M =BashOperator(
     task_id='MAPS_CORE_SGSN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_SGSN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_GGSN_M =BashOperator(
     task_id='MAPS_CORE_GGSN_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_GGSN_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MSC_M =BashOperator(
     task_id='MAPS_CORE_MSC_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MSC_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
MAPS_CORE_MGW_M =BashOperator(
     task_id='MAPS_CORE_MGW_M' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MAPS_CORE_MGW_M	',
     run_as_user = 'daasuser',
     dag=dag,
)
SW_CAPEX_FIXED =BashOperator(
     task_id='SW_CAPEX_FIXED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SW_CAPEX_FIXED	',
     run_as_user = 'daasuser',
     dag=dag,
)
HW_CAPEX_FIXED =BashOperator(
     task_id='HW_CAPEX_FIXED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HW_CAPEX_FIXED	',
     run_as_user = 'daasuser',
     dag=dag,
)
PENETRATION_RATIO_FIXED =BashOperator(
     task_id='PENETRATION_RATIO_FIXED' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PENETRATION_RATIO_FIXED	',
     run_as_user = 'daasuser',
     dag=dag,
)
PSB_SYSTEM_ACCOUNT_BALANCES_REPORT =BashOperator(
     task_id='PSB_SYSTEM_ACCOUNT_BALANCES_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PSB_SYSTEM_ACCOUNT_BALANCES_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
PSB_USER_ACCOUNT_BALANCES_REPORT =BashOperator(
     task_id='PSB_USER_ACCOUNT_BALANCES_REPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PSB_USER_ACCOUNT_BALANCES_REPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_IFSCONNECT_SALES_HIST =BashOperator(
     task_id='IFSAPP_IFSCONNECT_SALES_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_IFSCONNECT_SALES_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
IFSAPP_SERVICE_CONNECT_STORE =BashOperator(
     task_id='IFSAPP_SERVICE_CONNECT_STORE' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh IFSAPP_SERVICE_CONNECT_STORE	',
     run_as_user = 'daasuser',
     dag=dag,
)
ISP_LOGIN_IDS_LOOKUP =BashOperator(
     task_id='ISP_LOGIN_IDS_LOOKUP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh ISP_LOGIN_IDS_LOOKUP	',
     run_as_user = 'daasuser',
     dag=dag,
)
COMPENSATION_MON =BashOperator(
     task_id='COMPENSATION_MON' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh COMPENSATION_MON	',
     run_as_user = 'daasuser',
     dag=dag,
)
DAILY_COMEBACK_RPT =BashOperator(
     task_id='DAILY_COMEBACK_RPT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh DAILY_COMEBACK_RPT	',
     run_as_user = 'daasuser',
     dag=dag,
)
INVENTORY_TRANSACTION_HIST =BashOperator(
     task_id='INVENTORY_TRANSACTION_HIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh INVENTORY_TRANSACTION_HIST	',
     run_as_user = 'daasuser',
     dag=dag,
)
PSB_REGISTRATIONS =BashOperator(
     task_id='PSB_REGISTRATIONS' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh PSB_REGISTRATIONS	',
     run_as_user = 'daasuser',
     dag=dag,
)
FIN_LOG =BashOperator(
     task_id='FIN_LOG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh FIN_LOG	',
     run_as_user = 'daasuser',
     dag=dag,
)
LATEARRIVALREPORT =BashOperator(
     task_id='LATEARRIVALREPORT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh LATEARRIVALREPORT	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_OPTOUT_AUTORENEWAL =BashOperator(
     task_id='HSDP_OPTOUT_AUTORENEWAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HSDP_OPTOUT_AUTORENEWAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
HSDP_OPTIN_AUTORENEWAL =BashOperator(
     task_id='HSDP_OPTIN_AUTORENEWAL' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh HSDP_OPTIN_AUTORENEWAL	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIBDATA_FILTER_CS5_CCN_GPRS_MA =BashOperator(
     task_id='BIBDATA_FILTER_CS5_CCN_GPRS_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BIBDATA_FILTER_CS5_CCN_GPRS_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA =BashOperator(
     task_id='BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
BIBDATA_FILTER_CS5_CCN_VOICE_MA =BashOperator(
     task_id='BIBDATA_FILTER_CS5_CCN_VOICE_MA' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh BIBDATA_FILTER_CS5_CCN_VOICE_MA	',
     run_as_user = 'daasuser',
     dag=dag,
)
SMF_DEVICE_MAP =BashOperator(
     task_id='SMF_DEVICE_MAP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SMF_DEVICE_MAP	',
     run_as_user = 'daasuser',
     dag=dag,
)
CB_SCHEDULES_LIVE_NEW =BashOperator(
     task_id='CB_SCHEDULES_LIVE_NEW' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh CB_SCHEDULES_LIVE_NEW	',
     run_as_user = 'daasuser',
     dag=dag,
)
SDP_API =BashOperator(
     task_id='SDP_API' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SDP_API	',
     run_as_user = 'daasuser',
     dag=dag,
)
SITE_MAPPING =BashOperator(
     task_id='SITE_MAPPING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SITE_MAPPING	',
     run_as_user = 'daasuser',
     dag=dag,
)
OFFER_PLAN_INFORMATION =BashOperator(
     task_id='OFFER_PLAN_INFORMATION' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh OFFER_PLAN_INFORMATION	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSO_REVERSED_BILLING =BashOperator(
     task_id='MSO_REVERSED_BILLING' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSO_REVERSED_BILLING	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIT_VAS_EVENT =BashOperator(
     task_id='SIT_VAS_EVENT' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIT_VAS_EVENT	',
     run_as_user = 'daasuser',
     dag=dag,
)
SIT_USG_VOICE_OG =BashOperator(
     task_id='SIT_USG_VOICE_OG' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SIT_USG_VOICE_OG	',
     run_as_user = 'daasuser',
     dag=dag,
)
MSC_CDR_TEMP =BashOperator(
     task_id='MSC_CDR_TEMP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh MSC_CDR_TEMP	',
     run_as_user = 'daasuser',
     dag=dag,
)
TT_MSO_1_ISP_LOGIN_IDS_LOOKUP =BashOperator(
     task_id='TT_MSO_1_ISP_LOGIN_IDS_LOOKUP' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh TT_MSO_1_ISP_LOGIN_IDS_LOOKUP	',
     run_as_user = 'daasuser',
     dag=dag,
)
SERVICE_LIST =BashOperator(
     task_id='SERVICE_LIST' ,
     bash_command='bash /nas/share05/ops/mtnops/move_to_ecs.sh SERVICE_LIST	',
     run_as_user = 'daasuser',
     dag=dag,
)

END = BashOperator(
     task_id='end' ,
     bash_command='echo "end"	',
     run_as_user = 'daasuser',
     dag=dag,
)


START >> [IFSAPP_BUDGET_COMM_COST_VAR, CUSTOMER_DETAILS, DBA_ROLE_PRIVS, IDENTITY_INVOICE_INFO, DYA_DAILY_ACTIVATION, IFS_SERVICE_CENTRE_SALES, STATE, VALID_USER, BYPASS_VENDOR, OUTWARD, TAS_PRIMARY_SALE, IFSAPP_ACCOUNT, MSO_BIB_PAYMENT_REVERSAL_VW, CUSTOMER_CREDIT_INFO, PAYMENT_PLAN_AUTH, PRICE_LIST, SIM_SWAP, INACTIVE_DEVICES, CODE_F, CUSTOMER_INFO_COMM_METHOD, ENROLLMENT_REF, MTN_CONNECT_USER_DETAILS, TAS_PRODUCT_MASTER, KYC_DEALER, TAS_KPI_TARGET_VS_ACHIEVEMENT, CODE_G, PAYMENT_GATEWAY_AIRTIME, PAYMENT_GATEWAY, MTN_CUST_AVAILABLE_CREDIT, INWARD, CLM_WBO, IFSAPP_BUDGET_YEAR_AMOUNT_UNION, TAS_CLOSING_STOCK_BALANCE, MNP_LOCATION, RETURN_MATERIAL_LINE, QMATIC, PBM_PROBLEM_INVESTIGATION, VAT_PERC, CUST_ORD_CUSTOMER_ENT, SUPPLIER_INFO, PAYMENT_TERM, CUSTOMER_GROUP, FND_USER, PAYMENT_ADDRESS_GENERAL, SALES_PRICE_LIST_PART, CB_RETAIL_OUTLETS, CHANGE_REPORT_MTN, MTN_LOCATION_REGION, MASTER_DATA_VIEW, DEALER_TYPE, CORPORATE_FORM, ESM_SIMSWAP_METRIC, RETAIL_SHOP, VTU_KPI_METRICS, SPON_PROVIDER_DTLS, MAPS_CORE_PGW_DAILY, PURCHASE_ORDER, SUPPLIER_INFO_ADDRESS, CUSTOMER_INFO_TAB, IFSAPP_CUSTOMER_INFO_TAB, MKT_DAILY_CITY_AH_REPORT, CC_ONLINE_ACTIVITY, CC_SURVEY, CC_AGENT_ACTIVITY, CANVASA, MAPS_CORE_PGW_WEEKLY, MAPS_3G_CN_W_BH, MAPS_CORE_HSS_W, MAPS_4G_BOARD_W, MAPS_CORE_MGW_W, MAPS_CORE_HLR_W, MAPS_CORE_MSC_W, MAPS_3G_CN_W, GUEST_PASS, MAPS_2G_CN_W_BH, MAPS_CORE_ROUTE_W, MAPS_CORE_MME_W, MAPS_2G_CELL_W_BH, MAPS_CORE_CN_W, SIM_SWAP_SUCCESS_COUNT, SIM_REG_SUCCESS_COUNT, BIB_CREDIT_CONTROL_REPORT, BULK_SMS, MKT_DAILY_COUNTRY_AH_REPORT, MKT_WEEKLY_COUNTRY_AH_REPORT, MKT_WEEKLY_LGA_AH_REPORT, SMSC_LICENSE, MKT_WEEKLY_TERRITORY_AH_REPORT, MKT_DAILY_STATE_AH_REPORT, MKT_WEEKLY_STATE_AH_REPORT, MKT_WEEKLY_CLUSTER_AH_REPORT, MKT_DAILY_CLUSTER_AH_REPORT, MKT_DAILY_TERRITORY_AH_REPORT, MKT_WEEKLY_CITY_AH_REPORT, SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE, USSD_ERROR_CODE_BREAKDOWN, MKT_WEEKLY_SITE_AH_REPORT, MKT_DAILY_LGA_AH_REPORT, MKT_DAILY_REGION_AH_REPORT, MKT_WEEKLY_REGION_AH_REPORT, COMPENSATION, EMM_DELIVERY_KPI, APILOG, SMSC_TOTAL_SUBMIT_SUCCESS_RATE, USSD_TRAFFIC_SUCCESS_RATE, NG_USIM_STG, D_CONNECT_ACTIVATION_YYYYMM_LIVE, MKT_MONTHLY_SITE_AH_REPORT, MKT_MONTHLY_REGION_AH_REPORT, MKT_MONTHLY_LGA_AH_REPORT, MKT_MONTHLY_COUNTRY_AH_REPORT, MKT_MONTHLY_CLUSTER_AH_REPORT, WBS_REPORT, MAPS_4G_CELL_W, MAPS_4G_CELL_D, MNP_PORTING_BROADCAST, MAPS_CORE_GGSN_W, MAPS_CORE_SGSN_W, DSA_SUCCESS_COUNT, MKT_MONTHLY_TERRITORY_AH_REPORT, TARIFF_TYPE, SMSC_ERROR_BREAKDOWN_PER_ACCOUNT, MAPS_CORE_MME_D, MAPS_CORE_MSC_D, MAPS_CORE_HSS_D, MAPS_CORE_GGSN_D, MAPS_CORE_SGSN_D, MAPS_CORE_MGW_D, MAPS_CORE_ROUTE_D, PAYLOAD_DATA_AT_AREA_AND_BTS_ID, MKT_MONTHLY_CITY_AH_REPORT, PROVISIONING_LOG, DPI_CDR, SHOP_LOCATOR, COSTS_FOR_PROFITABILITY, BILL_RUN_STATISTICS_TAB, MRKT_SIZING_NEXT_10_YRS, COUNTRY_WIDE_PARAMETERS, DIM_PACKAGE_NG, ERM_10YRS, TECHNOLOGY_CELL_SPECIFICATION, AREA_WIDE_PARAMETERS, DATA_QUALITY_CHECK_LOGS, RECHARGE_SUCCESS_RATE, INTERNATIONAL_ICX, AA_AD_BONUS, AA_AD_BUNDLE, AA_TARIFF_DATA, MSO_ACCURACY_STATISTICS, MKT_MONTHLY_ZONE_AH_REPORT, MAPS_2G_CN_M_DATA_BH, MKT_MONTHLY_STATE_AH_REPORT, MAPS_3G_CN_M, QERROR_NG, ESM_DYA_METRIC, RBT_SUCCESS_COUNT, TAPOUT_GPRS_FINAL, TAPOUT_VOICE_FINAL, QRIOUS, DIRECT_CONNECT_CCTP, ESM_VTU_VENDING_METRIC, MAPS_CORE_HLR_M, MAPS_2G_CELL_MONTHLY_BH, MAPS_CORE_HSS_M, MAPS_2G_COUNTRY_MONTHLY_BH_BTS_UTILIZATION, MAPS_CORE_ROUTE_M, MAPS_4G_CELL_M, MAPS_CORE_SGSN_M_BH, MAPS_4G_CELL_M_BH, MAPS_2G_CN_M, MAPS_4G_BOARD_M, MAPS_CORE_PGW_MONTHLY, MAPS_CORE_MME_M, MAPS_CORE_CN_M, MAPS_CORE_SGSN_M, MAPS_CORE_GGSN_M, MAPS_CORE_MSC_M, MAPS_CORE_MGW_M, SW_CAPEX_FIXED, HW_CAPEX_FIXED, PENETRATION_RATIO_FIXED, PSB_SYSTEM_ACCOUNT_BALANCES_REPORT, PSB_USER_ACCOUNT_BALANCES_REPORT, IFSAPP_IFSCONNECT_SALES_HIST, IFSAPP_SERVICE_CONNECT_STORE, ISP_LOGIN_IDS_LOOKUP, COMPENSATION_MON, DAILY_COMEBACK_RPT, INVENTORY_TRANSACTION_HIST, PSB_REGISTRATIONS, FIN_LOG, LATEARRIVALREPORT, HSDP_OPTOUT_AUTORENEWAL, HSDP_OPTIN_AUTORENEWAL, BIBDATA_FILTER_CS5_CCN_GPRS_MA, BIBDATA_FILTER_CS5_SDP_ACC_ADJ_MA, BIBDATA_FILTER_CS5_CCN_VOICE_MA, SMF_DEVICE_MAP, CB_SCHEDULES_LIVE_NEW, SDP_API, SITE_MAPPING, OFFER_PLAN_INFORMATION, MSO_REVERSED_BILLING, SIT_VAS_EVENT, SIT_USG_VOICE_OG, MSC_CDR_TEMP, TT_MSO_1_ISP_LOGIN_IDS_LOOKUP, SERVICE_LIST] >> END

#CCN_CDR_Detail, LEA_MAPPING_MSC_DAAS 

