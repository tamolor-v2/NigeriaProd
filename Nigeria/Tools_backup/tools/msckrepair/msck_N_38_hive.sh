ts=$(date +"%Y%m%d_%H%M%S")
echo "running msck_repair at $ts">>/home/daasuser/last_msck_run.log
hive -e "set hive.msck.path.validation =ignore;msck repair table kamanja_test.rejecteddata"

hive -e "MSCK REPAIR TABLE FLARE_8.CS5_VTU_DUMP;
MSCK REPAIR TABLE FLARE_8.NGVS_CDR;
MSCK REPAIR TABLE FLARE_8.TAS_CLOSING_STOCK_BALANCE;
MSCK REPAIR TABLE FLARE_8.TAS_DSR_ADHERENCE;
MSCK REPAIR TABLE FLARE_8.TAS_KPI_TARGET_VS_ACHIEVEMENT;
MSCK REPAIR TABLE FLARE_8.TAS_PRIMARY_SALE;
MSCK REPAIR TABLE FLARE_8.TAS_PRODUCT_MASTER;
MSCK REPAIR TABLE FLARE_8.WBS_BIB_REPORT_DAAS;
MSCK REPAIR TABLE FLARE_8.SPONSORED_DATA_MA;
MSCK REPAIR TABLE FLARE_8.SPONSORED_DATA_DA;
MSCK REPAIR TABLE FLARE_8.INV_SPECIFIC_PAYMENT_VW2;
MSCK REPAIR TABLE FLARE_8.INV_SPECIFIC_PAYMENT_VW;
MSCK REPAIR TABLE FLARE_8.TAX_INVOICE_VW;
MSCK REPAIR TABLE FLARE_8.AVAYA_CLID;
MSCK REPAIR TABLE FLARE_8.AVAYA_IVR;"
hive -e "MSCK REPAIR TABLE FLARE_8.HSDP_OPTIN_AUTORENEWAL;
 MSCK REPAIR TABLE FLARE_8.TPC_CB_NEWREG_BIOUPDT_POOL_DAILY;
 MSCK REPAIR TABLE FLARE_8.HSDP_OPTOUT_AUTORENEWAL;
 MSCK REPAIR TABLE FLARE_8.HSDP_RENEWAL_BASE;
 MSCK REPAIR TABLE FLARE_8.HUAMSC_DAAS;
 MSCK REPAIR TABLE FLARE_8.NG_RETAILERPROMO;
 MSCK REPAIR TABLE FLARE_8.SMART_APP_DOWNLOAD;
 MSCK REPAIR TABLE FLARE_8.SMART_APP;
 MSCK REPAIR TABLE FLARE_8.SMART_APP_NEW_USERS;
 MSCK REPAIR TABLE FLARE_8.QMATIC;
 MSCK REPAIR TABLE FLARE_8.UPC_SUBSCRIPTION;
 MSCK REPAIR TABLE FLARE_8.SMSC;
 MSCK REPAIR TABLE FLARE_8.MSC_CDR;
 MSCK REPAIR TABLE FLARE_8.SHOP_LOCATOR;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_CITY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_CITY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_CLUSTER_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_CLUSTER_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_COUNTRY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_COUNTRY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_COUNTRY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_LGA_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_LGA_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_LGA_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_REGION_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_REGION_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_SITE_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_SITE_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_STATE_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_STATE_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.DIRECT_CONNECT_CCTP;
 MSCK REPAIR TABLE FLARE_8.ESM_SIMSWAP_METRIC;
 MSCK REPAIR TABLE FLARE_8.ESM_SUB_METRIC;
 MSCK REPAIR TABLE FLARE_8.ESM_UNSUB_METRIC;
 MSCK REPAIR TABLE FLARE_8.ESM_VTU_VENDING_METRIC;
 MSCK REPAIR TABLE FLARE_8.CC_SURVEY;
"
hive -e "MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_CLUSTER_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.DIRECT_CONNECT_CCTP;
 MSCK REPAIR TABLE FLARE_8.MAPS_CORE_PGW_DAILY;
 MSCK REPAIR TABLE FLARE_8.MAPS_4G_CELL_D;
"
hive -e "MSCK REPAIR TABLE FLARE_8.AUDIT_LOGS;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_CLUSTER_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_COUNTRY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_LGA_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_MONTHLY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.ESM_SIM_REG_METRIC;
 MSCK REPAIR TABLE FLARE_8.ESM_SHARENSELL_METRICS;
 MSCK REPAIR TABLE FLARE_8.ESM_DYA_METRIC;
 MSCK REPAIR TABLE FLARE_8.BILL_SUMMARY_REP_MON;
 MSCK REPAIR TABLE FLARE_8.BIB_CREDIT_CONTROL_REPORT;
 MSCK REPAIR TABLE FLARE_8.MTN_BIB_PALLET_ALLSK;
 MSCK REPAIR TABLE FLARE_8.MTN_BIB_SERIAL_ALLSK;
 MSCK REPAIR TABLE FLARE_8.MTN_BIB_SHIPPER_ALLSK;
 MSCK REPAIR TABLE FLARE_8.IFS_BIB_CUSTOMER_CREDIT_INFO;
 MSCK REPAIR TABLE FLARE_8.SMART_APP_NEW_USERS;
 MSCK REPAIR TABLE FLARE_8.ERS_VEND;
 MSCK REPAIR TABLE FLARE_8.CCN_CDR;
 MSCK REPAIR TABLE FLARE_8.HUAMSC_DAAS;
"
hive -e "
MSCK REPAIR TABLE FLARE_8.AGENT_USER_BIB;
MSCK REPAIR TABLE FLARE_8.AGL_CRM_COUNTRY_MAP;
MSCK REPAIR TABLE FLARE_8.AGL_CRM_LGA_MAP;
MSCK REPAIR TABLE FLARE_8.AGL_CRM_STATE_MAP;
MSCK REPAIR TABLE FLARE_8.BIB_AGL_FXL_STS_REPORT;
MSCK REPAIR TABLE FLARE_8.BIB_AGL_GSM_STS_REPORT;
MSCK REPAIR TABLE FLARE_8.COMPENSATION;
MSCK REPAIR TABLE FLARE_8.CREDIT_INFORMATION_MON;
MSCK REPAIR TABLE FLARE_8.CUG_ACCESS_FEES;
MSCK REPAIR TABLE FLARE_8.IFS_BIB_customer_credit_info;
MSCK REPAIR TABLE FLARE_8.MNP_PORTING_BROADCAST;
MSCK REPAIR TABLE FLARE_8.PREPAID_PAYMENTS_LOG;
MSCK REPAIR TABLE FLARE_8.SMS_INCENTIVE_IMEI_VW;
MSCK REPAIR TABLE FLARE_8.tbl_imei_registration_dtls_vw;
MSCK REPAIR TABLE FLARE_8.TT_MSO_1_ISP_LOGIN_IDS_LOOKUP;
MSCK REPAIR TABLE FLARE_8.WBS_PM_RATED_CDRS;
MSCK REPAIR TABLE FLARE_8.BIB_AGL_SUBBASE_TAB_RPT;
MSCK REPAIR TABLE FLARE_8.MULTIPLE_REG_BIB;
MSCK REPAIR TABLE FLARE_8.WBS_CLIENT_DAARS_TAPIN;
MSCK REPAIR TABLE FLARE_8.WBS_CLIENT_DAARS_TAPOUT;
"

hive -e "
MSCK REPAIR TABLE FLARE_8.AGL_BILL_ANALYZER_MONTHLY;
MSCK REPAIR TABLE FLARE_8.BILL_RUN_STATISTICS_TAB;
MSCK REPAIR TABLE FLARE_8.BUDGET_YEAR_AMOUNT;
MSCK REPAIR TABLE FLARE_8.CB_SCHEDULES;
MSCK REPAIR TABLE FLARE_8.CB_SUBS_POS_SERVICES;
MSCK REPAIR TABLE FLARE_8.CHANGE_REPORT_MTN;
MSCK REPAIR TABLE FLARE_8.CLIENT_ACTIVITY_LOG;
MSCK REPAIR TABLE FLARE_8.DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE;
MSCK REPAIR TABLE FLARE_8.ECW_TRANSACTION; 
MSCK REPAIR TABLE FLARE_8.EMM_DELIVERY_KPI;
MSCK REPAIR TABLE FLARE_8.FINANCIALLOG;
MSCK REPAIR TABLE FLARE_8.HPD_HELP_DESK;
MSCK REPAIR TABLE FLARE_8.MANUAL_KPI;
MSCK REPAIR TABLE FLARE_8.MFS_AUDIT_LOG;
MSCK REPAIR TABLE FLARE_8.MKT_DATA_METRICS;
MSCK REPAIR TABLE FLARE_8.MSO_ACCURACY_STATISTICS;
MSCK REPAIR TABLE FLARE_8.MSO_PROCESS_AVG_TIME;
MSCK REPAIR TABLE FLARE_8.MTN_OVERVIEW_COMM_RPT;
MSCK REPAIR TABLE FLARE_8.MTN_PR_DETAILS;
MSCK REPAIR TABLE FLARE_8.OTP_STATUS;
MSCK REPAIR TABLE FLARE_8.RECHARGE_SUCCESS_RATE;
MSCK REPAIR TABLE FLARE_8.SERV_STATUS_SEAMFIX_REF;
MSCK REPAIR TABLE FLARE_8.SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE;
MSCK REPAIR TABLE FLARE_8.SMS_ACTIVATION_REQUEST_JOIN;
MSCK REPAIR TABLE FLARE_8.SMS_ACTIVATION_REQUEST;
MSCK REPAIR TABLE FLARE_8.SMS_ACTIVATION_VW;
MSCK REPAIR TABLE FLARE_8.USSD_ERROR_CODE_BREAKDOWN;
MSCK REPAIR TABLE FLARE_8.VALIDATION_PASSED;

MSCK REPAIR TABLE FLARE_8.VTU_LOG;
MSCK REPAIR TABLE FLARE_8.XAAS_BRANCH;
MSCK REPAIR TABLE FLARE_8.MYMTN_ADOPTION_KPI;
MSCK REPAIR TABLE FLARE_8.CIS_KPI;
"

hive -e "
MSCK REPAIR TABLE FLARE_8.CGW_API;
MSCK REPAIR TABLE FLARE_8.IB_API;
MSCK REPAIR TABLE FLARE_8.MYMTNAPP;
MSCK REPAIR TABLE FLARE_8.SAG_API;
MSCK REPAIR TABLE FLARE_8.SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE;
MSCK REPAIR TABLE FLARE_8.SMSC_ERROR_BREAKDOWN_PER_ACCOUNT;
MSCK REPAIR TABLE FLARE_8.SMSC_LICENSE;
MSCK REPAIR TABLE FLARE_8.SMSC_TOTAL_DELIVERY_SUCCESS_RATE;
MSCK REPAIR TABLE FLARE_8.SMSC_TOTAL_SUBMIT_SUCCESS_RATE;
MSCK REPAIR TABLE FLARE_8.USSD_ERROR_CODE_BREAKDOWN;
MSCK REPAIR TABLE FLARE_8.USSD_LICENSE_USAGE;
MSCK REPAIR TABLE FLARE_8.USSD_TRAFFIC_SUCCESS_RATE;
MSCK REPAIR TABLE FLARE_8.MSO_PROCESS_AVG_TIME;
MSCK REPAIR TABLE FLARE_8.CS5_AIR_ADJ_DA;
MSCK REPAIR TABLE FLARE_8.CS5_AIR_REFILL_AC;
MSCK REPAIR TABLE FLARE_8.CS5_AIR_REFILL_DA;
MSCK REPAIR TABLE FLARE_8.CS5_CCN_GPRS_AC;
MSCK REPAIR TABLE FLARE_8.CS5_CCN_GPRS_DA;
MSCK REPAIR TABLE FLARE_8.CS5_CCN_SMS_AC;
MSCK REPAIR TABLE FLARE_8.CS5_CCN_SMS_DA;
MSCK REPAIR TABLE FLARE_8.CS5_CCN_VOICE_AC;
MSCK REPAIR TABLE FLARE_8.CS5_CCN_VOICE_DA;
MSCK REPAIR TABLE FLARE_8.CS5_SDP_ACC_ADJ_AC;
MSCK REPAIR TABLE FLARE_8.CS5_SDP_ACC_ADJ_DA;
MSCK REPAIR TABLE FLARE_8.MSC_DAAS;
"
