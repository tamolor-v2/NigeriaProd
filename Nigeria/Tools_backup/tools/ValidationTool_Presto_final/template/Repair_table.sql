set hive.msck.path.validation =ignore;
msck repair table flare_8.FINANCIAL_LOG;
msck repair table flare_8.rejecteddata;
msck repair table audit.files_filesopps_summary;
msck repair table flare_8.filetransdatemsgbydate;
msck repair table flare_8.BUNDLE4U_GPRS;
msck repair table flare_8.BUNDLE4U_VOICE;
msck repair table flare_8.CS5_AIR_ADJ_MA;
msck repair table flare_8.CS5_CCN_GPRS_MA;
msck repair table flare_8.CS5_CCN_SMS_MA;
msck repair table flare_8.CS5_SDP_ACC_ADJ_MA;
msck repair table flare_8.DMC_DUMP_ALL;
msck repair table flare_8.GGSN_CDR;
msck repair table flare_8.MSC_CDR;
msck repair table flare_8.SDP_DMP_MA;
msck repair table flare_8.UC_DUMP;
msck repair table flare_8.MOBILE_MONEY;
msck repair table flare_8.CS5_AIR_REFILL_MA;
msck repair table flare_8.SDP_DMP_DA;
msck repair table flare_8.CS5_CCN_VOICE_MA;
msck repair table flare_8.DPI_CDR;
msck repair table flare_8.CIS_CDR;
msck repair table flare_8.ERS_VEND_NEW;
msck repair table flare_8.USSD_TRAFFIC_SUCCESS_RATE;
msck repair table flare_8.SMSC_ERROR_BREAKDOWN_PER_ACCOUNT;
msck repair table flare_8.SMSC_TOTAL_DELIVERY_SUCCESS_RATE;
msck repair table flare_8.SMSC_LICENSE;
msck repair table flare_8.SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE;
msck repair table flare_8.MyMTNApp;
msck repair table flare_8.CGW_API;
msck repair table flare_8.gsm_sims_master;
msck repair table flare_8.AGILITY_GSM_SERVICE_MAST;
msck repair table flare_8.SRM_REQUEST;
msck repair table flare_8.PBM_PROBLEM_INVESTIGATION;
msck repair table flare_8.AGILITY_PAYMENT;
msck repair table flare_8.IFSAPP_ACCOUNT;
msck repair table flare_8.IFSAPP_BUDGET_COMM_COST_VAR;
msck repair table flare_8.IFSAPP_PRO_BUDGET_COMMITMENTS;
msck repair table flare_8.IFSAPP_MTN_RECEIPT_RECONC_RPT;
msck repair table flare_8.IFSAPP_BUDGET_YEAR_AMOUNT_UNION;
msck repair table flare_8.MTN_PR_DETAILS;
msck repair table flare_8.VTU_KPI_METRICS;
msck repair table flare_8.SIM_SWAP_SUCCESS_COUNT;
msck repair table flare_8.SIM_REG_SUCCESS_COUNT;
msck repair table flare_8.DSA_SUCCESS_COUNT;
msck repair table flare_8.RBT_SUCCESS_COUNT;
msck repair table flare_8.PAYMENT_GATEWAY;
msck repair table flare_8.IB_API;
msck repair table flare_8.SAG_API;
msck repair table flare_8.AVAYA_CLID;
msck repair table flare_8.AVAYA_IVR;
msck repair table flare_8.TAPIN_VOICE;
msck repair table flare_8.TAPIN_GPRS;
msck repair table flare_8.TAPOUT_VOICE;
msck repair table flare_8.TAPOUT_GPRS;
msck repair table flare_8.CS5_SDP_PAM_ALL;
msck repair table flare_8.CS6_CCN_CDR;
msck repair table flare_8.CS6_AIR_CDR;
msck repair table flare_8.CS6_SDP_CDR;
msck repair table flare_8.MNP;
msck repair table flare_8.HSDP_CDR;
msck repair table flare_8.SGSN_CDR;
msck repair table flare_8.MTN_BIB_SERIAL_ALLSK;
msck repair table flare_8.MTN_BIB_PALLET_ALLSK;
msck repair table flare_8.MTN_BIB_SHIPPER_ALLSK;
msck repair table flare_8.FLYTXT_LATCH_DUMP;
msck repair table flare_8.MAPS2G;
msck repair table flare_8.MAPS3G;
msck repair table flare_8.MAPS4G;
msck repair table flare_8.UDC_DUMP;
msck repair table flare_8.VALID_USER;
msck repair table flare_8.MTN_CONNECT_USER_DETAILS;
msck repair table flare_8.port_in_out;
msck repair table flare_8.BALANCES;
msck repair table flare_8.DAILY_BVN_LINKING;
msck repair table flare_8.DYA_DAILY_ACTIVATION;
msck repair table flare_8.CANVASA;
msck repair table flare_8.INWARD;
msck repair table flare_8.OUTWARD;
msck repair table flare_8.TOKEN_REPORT;
msck repair table flare_8.TRANSACTING_AGENT;
msck repair table flare_8.USSD_CDR;
msck repair table flare_8.BYPASS_VENDOR;
msck repair table flare_8.DUMP_SHARESELL;
msck repair table flare_8.PAYMENT_GATEWAY_AIRTIME;
msck repair table flare_8.SUBSCRIBER_TRANSACTIONS_CDR;
msck repair table flare_8.XAAS_DAILY;
msck repair table flare_8.HSS;
msck repair table flare_8.LTE_HSS;
msck repair table flare_8.CS5_AIR_ADJ_DA;
msck repair table flare_8.CS5_AIR_REFILL_AC;
msck repair table flare_8.CS5_AIR_REFILL_DA;
msck repair table flare_8.CS5_CCN_GPRS_AC;
msck repair table infra.apilog;
msck repair table flare_8.CS5_CCN_GPRS_DA;
msck repair table flare_8.CS5_CCN_SMS_AC;
msck repair table flare_8.CS5_CCN_SMS_DA;
msck repair table flare_8.CS5_CCN_VOICE_AC;
msck repair table flare_8.CS5_CCN_VOICE_DA;
msck repair table flare_8.CS5_SDP_ACC_ADJ_AC;
msck repair table flare_8.CS5_SDP_ACC_ADJ_DA;
msck repair table flare_8.CLM_SHOP_REQUESTS;
msck repair table flare_8.MSC_DAAS;
msck repair table flare_8.BILL_SUMMARY_REP_MON;
msck repair table flare_8.CREDIT_INFORMATION_MON;
msck repair table flare_8.COMPENSATION;
msck repair table flare_8.AGL_CRM_COUNTRY_MAP;
msck repair table flare_8.AGL_CRM_LGA_MAP;
msck repair table flare_8.AGL_CRM_STATE_MAP;
msck repair table flare_8.BIB_AGL_FXL_STS_REPORT;
msck repair table flare_8.BIB_AGL_GSM_STS_REPORT;
msck repair table flare_8.CALL_REASON;
msck repair table flare_8.IFSAPP_CUSTOMER_INFO_TAB;
msck repair table flare_8.IFSAPP_CUSTOMER_ORDER_LINE;
msck repair table flare_8.IFSAPP_INVENTORY_TRANSACTION_HIS;
msck repair table flare_8.IFSAPP_MANUF_FILE_DETAIL;
msck repair table flare_8.IFSAPP_PURCHASE_ORDER_LINE_TAB;
msck repair table flare_8.TT_MSO_1_ISP_LOGIN_IDS_LOOKUP;
msck repair table flare_8.SMS_INCENTIVE_IMEI_VW;
msck repair table flare_8.SPON_PROVIDER_DTLS;
msck repair table flare_8.PREPAID_PAYMENTS_LOG;
msck repair table flare_8.BIB_AGL_SUBBASE_TAB_RPT;
msck repair table flare_8.AGENT_USER_BIB;
msck repair table flare_8.MULTIPLE_REG_BIB;
msck repair table flare_8.SDP_DMP_AC;
msck repair table flare_8.NG_RETAILERPROMO;
msck repair table flare_8.SHOP_LOCATOR;
msck repair table flare_8.SMART_APP_CDR;
msck repair table flare_8.DIRECT_CONNECT_CCTP;
msck repair table flare_8.ESM_DYA_METRIC;
msck repair table flare_8.ESM_PMT_METRIC;
msck repair table flare_8.ESM_REFILL_METRIC;
msck repair table flare_8.ESM_SIM_REG_METRIC;
msck repair table flare_8.ESM_SUB_METRIC;
msck repair table flare_8.ESM_UNSUB_METRIC;
msck repair table flare_8.ESM_VTU_VENDING_METRIC;
msck repair table flare_8.HSDP_OPTIN_AUTORENEWAL;
msck repair table flare_8.HSDP_OPTOUT_AUTORENEWAL;
msck repair table flare_8.HSDP_RENEWAL_BASE;
msck repair table flare_8.MA_MONITOR;
msck repair table flare_8.SMART_APP_DOWNLOAD;
msck repair table flare_8.SMART_APP_NEW_USERS;
msck repair table flare_8.SMSC;
msck repair table flare_8.UPC_SUBSCRIPTION;
msck repair table flare_8.AGL_BILL_ANALYZER_MONTHLY;
msck repair table flare_8.SIM_SWAP;
msck repair table flare_8.BILL_RUN_STATISTICS_TAB;
msck repair table flare_8.BUDGET_YEAR_AMOUNT;
msck repair table flare_8.CB_SCHEDULES;
msck repair table flare_8.CB_SUBS_POS_SERVICES;
msck repair table flare_8.CHANGE_REPORT_MTN;
msck repair table flare_8.CLIENT_ACTIVITY_LOG;
msck repair table flare_8.HPD_HELP_DESK;
msck repair table flare_8.MSO_ACCURACY_STATISTICS;
msck repair table flare_8.MSO_PROCESS_AVG_TIME;
msck repair table flare_8.MTN_OVERVIEW_COMM_RPT;
msck repair table flare_8.MTN_PR_DETAILS;
msck repair table flare_8.OTP_STATUS;
msck repair table flare_8.PROVISIONING_LOG;
msck repair table flare_8.SME_REPORTS;
msck repair table flare_8.SERV_STATUS_SEAMFIX_REF;
msck repair table flare_8.SMS_ACTIVATION_REQUEST_KPI;
msck repair table flare_8.SMS_ACTIVATION_REQUEST_JOIN;
msck repair table flare_8.SMS_ACTIVATION_VW;
msck repair table flare_8.VALIDATION_PASSED;
msck repair table flare_8.RECHARGE_SUCCESS_RATE;
msck repair table flare_8.USSD_ERROR_CODE_BREAKDOWN;
msck repair table flare_8.MFS_AUDIT_LOG;
msck repair table flare_8.DELIVERY_ERROR_BREAKDOWN_PER_TRAFFIC_TYPE;
msck repair table flare_8.SERVICE_USAGE_NOTIFICATION_DELIVERY_SUCCESS_RATE;
msck repair table flare_8.EMM_DELIVERY_KPI;
msck repair table flare_8.FINANCIALLOG;
msck repair table flare_8.MANUAL_KPI;
msck repair table flare_8.ECW_TRANSACTION;
msck repair table flare_8.MAPS_2G_CELL_W_BH;
msck repair table flare_8.MAPS_2G_CN_W_BH;
msck repair table flare_8.MAPS_3G_CELL_D_BH;
msck repair table flare_8.MAPS_3G_CELL_M;
msck repair table flare_8.MAPS_3G_CELL_M_BH;
msck repair table flare_8.MAPS_CORE_HLR_D;
msck repair table flare_8.MAPS_CORE_HSS_D;
msck repair table flare_8.MAPS_CORE_MGW_D;
msck repair table flare_8.MAPS_CORE_MME_D;
msck repair table flare_8.MAPS_CORE_MSC_D;
msck repair table flare_8.MAPS_2G_CELL_D;
msck repair table flare_8.MAPS_2G_CN_M_DATA_BH;
msck repair table flare_8.MAPS_3G_CELL_W;
msck repair table flare_8.MAPS_3G_CN_D;
msck repair table flare_8.MAPS_2G_CN_M_DATA_BH;
msck repair table flare_8.MAPS_3G_CN_W_BH;
msck repair table flare_8.MAPS_3G_CELL_D;
msck repair table flare_8.MAPS_3G_CN_M;
msck repair table flare_8.MAPS_3G_CN_D_BH;
msck repair table flare_8.NETWORK_DAILY_SITE_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_CITY_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_CLUSTER_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_LGA_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_TERRITORY_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_STATE_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_REGION_AH_REPORT;
msck repair table flare_8.NETWORK_DAILY_COUNTRY_AH_REPORT;
msck repair table flare_8.NETWORK_MONTHLY_COUNTRY_AH_REPORT;
msck repair table flare_8.NETWORK_MONTHLY_LGA_AH_REPORT;
msck repair table flare_8.NETWORK_MONTHLY_CLUSTER_AH_REPORT;
msck repair table flare_8.NETWORK_Monthly_REGION_AH_Report;
msck repair table flare_8.NETWORK_Monthly_SITE_AH_Report;
msck repair table flare_8.NETWORK_Monthly_CITY_AH_Report;
msck repair table flare_8.NETWORK_MONTHLY_TERRITORY_AH_REPORT;
msck repair table flare_8.MKT_DAILY_COUNTRY_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_COUNTRY_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_STATE_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_TERRITORY_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_CITY_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_LGA_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_SITE_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_REGION_AH_REPORT;
msck repair table flare_8.MKT_WEEKLY_CLUSTER_AH_REPORT;
msck repair table flare_8.MSO_REVERSED_BILLING;
msck repair table flare_8.OFFER_PLAN_INFORMATION;
msck repair table flare_8.TBL_IMEI_REGISTRATION_DTLS_VW;
msck repair table flare_8.AA_AD_BUNDLE;
msck repair table flare_8.AA_AD_BONUS;
msck repair table flare_8.NGVS_DUMP;
msck repair table flare_8.SERVICE_LIST;
msck repair table flare_8.TARIFF_TYPE;
msck repair table flare_8.AA_TARIFF_DATA;
msck repair table flare_8.COSTS_FOR_PROFITABILITY;
msck repair table flare_8.HSDP_DOI_LOG;
msck repair table flare_8.FLYTXT_CAMPAIGN_EVENTS_DATA;
msck repair table flare_8.SDP_API;
msck repair table flare_8.TBL_DATA_AGENT_REGISTRATION_VW;
msck repair table flare_8.WBS_PM_RATED_CDRS;
msck repair table flare_8.NEWREG_BIOUPDT_POOL_DAILY;
msck repair table flare_8.OFFER_DUMP;
msck repair table flare_8.NEWREG_BIOUPDT_POOL_WEEKLY;
msck repair table flare_8.NGVS_CDR;
msck repair table flare_8.CUG_ACCESS_FEES;
msck repair table flare_8.MNP_PORTING_BROADCAST;
msck repair table flare_8.ERS_VEND;
msck repair table flare_8.ERS_VEND_NEW;
msck repair table flare_8.CS5_VTU_DUMP;
msck repair table flare_8.RECON;
msck repair table flare_8.MVAS_DND_MSISDN_REPORT;
msck repair table flare_8.dpi_cdr_unpack;
msck repair table AUDIT.Reprocessing_summary_report;
msck repair table AUDIT.REPROCESSING_REPORT;
msck repair table flare_8.MKT_DAILY_CITY_AH_REPORT;
msck repair table flare_8.PSB_TRANSACTIONS; 
msck repair table flare_8.PSB_REGISTRATIONS; 
msck repair table flare_8.PSB_USER_ACCOUNT_BALANCES_REPORT ;
msck repair table flare_8.PSB_SYSTEM_ACCOUNT_BALANCES_REPORT ;
msck repair table flare_8.PROVISIONING_LOG ;
msck repair table flare_8.FIN_LOG;
msck repair table flare_8.AUDIT_LOGS ;
msck repair table flare_8.EWP_ACCOUNT_HOLDERS_DUMP ;
msck repair table flare_8.INACTIVE_DEVICES ;
msck repair table flare_8.UC_DUMP;
msck repair table flare_8.FINANCIAL_LOG;
msck repair table flare_8.edw_report;
--msck repair table flare_8.CB_RETAIL_OUTLETS;
--msck repair table flare_8.BUDGET_PERIOD_AMOUNT;
--msck repair table flare_8.CORPORATE_FORM;
--msck repair table flare_8.CUST_ORD_CUSTOMER_ENT;
--msck repair table flare_8.CUST_ORDER_TYPE;
--msck repair table flare_8.CUSTOMER_CREDIT_INFO;
--msck repair table flare_8.CUSTOMER_DETAILS;
--msck repair table flare_8.CUSTOMER_GROUP;
--msck repair table flare_8.CUSTOMER_INFO_COMM_METHOD;
--msck repair table flare_8.CUSTOMER_INFO_TAB;
--msck repair table flare_8.CUSTOMER_ORDER_HISTORY;
--msck repair table flare_8.CUSTOMER_ORDER_LINE_TAB;
--msck repair table flare_8.CUSTOMER_ORDER_TAB;
--msck repair table flare_8.DAY_TRANS_DETAIL;
--msck repair table flare_8.DBA_ROLE_PRIVS;
--msck repair table flare_8.DBA_TAB_PRIVS;
--msck repair table flare_8.FND_USER;
--msck repair table flare_8.FND_USER_ROLE;
--msck repair table flare_8.GEN_LED_VOUCHER;
--msck repair table flare_8.BLACKLIST_HISTORY;
--msck repair table flare_8.DEALER_TYPE;
--msck repair table flare_8.ENROLLMENT_REF;
--msck repair table flare_8.KM_USER;
--msck repair table flare_8.KM_USER_ROLE;
--msck repair table flare_8.KYC_DEALER;
--msck repair table flare_8.NODE;
--msck repair table flare_8.NODE_ASSIGNMENT;
--msck repair table flare_8.SMS_ACTIVATION_REQUEST;
--msck repair table flare_8.STATE;
--msck repair table flare_8.MASTER_DATA_VIEW;
--msck repair table flare_8.PAYMENT_ADDRESS_GENERAL;
--msck repair table flare_8.PRICE_LIST;
--msck repair table flare_8.PURCHASE_REQ_LINE;
--msck repair table flare_8.RETURN_MATERIAL_LINE;
--msck repair table flare_8.SALES_PRICE_LIST_PART;
--msck repair table flare_8.SUPPLIER_INFO_ADDRESS;
--msck repair table flare_8.HISTORY_LOG;
--msck repair table flare_8.INVENTORY_PART_IN_STOCK;
--msck repair table flare_8.INVENTORY_TRANSACTION_HIST;
--msck repair table flare_8.LEDGER_ITEM_TAB;
--msck repair table flare_8.PAYMENT_TAB;
--msck repair table flare_8.PAYMENT_TERM;
--msck repair table flare_8.PURCH_REQ_APPROVAL;
--msck repair table flare_8.PURCHASE_ORDER;
--msck repair table flare_8.PURCHASE_ORDER_APPROVAL;
--msck repair table flare_8.PURCHASE_ORDER_HIST;
--msck repair table flare_8.PURCHASE_ORDER_LINE;
--msck repair table flare_8.PURCHASE_RECEIPT_TAB;
--msck repair table flare_8.PURCHASE_REQUISITION_TAB;
--msck repair table flare_8.SUPPLIER_INFO;
--msck repair table flare_8.CODE_G;
--msck repair table flare_8.CODE_F;
--msck repair table flare_8.MAN_SUPP_INVOICE_ITEM;
--msck repair table flare_8.MAN_SUPP_INVOICE;
--msck repair table flare_8.PAYMENT_PLAN_AUTH;
--msck repair table flare_8.MTN_CUST_AVAILABLE_CREDIT;
--msck repair table flare_8.TAX_ITEM_QRY;
--msck repair table flare_8.VAT_PERC;
--msck repair table flare_8.HISTORY_LOG_ATTRIBUTE;
