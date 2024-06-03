ts=$(date +"%Y%m%d_%H%M%S")
echo "running msck_repair at $ts">>/home/daasuser/last_msck_run.log

beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.CS5_VTU_DUMP;
MSCK REPAIR TABLE FLARE_8.NGVS_CDR;
 MSCK REPAIR TABLE FLARE_8.AVAYA_CLID;
MSCK REPAIR TABLE FLARE_8.AVAYA_IVR"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "set hive.msck.path.validation =ignore;msck repair table kamanja_test.rejecteddata"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.HSDP_OPTIN_AUTORENEWAL;
 MSCK REPAIR TABLE FLARE_8.HSDP_OPTOUT_AUTORENEWAL;
 MSCK REPAIR TABLE FLARE_8.HSDP_RENEWAL_BASE;
 MSCK REPAIR TABLE FLARE_8.HUAMSC_DAAS;
 MSCK REPAIR TABLE FLARE_8.NG_RETAILERPROMO;
 MSCK REPAIR TABLE FLARE_8.SMART_APP_DOWNLOAD;
 MSCK REPAIR TABLE FLARE_8.SMART_APP;
 MSCK REPAIR TABLE FLARE_8.SMART_APP_NEW_USERS;
 MSCK REPAIR TABLE FLARE_8.QMATIC;
 MSCK REPAIR TABLE FLARE_8.WBS_BIB_REPORT_DAAS;
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
 MSCK REPAIR TABLE FLARE_8.MOBILE_MONEY;
"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_CLUSTER_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_DAILY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.NETWORK_WEEKLY_TERRITORY_AH_REPORT;
 MSCK REPAIR TABLE FLARE_8.DIRECT_CONNECT_CCTP;
 MSCK REPAIR TABLE FLARE_8.MAPS_CORE_PGW_DAILY;
 MSCK REPAIR TABLE FLARE_8.MAPS_4G_CELL_D;
"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "MSCK REPAIR TABLE FLARE_8.AUDIT_LOGS;
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
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "
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
 msck repair table  flare_8.sdp_dmp_ac;
"


beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table audit.files_filesopps_summary"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.ngvs_cdr"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.AVAYA_CLID"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.AVAYA_IVR"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.cs5_vtu_dump"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.NGVS_CDR"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.CIS_CDR"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.msc_daas"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.TAPIN_VOICE"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.TAPOUT_VOICE"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.TAPIN_GPRS"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.TAPOUT_GPRS"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.FLYTXT_CAMPAIGN_EVENTS_DATA_new"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.FLYTXT_CAMPAIGN_EVENTS_DATA"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.offer_dump"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.LEA_MAPPING_MSC_DAAS"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.file_stats"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.landingzonerecon"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.cs5_sdp_pam_all"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.HSDP_DOI_LOG"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.MVAS_DND_MSISDN_REPORT"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.ESM_PMT_METRIC"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.ESM_REFILL_METRIC"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.MA_MONITOR"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.MAPS_2G_CELL_D"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.MAPS_3G_CELL_D"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.MAPS_3G_CN_D"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.MAPS_3G_CN_D_BH"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.FLYTXT_LATCH_DUMP"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.PAYMENT_GATEWAY_AIRTIME"
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "msck repair table flare_8.DUMP_SHARESELL"
echo "Finished"
