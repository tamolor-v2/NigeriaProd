kinit -kt /etc/security/keytabs/daasuser.keytab daasuser@MTN.COM
#mv /mnt/beegfs/live/SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE/incoming/* /mnt/beegfs/FlareDataTest/SMSC_A2P_P2P_P2A_A2A_SUCCESS_RATE_NEW/incoming/
#mv /mnt/beegfs/live/SMSC_TOTAL_SUBMIT_SUCCESS_RATE/incoming/* /mnt/beegfs/FlareDataTest/SMSC_TOTAL_SUBMIT_SUCCESS_RATE_NEW/incoming/
#mv /mnt/beegfs/live/SMSC_TOTAL_DELIVERY_SUCCESS_RATE/incoming/* /mnt/beegfs/FlareDataTest/SMSC_TOTAL_DELIVERY_SUCCESS_RATE_NEW/incoming/
#mv /mnt/beegfs/live/SMSC_ERROR_BREAKDOWN_PER_ACCOUNT/incoming/* /mnt/beegfs/FlareDataTest/SMSC_ERROR_BREAKDOWN_PER_ACCOUNT_NEW/incoming/
#mv /mnt/beegfs/live/SMSC_LICENSE/incoming/* /mnt/beegfs/FlareDataTest/SMSC_LICENSE_NEW/incoming/
#mv /mnt/beegfs/live/MyMTNApp/incoming/* /mnt/beegfs/FlareDataTest/MyMTNApp/incoming/
#mv /mnt/beegfs/live/USSD_TRAFFIC_SUCCESS_RATE/incoming/* /mnt/beegfs/FlareDataTest/USSD_TRAFFIC_SUCCESS_RATE/incoming/
#mv /mnt/beegfs/live/USSD_ERROR_CODE_BREAKDOWN/incoming/* /mnt/beegfs/FlareDataTest/USSD_ERROR_CODE_BREAKDOWN/incoming/
#mv /mnt/beegfs/live/USSD_LICENSE_USAGE/incoming/* /mnt/beegfs/FlareDataTest/USSD_LICENSE_USAGE_NEW/incoming/
#mv /mnt/beegfs/live/SAG_API/incoming/* /mnt/beegfs/FlareDataTest/SAG_API/incoming/
#mv /mnt/beegfs/live/IB_API/incoming/* /mnt/beegfs/FlareDataTest/IB_API/incoming/
#mv /mnt/beegfs/live/CGW_API/incoming/* /mnt/beegfs/FlareDataTest/CGW_API_NEW/incoming/


hive -e ' msck repair table flare_8.cgw_api;msck repair table flare_8.ib_api;msck repair table flare_8.mymtnapp;msck repair table flare_8.sag_api;msck repair table flare_8.smsc_a2p_p2p_p2a_a2a_success_rate;msck repair table flare_8.smsc_error_breakdown_per_account;msck repair table flare_8.smsc_license;msck repair table flare_8.smsc_total_delivery_success_rate;msck repair table flare_8.smsc_total_submit_success_rate;msck repair table flare_8.ussd_error_code_breakdown;msck repair table flare_8.ussd_license_usage;msck repair table flare_8.ussd_traffic_success_rate;msck repair table flare_8.MSO_PROCESS_AVG_TIME;'
