ts=$(date +"%Y%m%d_%H%M%S")
echo "running msck_repair at $ts">>/home/daasuser/last_CaaS_msck_run.log
beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "set hive.msck.path.validation =ignore;msck repair table kamanja_test.rejecteddata"

beeline -n daasuser -p '' -u 'jdbc:hive2://datanode01026.mtn.com:2181,datanode01028.mtn.com:2181,master01001.mtn.com:2181,master01002.mtn.com:2181,master01003.mtn.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2' -e "
msck repair table stg_gen.GEN_LED_VOUCHER_ROW ;
msck repair table stg_gen.KYC_DEALER; 
msck repair table stg_gen.GEN_LED_VOUCHER ;
msck repair table stg_gen.LEDGER_ITEM_TAB ;
--msck repair table stg_gen.CUST_ORD_CUSTOMER_ENT ; 
msck repair table stg_gen.CUSTOMER_ORDER_LINE_TAB ;
msck repair table stg_gen.CUSTOMER_GROUP;
msck repair table stg_gen.CUSTOMER_INFO_TAB ;
msck repair table stg_gen.FND_USER;
msck repair table stg_gen.FND_USER_ROLE ;
msck repair table stg_gen.PRICE_LIST;
msck repair table stg_gen.PURCHASE_ORDER;
msck repair table stg_gen.PURCHASE_REQUISiTION_TAB;
msck repair table stg_gen.RETURN_MATERIAL_LINE;
msck repair table stg_gen.SALES_PRICE_LIST_PART ;
--msck repair table stg_gen.SUPPLIER_INFO_ADDRESS ;
msck repair table stg_gen.SUPPLIER_INFO ;
msck repair table stg_gen.BUDGET_PERIOD_AMOUNT;
msck repair table stg_gen.CUSTOMER_ORDER_HISTORY;
msck repair table stg_gen.CUSTOMER_DETAILS;
msck repair table stg_gen.CUSTOMER_ORDER_TAB;
msck repair table stg_gen.PAYMENT_ADDRESS_GENERAL ;
msck repair table stg_gen.PAYMENT_TERM;
msck repair table stg_gen.PAYMENT_TAB ;
msck repair table stg_gen.PURCHASE_ORDER_HIST ;
msck repair table stg_gen.PURCHASE_REQ_LINE ;
msck repair table stg_gen.PURCHASE_ORDER_LINE ;
msck repair table stg_gen.PURCHASE_RECEIPT_TAB;
msck repair table stg_gen.PURCH_REQ_APPROVAL;
msck repair table stg_gen.PURCHASE_ORDER_APPROVAL ;
msck repair table stg_gen.DBA_ROLE_PRIVS;
msck repair table stg_gen.DBA_TAB_PRIVS ;
--msck repair table stg_gen.BLACKLIST_HISTORY ;
msck repair table stg_gen.KM_USER_ROLE;
msck repair table stg_gen.ENROLLMENT_REF;
msck repair table stg_gen.NODE;
msck repair table stg_gen.NODE_ASSIGNMENT ;
msck repair table stg_gen.DEALER_TYPE ;
msck repair table stg_gen.KM_USER ;
msck repair table stg_gen.SMS_ACTIVATION_REQUEST;
msck repair table stg_gen.STATE ;
msck repair table stg_gen.CB_POS_TRANSACTIONS ;
msck repair table stg_gen.CB_RETAIL_OUTLETS ;
msck repair table stg_gen.CODE_G;
msck repair table stg_gen.CODE_F;
msck repair table stg_gen.MAN_SUPP_INVOICE_ITEM ;
--msck repair table stg_gen.MAN_SUPP_INVOICE;
msck repair table stg_gen.PAYMENT_PLAN_AUTH ;
--msck repair table stg_gen.MTN_CUST_AVAILABLE_CREDIT ;
msck repair table stg_gen.TAX_ITEM_QRY;
msck repair table stg_gen.VAT_PERC;
msck repair table stg_gen.inventory_part_in_stock;
msck repair table stg_gen.MASTER_DATA_VIEW;
msck repair table stg_gen.INVOICE_LEDGER_ITEM_SU_QRY;
msck repair table stg_gen.INVOICE ;
msck repair table stg_gen.Ledger_Item ;
msck repair table stg_gen.CUSTOMER_CREDIT_INFO;
--msck repair table stg_gen.CORPORATE_FORM;
msck repair table stg_gen.HISTORY_LOG ;
msck repair table stg_gen.SPECIAL_DATA;
msck repair table stg_gen.WSQ_IMAGE;
msck repair table stg_gen.CB_ACCOUNT_SERVICE_LIST ;
msck repair table stg_gen.TBL_ORDER_DTLS_VIEW ;
msck repair table stg_gen.HISTORY_LOG_ATTRIBUTE ;
msck repair table stg_gen.IDENTITY_INVOICE_INFO ;
msck repair table stg_gen.BANK_GUARANTEE;
msck repair table stg_gen.DAY_TRANS_DETAIL ;
msck repair table stg_gen.CUST_ORDER_TYPE ;
msck repair table stg_gen.customer_info_comm_method ;

"
