select LOGIN_ID_V, PASSWORD_V, SERV_ACC_LINK_CODE_N, STATUS_V, TRANSACTION_NUM_V, TO_DATE(TO_CHAR(TRANS_DATE_D, 'yyyy-MM-DD HH:mm:ss'), 'yyyy-MM-DD HH:mi:ss') TRANS_DATE_D,ITEM_CODE_N,LOCATION_CODE_N,SERVICE_CODE_V,SUB_SERVICE_CODE_V,'234'||DUMMY_NUMBER_V as msisdn from CBS_TBL_CUST.ISP_LOGIN_IDS_LOOKUP