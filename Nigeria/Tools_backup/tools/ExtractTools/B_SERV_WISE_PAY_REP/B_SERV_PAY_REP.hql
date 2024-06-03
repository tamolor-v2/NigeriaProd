CREATE EXTERNAL TABLE `flare_8.b_serv_wise_pay_rep_tbl`(
 SUBSCRIBER_NAME_V  string,
 PAY_MODE_CODE_V string,
 MODE_PAYMENT_V string,
 TRANS_NUM_V string,
 CHEQUE_NUM_V string,
 USER_NAME_V string,
 LOCATION_CODE_V string,
 LOCATION_V string,
 BANK_NAME_V string,
 SUBS_CATEGORY_DESC_V  string,
 SUBS_SUB_CATEGORY_DESC_V string,
 MSISDN_V string,
 SERVICE_CODE_V string,
 TRANS_DATE_D string,
 CHQ_EXP_DATE_D string,
 ACCOUNT_CODE_N bigint,
 SUBSCRIBER_CODE_N bigint,
 USER_CODE_N  int,
 PAYMENT_AMT_N double,
 ADV_AMT_N double,
 AM_NAME_V string,
 OUTSTANDING_AMT_N  double,
 PACKAGE_NAME_V string,
 DESCRIPTION_V string)
 ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '/FlareData/output_8/B_SERV_WISE_PAY_REP/'
