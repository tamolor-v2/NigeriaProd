CREATE EXTERNAL TABLE `agent_user_bib`(
  `get_email` string, 
  `agent_first_name` string, 
  `agent_surname` string, 
  `agent_mobile` string, 
  `dealer_name` string, 
  `dealer_code` string, 
  `dealer_mobile` string, 
  `deal_address` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:27 15:36:24.629
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/AGENT_USER_BIB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543329879')
CREATE EXTERNAL TABLE `bank_guarantee`(
  `advance_amount` string, 
  `bank_name` string, 
  `bg_amount` string, 
  `bg_category_type` string, 
  `bg_no` string, 
  `bg_type` string, 
  `company` string, 
  `doc_date` string, 
  `expired_by` string, 
  `expiry_date` string, 
  `expiry_reason` string, 
  `ifs_ref` string, 
  `issue_date` string, 
  `vendor_no` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 18:09:57.670
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/BANK_GUARANTEE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542906853')
CREATE EXTERNAL TABLE `basic_data`(
  `id` string, 
  `biometric_capture_agent` string, 
  `birthday` string, 
  `bd_part_key` string, 
  `firstname` string, 
  `gender` string, 
  `is_processed` string, 
  `last_basic_data_edit_agent` string, 
  `last_basic_data_edit_login_id` string, 
  `match_found` string, 
  `match_id` string, 
  `othername` string, 
  `sms_status` string, 
  `surname` string, 
  `sv_init_timestamp` string, 
  `sv_push_timestamp` string, 
  `sv_status` string, 
  `sync_status` string, 
  `state_of_registration_fk` string, 
  `user_id_fk` string, 
  `type_v` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:21 14:24:06.500
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/BASIC_DATA'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542807007')
CREATE EXTERNAL TABLE `bfp_failure_log`(
  `pk` bigint, 
  `active` int, 
  `create_date` string, 
  `deleted` string, 
  `last_modified` string, 
  `app_version` string, 
  `capture_agent` string, 
  `filename` string, 
  `kit_tag` string, 
  `mac_address` string, 
  `msisdn` string, 
  `reason1` string, 
  `reason2` string, 
  `reason3` string, 
  `reason4` string, 
  `reason5` string, 
  `rejection_reason` string, 
  `sim_serial` string, 
  `unique_id` string, 
  `reg_type` string, 
  `file_sync_date` string, 
  `source_path` string, 
  `target_path` string, 
  `enrollment_ref_device_id` string, 
  `metadata_device_id` string, 
  `realtime_device_id` string, 
  `ref_device_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `msisdn_key` bigint)
COMMENT '
  DDL generation date: 2018:11:07 09:24:13.510
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/BFP_FAILURE_LOG'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541585483')
CREATE EXTERNAL TABLE `blacklist_history`(
  `id` bigint, 
  `activity_timestamp` string, 
  `blacklisted` int, 
  `enrollment_ref` string, 
  `blacklist_reason` string, 
  `km_user_id` string, 
  `ip_address` string, 
  `mac_address` string, 
  `approved_by` string, 
  `node_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:07 09:17:35.120
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/BLACKLIST_HISTORY'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541585441')
CREATE EXTERNAL TABLE `budget_period_amount`(
  `budget_year` string, 
  `budget_version` string, 
  `posting_combination_id` string, 
  `account` string, 
  `code_f` string, 
  `project_id` string, 
  `budget_period` string, 
  `amount` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 17:46:51.630
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/BUDGET_PERIOD_AMOUNT'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542905542')
CREATE EXTERNAL TABLE `cb_account_service_list`(
  `account_code_n` bigint, 
  `account_link_code_n` bigint, 
  `service_code_v` string, 
  `from_date_d` bigint, 
  `to_date_d` string, 
  `bill_cycl_code_n` bigint, 
  `service_info_v` string, 
  `status_code_v` string, 
  `contract_type_v` string, 
  `view_subs_level_n` int, 
  `services_right_code_n` int, 
  `sub_service_code_v` string, 
  `summary_created_flg_v` string, 
  `cs_status_v` string, 
  `status_change_date_d` string, 
  `subscriber_type_v` string, 
  `accs_accnt_mgnr_n` bigint, 
  `last_modified_date_d` string, 
  `proxy_flag_v` string, 
  `proxy_msisdn_id` string, 
  `package_code_v` string, 
  `tax_optn_v` string, 
  `tax_plan_code_v` string, 
  `subsidiary_code_v` string, 
  `ledger_code_n` bigint, 
  `part_subsidiary_v` string, 
  `service_po_v` string, 
  `sub_status_code_v` string, 
  `ext_subscriber_code_v` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 09:27:05.327
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CB_ACCOUNT_SERVICE_LIST'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1544611622')
CREATE EXTERNAL TABLE `cb_pos_transactions`(
  `location_code_v` string, 
  `total_amount_n` string, 
  `trans_date_d` bigint, 
  `amount_n` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:12:09 11:42:39.145
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CB_POS_TRANSACTIONS'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1544370512')
CREATE EXTERNAL TABLE `cb_retail_outlets`(
  `location_code_n` bigint, 
  `location_code_v` string, 
  `location_v` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 15:42:40.917
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CB_RETAIL_OUTLETS'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543149826')
CREATE EXTERNAL TABLE `cgw_api`(
  `transactiondate` bigint, 
  `srcmodule` string, 
  `destmodule` string, 
  `apiname` string, 
  `total_success` string, 
  `total_failed` string, 
  `error_code_list` string, 
  `successrate` string, 
  `timedelay` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:14 10:35:05.154
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CGW_API'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542188629')
CREATE EXTERNAL TABLE `code_f`(
  `code_f_value` string, 
  `description` string, 
  `valid_from` string, 
  `valid_until` string, 
  `sort_value` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 18:27:10.025
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CODE_F'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542907833')
CREATE EXTERNAL TABLE `code_g`(
  `code_g_value` string, 
  `description` string, 
  `valid_from` string, 
  `valid_until` string, 
  `accounting_text_id` string, 
  `text` string, 
  `cons_company` string, 
  `cons_code_part` string, 
  `cons_code_part_value` string, 
  `sort_value` string, 
  `objid` string, 
  `objversion` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 16:26:06.461
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CODE_G'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542900800')
CREATE EXTERNAL TABLE `corporate_form`(
  `country_code` string, 
  `corporate_form` string, 
  `corporate_form_desc` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
DDL generation date: 2018:11:08 08:35:44.491'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CORPORATE_FORM'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541675984')
CREATE EXTERNAL TABLE `cust_ord_customer_ent`(
  `customer_id` string, 
  `name` string, 
  `cust_grp` string, 
  `date_del` string, 
  `objid` string, 
  `objversion` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 11:25:11.777
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUST_ORD_CUSTOMER_ENT'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542628825')
CREATE EXTERNAL TABLE `cust_order_type`(
  `description` string, 
  `objid` string, 
  `objversion` string, 
  `oe_alloc_assign_flag` string, 
  `oe_alloc_assign_flag_db` string, 
  `pick_inventory_type` string, 
  `pick_inventory_type_db` string, 
  `order_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 12:54:51.636
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUST_ORDER_TYPE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542628881')
CREATE EXTERNAL TABLE `customer_credit_info`(
  `allowed_due_amount` string, 
  `allowed_due_days` string, 
  `credit_limit` string, 
  `identity` string, 
  `name` string, 
  `objversion` string, 
  `pre_set_credit_limit` string, 
  `party_type` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 11:57:35.818
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_CREDIT_INFO'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542885215')
CREATE EXTERNAL TABLE `customer_details`(
  `address1` string, 
  `address2` string, 
  `city` string, 
  `customer_id` string, 
  `name` string, 
  `region_code` string, 
  `region_description` string, 
  `state` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 09:40:34.991
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_DETAILS'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629455')
CREATE EXTERNAL TABLE `customer_group`(
  `cust_grp` string, 
  `description` string, 
  `objversion` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:08 09:47:57.548
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_GROUP'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541677556')
CREATE EXTERNAL TABLE `customer_info_comm_method`(
  `customer_id` string, 
  `comm_id` int, 
  `value` string, 
  `description` string, 
  `valid_from` string, 
  `valid_to` string, 
  `method_default` string, 
  `address_default` string, 
  `method_id` string, 
  `address_id` string, 
  `objversion` string, 
  `name` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 13:04:27.281
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_INFO_COMM_METHOD'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542888708')
CREATE EXTERNAL TABLE `customer_info_tab`(
  `corporate_form` string, 
  `creation_date` string, 
  `customer_id` string, 
  `default_domain` string, 
  `identifier_reference` string, 
  `name` string, 
  `rowversion` int, 
  `party` string, 
  `party_type` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 13:14:44.923
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_INFO_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542628999')
CREATE EXTERNAL TABLE `customer_order_history`(
  `date_entered` string, 
  `history_no` int, 
  `message_text` string, 
  `order_no` string, 
  `objstate` string, 
  `objversion` string, 
  `userid` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 09:24:35.940
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_ORDER_HISTORY'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629410')
CREATE EXTERNAL TABLE `customer_order_line_tab`(
  `base_sale_unit_price` string, 
  `buy_qty_due` string, 
  `catalog_no` string, 
  `catalog_type` string, 
  `contract` string, 
  `customer_no` string, 
  `date_entered` string, 
  `demand_order_ref3` string, 
  `desired_qty` string, 
  `discount` string, 
  `district_code` string, 
  `line_item_no` string, 
  `line_no` string, 
  `order_code` string, 
  `order_discount` string, 
  `order_no` string, 
  `part_price` string, 
  `price_list_no` string, 
  `qty_invoiced` string, 
  `qty_shipped` string, 
  `qty_to_ship` string, 
  `region_code` string, 
  `rel_no` string, 
  `rma_line_no` string, 
  `rma_no` string, 
  `rowstate` string, 
  `rowversion` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:26 13:57:57.604
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_ORDER_LINE_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543237346')
CREATE EXTERNAL TABLE `customer_order_tab`(
  `authorize_code` string, 
  `contract` string, 
  `customer_po_no` string, 
  `customer_no` string, 
  `date_entered` string, 
  `order_id` string, 
  `order_no` string, 
  `rowversion` string, 
  `cancel_reason` string, 
  `rowstate` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 10:00:41.915
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/CUSTOMER_ORDER_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629521')
CREATE EXTERNAL TABLE `dashboard_quarantine`(
  `date_column` bigint, 
  `msisdn` bigint, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:27 15:18:00.258
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DASHBOARD_QUARANTINE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543328546')
CREATE EXTERNAL TABLE `dashboard_validation`(
  `sync_date` bigint, 
  `total_msisdns` bigint, 
  `passed` bigint, 
  `failed` bigint, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:12:03 15:11:40.587
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DASHBOARD_VALIDATION'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543846466')
CREATE EXTERNAL TABLE `day_trans_detail`(
  `gross_price` string, 
  `ref_date` string, 
  `retail_shop_no` string, 
  `net_price` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:21 14:37:29.868
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DAY_TRANS_DETAIL'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542811817')
CREATE EXTERNAL TABLE `dba_role_privs`(
  `admin_option` string, 
  `default_role` string, 
  `granted_role` string, 
  `grantee` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:12 10:15:50.857
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DBA_ROLE_PRIVS'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542014506')
CREATE EXTERNAL TABLE `dba_tab_privs`(
  `grantable` string, 
  `grantor` string, 
  `hierarchy` string, 
  `owner` string, 
  `privilege` string, 
  `table_name` string, 
  `grantee` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:12 13:02:07.167
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DBA_TAB_PRIVS'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542024706')
CREATE EXTERNAL TABLE `dealer_type`(
  `pk` bigint, 
  `active` int, 
  `create_date` bigint, 
  `deleted` int, 
  `last_modified` string, 
  `code` string, 
  `name` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:06 10:55:55.556
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DEALER_TYPE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541504917')
CREATE EXTERNAL TABLE `device_mapper`(
  `code` string, 
  `mac_address` string, 
  `dealer` string, 
  `dealer_code` string, 
  `dealer_fk` bigint, 
  `orbita_id` string, 
  `dept` string, 
  `dealer_division` string, 
  `dealer_email` string, 
  `dealer_mobile` string, 
  `km_user_pk` string, 
  `node_id` bigint, 
  `zone_fk` string, 
  `region` string, 
  `state` string, 
  `lga` string, 
  `area` string, 
  `territory` string, 
  `outlet` string, 
  `device_id` string, 
  `enrollment_ref_id` bigint, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string, 
  `msisdn_key` bigint)
COMMENT '
  DDL generation date: 2018:11:27 15:11:55.015
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/device_mapper'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543328706')
CREATE EXTERNAL TABLE `dynamic_data`(
  `id` bigint, 
  `da1` string, 
  `da2` string, 
  `da3` string, 
  `da4` string, 
  `da5` string, 
  `da6` string, 
  `da7` string, 
  `da8` string, 
  `da9` string, 
  `da10` string, 
  `dda1` string, 
  `dda2` string, 
  `dda3` string, 
  `dda4` string, 
  `dda5` string, 
  `dda6` string, 
  `dda7` string, 
  `dda8` string, 
  `dda9` string, 
  `dda10` string, 
  `da11` string, 
  `da12` string, 
  `da13` string, 
  `da14` string, 
  `da15` string, 
  `da16` string, 
  `da17` string, 
  `da18` string, 
  `da19` string, 
  `da20` string, 
  `dda11` string, 
  `dda12` string, 
  `dda13` string, 
  `dda14` string, 
  `dda15` string, 
  `dda16` string, 
  `dda17` string, 
  `dda18` string, 
  `dda19` string, 
  `dda20` string, 
  `verified_basic_data_fk` string, 
  `basic_data_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:19 14:46:38.442
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/DYNAMIC_DATA'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542635645')
CREATE EXTERNAL TABLE `enrollment_ref`(
  `id` bigint, 
  `code` string, 
  `is_corporate` string, 
  `custom1` string, 
  `custom2` string, 
  `custom3` string, 
  `date_installed` bigint, 
  `description` string, 
  `installed_by` string, 
  `mac_address` string, 
  `name` string, 
  `network_card_name` string, 
  `state` string, 
  `country_fk` string, 
  `enref_part_key` string, 
  `libo` string, 
  `device_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:13 11:07:54.905
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/ENROLLMENT_REF'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542104191')
CREATE EXTERNAL TABLE `fnd_user`(
  `identity` string, 
  `objversion` string, 
  `description` string, 
  `web_user` string, 
  `active` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:08 10:39:14.279
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/FND_USER'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541682221')
CREATE EXTERNAL TABLE `fnd_user_role`(
  `identity` string, 
  `objid` string, 
  `objversion` string, 
  `role` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:08 12:54:28.881
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/FND_USER_ROLE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541682234')
CREATE EXTERNAL TABLE `gen_led_voucher`(
  `accounting_period` int, 
  `accounting_year` int, 
  `approval_date` string, 
  `approved_by_userid` string, 
  `status_cancelled` string, 
  `userid` string, 
  `voucher_date` string, 
  `voucher_no` bigint, 
  `voucher_type` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 09:14:23.402
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/GEN_LED_VOUCHER'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542628438')
CREATE EXTERNAL TABLE `gen_led_voucher_row`(
  `account` string, 
  `account_desc` string, 
  `accounting_year` string, 
  `amount` string, 
  `creator_desc` string, 
  `credit_amount` string, 
  `currency_code` string, 
  `debet_amount` string, 
  `objversion` string, 
  `row_no` string, 
  `third_currency_amount` string, 
  `third_currency_credit_amount` string, 
  `third_currency_debit_amount` string, 
  `trans_code` string, 
  `voucher_date` string, 
  `voucher_no` string, 
  `voucher_type` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 12:26:48.917
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/GEN_LED_VOUCHER_ROW'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543146671')
CREATE EXTERNAL TABLE `history_log`(
  `log_id` string, 
  `module` string, 
  `lu_name` string, 
  `table_name` string, 
  `time_stamp` string, 
  `keys` string, 
  `username` string, 
  `history_type` string, 
  `rowversion` string, 
  `transaction_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 13:00:01.692
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/HISTORY_LOG'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543147634')
CREATE EXTERNAL TABLE `history_log_attribute`(
  `column_name` string, 
  `new_value` string, 
  `old_value` string, 
  `objversion` string, 
  `log_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 16:03:01.552
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/HISTORY_LOG_ATTRIBUTE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542899386')
CREATE EXTERNAL TABLE `ib_api`(
  `transactiondate` bigint, 
  `srcmodule` string, 
  `destmodule` string, 
  `apiname` string, 
  `total_success` string, 
  `total_failed` string, 
  `error_code_list` string, 
  `successrate` string, 
  `timedelay` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:14 09:17:49.185
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/IB_API'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542187724')
CREATE EXTERNAL TABLE `identity_invoice_info`(
  `identity` string, 
  `objversion` string, 
  `description` string, 
  `oracle_user` string, 
  `web_user` string, 
  `active` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 16:52:21.224
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/IDENTITY_INVOICE_INFO'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542902102')
CREATE EXTERNAL TABLE `inventory_part_in_stock`(
  `create_date` string, 
  `waiv_dev_rej_no` string, 
  `location_no` string, 
  `receipt_date` string, 
  `contract` string, 
  `part_no` string, 
  `qty_onhand` int, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 10:16:19.975
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/INVENTORY_PART_IN_STOCK'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542628536')
CREATE EXTERNAL TABLE `inventory_transaction_hist`(
  `part_no` string, 
  `location_no` string, 
  `waiv_dev_rej_no` string, 
  `transaction` string, 
  `cost` string, 
  `date_applied` string, 
  `direction` string, 
  `quantity` string, 
  `userid` string, 
  `date_created` string, 
  `date_time_created` string, 
  `pallet_id` string, 
  `receipt_date` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:27 10:14:27.701
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/INVENTORY_TRANSACTION_HIST'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543311005')
CREATE EXTERNAL TABLE `invoice`(
  `delivery_date` string, 
  `accounting_year_ref` string, 
  `arrival_date` string, 
  `c3` string, 
  `c7` string, 
  `creation_date` string, 
  `creators_reference` string, 
  `currency` string, 
  `d1` string, 
  `due_date` string, 
  `identity` string, 
  `invoice_date` string, 
  `invoice_id` string, 
  `invoice_no` string, 
  `invoice_type` string, 
  `js_invoice_state_db` string, 
  `net_curr_amount` string, 
  `net_dom_amount` string, 
  `party_type` string, 
  `objstate` string, 
  `objversion` string, 
  `series_id` string, 
  `vat_curr_amount` string, 
  `vat_dom_amount` string, 
  `voucher_date_ref` string, 
  `voucher_no_ref` string, 
  `voucher_type_ref` string, 
  `canceled_time` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 17:26:04.003
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/INVOICE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542904162')
CREATE EXTERNAL TABLE `invoice_ledger_item_su_qry`(
  `invoice_id` string, 
  `party_type` string, 
  `invoice_date` string, 
  `inv_dom_amount` string, 
  `objstate` string, 
  `po_ref_number` string, 
  `invoice_no` string, 
  `identity` string, 
  `name` string, 
  `auth_id` string, 
  `first_auth_id` string, 
  `voucher_date_ref` string, 
  `voucher_no_ref` string, 
  `accounting_year_ref` string, 
  `pay_term_id` string, 
  `adv_inv` string, 
  `ledger_item_version` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 17:09:16.071
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/INVOICE_LEDGER_ITEM_SU_QRY'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542903258')
CREATE EXTERNAL TABLE `isp_login_ids_lookup`(
  `login_id_v` string, 
  `password_v` string, 
  `serv_acc_link_code_n` bigint, 
  `status_v` string, 
  `transaction_num_v` string, 
  `trans_date_d` bigint, 
  `item_code_n` bigint, 
  `location_code_n` bigint, 
  `service_code_v` string, 
  `sub_service_code_v` string, 
  `dummy_number_v` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `msisdn_key` bigint, 
  `date_key` int, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:21 14:20:12.906
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/ISP_LOGIN_IDS_LOOKUP'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543149964')
CREATE EXTERNAL TABLE `km_user`(
  `pk` bigint, 
  `active` string, 
  `create_date` bigint, 
  `deleted` string, 
  `last_modified` string, 
  `email_address` string, 
  `first_name` string, 
  `gender` string, 
  `mobile` string, 
  `orbita_id` string, 
  `other_name` string, 
  `password` string, 
  `surname` string, 
  `zone_fk` string, 
  `client_first_login` string, 
  `failed_login_attempts` string, 
  `last_failed_login` string, 
  `last_password_change` string, 
  `last_successful_login` string, 
  `locked_out` string, 
  `dealer_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `msisdn_key` bigint)
COMMENT '
  DDL generation date: 2018:11:21 10:29:12.473
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/KM_USER'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542796225')
CREATE EXTERNAL TABLE `km_user_role`(
  `surname` string, 
  `first_name` string, 
  `email_address` string, 
  `mobile` string, 
  `role` string, 
  `active` string, 
  `active_status` string, 
  `create_date` bigint, 
  `last_modified` string, 
  `last_successful_login` string, 
  `dealer_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string, 
  `msisdn_key` bigint)
COMMENT '
  DDL generation date: 2018:11:06 16:35:06.256
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/KM_USER_ROLE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541585293')
CREATE EXTERNAL TABLE `kyc_blacklist`(
  `pk` string, 
  `active` string, 
  `create_date` string, 
  `deleted` string, 
  `last_modified` string, 
  `blacklisted_item` string, 
  `revoked` string, 
  `mac_address` string, 
  `enrollment_ref` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:27 15:09:53.429
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/KYC_BLACKLIST'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543328526')
CREATE EXTERNAL TABLE `kyc_dealer`(
  `pk` bigint, 
  `active` int, 
  `create_date` bigint, 
  `deleted` int, 
  `last_modified` string, 
  `address` string, 
  `contact_address` string, 
  `deal_code` string, 
  `email_address` string, 
  `mobile_number` string, 
  `name` string, 
  `zone_fk` string, 
  `dealer_type_fk` bigint, 
  `orbita_id` string, 
  `km_user_pk` string, 
  `dealer_division_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `msisdn_key` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:06 16:27:51.432
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/KYC_DEALER'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541585238')
CREATE EXTERNAL TABLE `ledger_item`(
  `objstate` string, 
  `objevents` string, 
  `state` string, 
  `company` string, 
  `identity` string, 
  `party_type` string, 
  `party_type_db` string, 
  `ledger_item_series_id` string, 
  `ledger_item_id` string, 
  `ledger_item_version` string, 
  `full_curr_amount` string, 
  `full_dom_amount` string, 
  `cleared_curr_amount` string, 
  `cleared_dom_amount` string, 
  `cancel_vou_date` string, 
  `objversion` string, 
  `ledger_status_type` string, 
  `ledger_status_type_db` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 17:37:01.384
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/LEDGER_ITEM'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542904854')
CREATE EXTERNAL TABLE `ledger_item_tab`(
  `full_dom_amount` string, 
  `cleared_dom_amount` string, 
  `cleared_curr_amount` string, 
  `curr_rate` string, 
  `currency` string, 
  `full_curr_amount` int, 
  `ledger_item_id` string, 
  `ledger_item_series_id` string, 
  `party_type` string, 
  `payment_id` string, 
  `rowstate` string, 
  `identity` string, 
  `trans_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:21 13:54:20.374
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/LEDGER_ITEM_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542811482')
CREATE EXTERNAL TABLE `man_supp_invoice`(
  `invoice_date` string, 
  `invoice_id` string, 
  `pay_term_id` string, 
  `objstate` string, 
  `po_ref_number` string, 
  `voucher_no_ref` string, 
  `voucher_type_ref` string, 
  `gross_dom_amount` string, 
  `identity` string, 
  `party_type` string, 
  `state` string, 
  `invoice_no` string, 
  `net_dom_amount` string, 
  `due_date` string, 
  `ncf_reference` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 19:16:42.080
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/MAN_SUPP_INVOICE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542911243')
CREATE EXTERNAL TABLE `man_supp_invoice_item`(
  `identity` string, 
  `party_type` string, 
  `party_type_db` string, 
  `invoice_id` string, 
  `item_id` string, 
  `reference` string, 
  `vat_code` string, 
  `gross_amount` string, 
  `gross_dom_amount` string, 
  `net_curr_amount` string, 
  `objstate` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 19:02:24.828
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/MAN_SUPP_INVOICE_ITEM'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542910027')
CREATE EXTERNAL TABLE `master_data_view`(
  `audituser` string, 
  `creation_date` bigint, 
  `distributor_id` string, 
  `user_name` string, 
  `first_name_v` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 15:51:18.635
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/MASTER_DATA_VIEW'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542639616')
CREATE EXTERNAL TABLE `meta_data`(
  `basic_data_fk` string, 
  `met_part_key2` string, 
  `latitude` string, 
  `location_accuracy` string, 
  `longitude` string, 
  `mocked_coordinate` string, 
  `realtime_device_id` string, 
  `within_geofence` string, 
  `enrollment_ref_fk` string, 
  `id` string, 
  `capture_machine_id` string, 
  `confirmation_timestamp` string, 
  `app_version` string, 
  `state_of_registration` string, 
  `agent_mobile` string, 
  `sync_timestamp` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:21 15:25:44.885
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/META_DATA'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542810618')
CREATE EXTERNAL TABLE `mtn_cust_available_credit`(
  `customer_id` string, 
  `customer_name` string, 
  `credit_limit` string, 
  `payments` string, 
  `commisions` string, 
  `uninvoiced_orders` string, 
  `items_in_dispute` string, 
  `total_credit` string, 
  `available_credit` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 10:51:08.036
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/MTN_CUST_AVAILABLE_CREDIT'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543139841')
CREATE VIEW `ng_ifs_corporate_form` AS select
`corporate_form`.`msg_unique_id_enrich` as `csv_row_key`, 
`corporate_form`.`country_code`, 
`corporate_form`.`corporate_form`, 
`corporate_form`.`corporate_form_desc`,
`corporate_form`.`date_key`,
`corporate_form`.`tbl_dt`
FROM `CAAS`.`CORPORATE_FORM`
CREATE VIEW `ng_ifs_customer_credit_info` AS select
`customer_credit_info`.`allowed_due_amount`, 
`customer_credit_info`.`allowed_due_days`, 
`customer_credit_info`.`credit_limit`,
`customer_credit_info`.`identity`, 
`customer_credit_info`.`name`, 
`customer_credit_info`.`objversion`, 
`customer_credit_info`.`pre_set_credit_limit`, 
`customer_credit_info`.`party_type`,
`customer_credit_info`.`date_key`,
`customer_credit_info`.`tbl_dt`
FROM `CAAS`.`CUSTOMER_CREDIT_INFO`
CREATE VIEW `ng_ifs_customer_group` AS SELECT
`customer_group`.`msg_unique_id_enrich` as `csv_row_key`,
`customer_group`.`cust_grp`,
`customer_group`.`description`,
`customer_group`.`date_key`,
`customer_group`.`tbl_dt`
FROM
`CAAS`.`CUSTOMER_GROUP`
CREATE VIEW `ng_ifs_customer_order_history` AS select
`customer_order_history`.`msg_unique_id_enrich` as `csv_row_key`,
`customer_order_history`.`date_entered`, 
`customer_order_history`.`history_no`, 
`customer_order_history`.`message_text`, 
`customer_order_history`.`order_no`, 
`customer_order_history`.`objstate`, 
`customer_order_history`.`objversion`, 
`customer_order_history`.`userid`,
`customer_order_history`.`date_key`,
`customer_order_history`.`tbl_dt`
FROM `CAAS`.`CUSTOMER_ORDER_HISTORY`
CREATE VIEW `ng_ifs_dba_role_privs` AS select
`dba_role_privs`.`admin_option`, 
`dba_role_privs`.`default_role`, 
`dba_role_privs`.`granted_role`, 
`dba_role_privs`.`grantee`,
`dba_role_privs`.`date_key`,
`dba_role_privs`.`tbl_dt`
FROM `CAAS`.`DBA_ROLE_PRIVS`
CREATE VIEW `ng_ifs_dba_tab_privs` AS select
`dba_tab_privs`.`grantable`,
`dba_tab_privs`.`grantor`, 
`dba_tab_privs`.`hierarchy`, 
`dba_tab_privs`.`owner`, 
`dba_tab_privs`.`privilege`, 
`dba_tab_privs`.`table_name`, 
`dba_tab_privs`.`grantee`,
`dba_tab_privs`.`date_key`,
`dba_tab_privs`.`tbl_dt`
FROM `CAAS`.`DBA_TAB_PRIVS`
CREATE VIEW `ng_ifs_fnd_user` AS select
`fnd_user`.`msg_unique_id_enrich` as `csv_row_key`,
`fnd_user`.`identity`, 
`fnd_user`.`objversion`, 
`fnd_user`.`description`, 
`fnd_user`.`web_user`, 
`fnd_user`.`active`,
`fnd_user`.`date_key`,
`fnd_user`.`tbl_dt`
FROM `CAAS`.`FND_USER`
CREATE VIEW `ng_ifs_gen_led_voucher` AS select
`gen_led_voucher`.`msg_unique_id_enrich` as `csv_row_key`,
 `gen_led_voucher`.`accounting_period`,
 `gen_led_voucher`.`accounting_year`,
 `gen_led_voucher`.`approval_date`, 
`gen_led_voucher`.`approved_by_userid`, 
`gen_led_voucher`.`status_cancelled`, 
`gen_led_voucher`.`userid`, 
`gen_led_voucher`.`voucher_date`,
`gen_led_voucher`.`voucher_no`,
`gen_led_voucher`.`voucher_type`,
`gen_led_voucher`.`date_key`,
`gen_led_voucher`.`tbl_dt`
FROM `CAAS`.`GEN_LED_VOUCHER`
CREATE VIEW `ng_ifs_inventory_part_in_stock` AS select
`inventory_part_in_stock`.`msg_unique_id_enrich` as `csv_row_key`,
`inventory_part_in_stock`.`create_date`,
`inventory_part_in_stock`.`waiv_dev_rej_no`, 
`inventory_part_in_stock`.`location_no`,
`inventory_part_in_stock`.`receipt_date`, 
`inventory_part_in_stock`.`contract`, 
`inventory_part_in_stock`.`part_no`, 
`inventory_part_in_stock`.`qty_onhand`,
`inventory_part_in_stock`.`date_key`,
`inventory_part_in_stock`.`tbl_dt`
FROM `CAAS`.`INVENTORY_PART_IN_STOCK`
CREATE VIEW `ng_ifs_payment_address_general` AS SELECT
`payment_address_general`.`msg_unique_id_enrich` as `csv_row_key`,
concat(`payment_address_general`.`account`, ' ', `payment_address_general`.`description`) AS `ACCOUNT_DESCRIPTION`,
`payment_address_general`.`identity`,
`payment_address_general`.`description`,
`payment_address_general`.`account`,
 `payment_address_general`.`company`,
`payment_address_general`.`party_type`,
`payment_address_general`.`party_type_db`,
`payment_address_general`.`way_id`,
`payment_address_general`.`address_id`,
`payment_address_general`.`date_key`,
`payment_address_general`.`tbl_dt`
FROM
`CAAS`.`PAYMENT_ADDRESS_GENERAL`
CREATE VIEW `ng_ifs_payment_tab` AS select
`payment_tab`.`msg_unique_id_enrich` as `csv_row_key`,
`payment_tab`.`company`, 
`payment_tab`.`series_id`, 
`payment_tab`.`payment_id`, 
`payment_tab`.`rowversion`, 
`payment_tab`.`reg_date`, 
`payment_tab`.`pay_date`, 
`payment_tab`.`voucher_date`, 
`payment_tab`.`accounting_year`, 
`payment_tab`.`voucher_type`, 
`payment_tab`.`user_group`, 
`payment_tab`.`userid`, 
`payment_tab`.`voucher_no`, 
`payment_tab`.`payment_type_code`, 
`payment_tab`.`note`, 
`payment_tab`.`year_period_key`, 
`payment_tab`.`payment_rollback_status`,
`payment_tab`.`date_key`,
`payment_tab`.`tbl_dt`
FROM `CAAS`.`PAYMENT_TAB`
CREATE VIEW `ng_ifs_payment_term` AS SELECT
`payment_term`.`msg_unique_id_enrich` as `csv_row_key`,
`payment_term`.`pay_term_id`, 
`payment_term`.`description`
FROM
`CAAS`.`PAYMENT_TERM`
CREATE VIEW `ng_seamfix_basic_data` AS select
`basic_data`.`id`, 
`basic_data`.`biometric_capture_agent`, 
`basic_data`.`birthday`, 
`basic_data`.`bd_part_key`, 
`basic_data`.`firstname`, 
`basic_data`.`gender`,
 `basic_data`.`is_processed`, 
`basic_data`.`last_basic_data_edit_agent`, 
`basic_data`.`last_basic_data_edit_login_id`, 
`basic_data`.`match_found`, 
`basic_data`.`match_id`, 
`basic_data`.`othername`, 
`basic_data`.`sms_status`, 
`basic_data`.`surname`, 
`basic_data`.`sv_init_timestamp`, 
`basic_data`.`sv_push_timestamp`, 
`basic_data`.`sv_status`, 
`basic_data`.`sync_status`, 
`basic_data`.`state_of_registration_fk`, 
`basic_data`.`user_id_fk`, 
`basic_data`.`type_v`,
`basic_data`.`date_key`,
`basic_data`.`tbl_dt`
FROM `CAAS`.`BASIC_DATA`
CREATE VIEW `ng_seamfix_bfp_failure_log` AS select
`bfp_failure_log`.`pk`, 
`bfp_failure_log`.`active`, 
`bfp_failure_log`.`create_date`,
`bfp_failure_log`.`deleted`, 
`bfp_failure_log`.`last_modified`,
`bfp_failure_log`.`app_version`, 
`bfp_failure_log`.`capture_agent`, 
`bfp_failure_log`.`filename`, 
`bfp_failure_log`.`kit_tag`, 
`bfp_failure_log`.`mac_address`, 
`bfp_failure_log`.`msisdn`,
`bfp_failure_log`.`reason1`, 
`bfp_failure_log`.`reason2`, 
`bfp_failure_log`.`reason3`, 
`bfp_failure_log`.`reason4`, 
`bfp_failure_log`.`reason5`, 
`bfp_failure_log`.`rejection_reason`, 
`bfp_failure_log`.`sim_serial`, 
`bfp_failure_log`.`unique_id`,
`bfp_failure_log`.`reg_type`, 
`bfp_failure_log`.`file_sync_date`, 
`bfp_failure_log`.`source_path`, 
`bfp_failure_log`.`target_path`, 
`bfp_failure_log`.`enrollment_ref_device_id`, 
`bfp_failure_log`.`metadata_device_id`, 
`bfp_failure_log`.`realtime_device_id`, 
`bfp_failure_log`.`ref_device_id`,
`bfp_failure_log`.`date_key`,
`bfp_failure_log`.`tbl_dt`
FROM `CAAS`.`BFP_FAILURE_LOG`
CREATE VIEW `ng_seamfix_blacklist_history` AS select 
`blacklist_history`.`id`, 
`blacklist_history`.`activity_timestamp`, 
`blacklist_history`.`blacklisted`, 
`blacklist_history`.`enrollment_ref`, 
`blacklist_history`.`blacklist_reason`, 
`blacklist_history`.`km_user_id`,
`blacklist_history`.`ip_address`, 
`blacklist_history`.`mac_address`, 
`blacklist_history`.`approved_by`, 
`blacklist_history`.`node_fk`, 
`blacklist_history`.`date_key`,
`blacklist_history`.`tbl_dt`
from `CAAS`.`BLACKLIST_HISTORY`
CREATE VIEW `ng_seamfix_dealer_type` AS select
`dealer_type`.`pk`, 
`dealer_type`.`active`, 
`dealer_type`.`create_date`, 
`dealer_type`.`deleted`, 
`dealer_type`.`last_modified`, 
`dealer_type`.`code`, 
`dealer_type`.`name`,
`dealer_type`.`date_key`,
`dealer_type`.`tbl_dt`
FROM `CAAS`.`DEALER_TYPE`
CREATE VIEW `ng_seamfix_dynamic_data` AS select
`dynamic_data`.`id`, 
`dynamic_data`.`da1`, 
`dynamic_data`.`da2`, 
`dynamic_data`.`da3`,
`dynamic_data`.`da4`, 
`dynamic_data`.`da5`, 
`dynamic_data`.`da6`, 
`dynamic_data`.`da7`, 
`dynamic_data`.`da8`, 
`dynamic_data`.`da9`, 
`dynamic_data`.`da10`, 
`dynamic_data`.`dda1`, 
`dynamic_data`.`dda2`, 
`dynamic_data`.`dda3`, 
`dynamic_data`.`dda4`, 
`dynamic_data`.`dda5`, 
`dynamic_data`.`dda6`, 
`dynamic_data`.`dda7`, 
`dynamic_data`.`dda8`, 
`dynamic_data`.`dda9`, 
`dynamic_data`.`dda10`, 
`dynamic_data`.`da11`, 
`dynamic_data`.`da12`, 
`dynamic_data`.`da13`, 
`dynamic_data`.`da14`, 
`dynamic_data`.`da15`, 
`dynamic_data`.`da16`, 
`dynamic_data`.`da17`, 
`dynamic_data`.`da18`, 
`dynamic_data`.`da19`, 
`dynamic_data`.`da20`, 
`dynamic_data`.`dda11`, 
`dynamic_data`.`dda12`, 
`dynamic_data`.`dda13`, 
`dynamic_data`.`dda14`, 
`dynamic_data`.`dda15`, 
`dynamic_data`.`dda16`, 
`dynamic_data`.`dda17`, 
`dynamic_data`.`dda18`, 
`dynamic_data`.`dda19`, 
`dynamic_data`.`dda20`, 
`dynamic_data`.`verified_basic_data_fk`, 
`dynamic_data`.`basic_data_fk`,
`dynamic_data`.`date_key`,
`dynamic_data`.`tbl_dt`
FROM `CAAS`.`DYNAMIC_DATA`
CREATE VIEW `ng_seamfix_enrollment_ref` AS select 
`enrollment_ref`.`id`, 
`enrollment_ref`.`code`, 
`enrollment_ref`.`is_corporate`, 
`enrollment_ref`.`custom1`, 
`enrollment_ref`.`custom2`, 
`enrollment_ref`.`custom3`, 
`enrollment_ref`.`date_installed`, 
`enrollment_ref`.`description`, 
`enrollment_ref`.`installed_by`, 
`enrollment_ref`.`mac_address`,
`enrollment_ref`.`name`, 
`enrollment_ref`.`network_card_name`, 
`enrollment_ref`.`state`, 
`enrollment_ref`.`country_fk`, 
`enrollment_ref`.`enref_part_key`, 
`enrollment_ref`.`libo`, 
`enrollment_ref`.`device_id`, 
`enrollment_ref`.`date_key`,
`enrollment_ref`.`tbl_dt`
from `CAAS`.`ENROLLMENT_REF`
CREATE VIEW `ng_seamfix_km_user` AS select
`km_user`.`pk`, 
`km_user`.`active`, 
`km_user`.`create_date`, 
`km_user`.`deleted`, 
`km_user`.`last_modified`, 
`km_user`.`email_address`, 
`km_user`.`first_name`, 
`km_user`.`gender`, 
`km_user`.`mobile`, 
`km_user`.`orbita_id`, 
`km_user`.`other_name`, 
`km_user`.`password`, 
`km_user`.`surname`, 
`km_user`.`zone_fk`, 
`km_user`.`client_first_login`, 
`km_user`.`failed_login_attempts`, 
`km_user`.`last_failed_login`, 
`km_user`.`last_password_change`, 
`km_user`.`last_successful_login`,
`km_user`.`locked_out`, 
`km_user`.`dealer_fk`,
`km_user`.`date_key`,
`km_user`.`tbl_dt`
FROM `CAAS`.`KM_USER`
CREATE VIEW `ng_seamfix_km_user_role` AS select 
`km_user_role`.`surname`, 
`km_user_role`.`first_name`, 
`km_user_role`.`email_address`, 
`km_user_role`.`mobile`, 
`km_user_role`.`role`, 
`km_user_role`.`active`, 
`km_user_role`.`active_status`, 
`km_user_role`.`create_date`, 
`km_user_role`.`last_modified`, 
`km_user_role`.`last_successful_login`, 
`km_user_role`.`dealer_fk`,
`km_user_role`.`date_key`,
`km_user_role`.`tbl_dt`
from `CAAS`.`KM_USER_ROLE`
CREATE VIEW `ng_seamfix_kyc_dealer` AS select 
`kyc_dealer`.`pk`, 
`kyc_dealer`.`active`, 
`kyc_dealer`.`create_date`, 
`kyc_dealer`.`deleted`, 
`kyc_dealer`.`last_modified`, 
`kyc_dealer`.`address`, 
`kyc_dealer`.`contact_address`, 
`kyc_dealer`.`deal_code`,
`kyc_dealer`.`email_address`, 
`kyc_dealer`.`mobile_number`, 
`kyc_dealer`.`name`, 
`kyc_dealer`.`zone_fk`, 
`kyc_dealer`.`dealer_type_fk`, 
`kyc_dealer`.`orbita_id`, 
`kyc_dealer`.`km_user_pk`, 
`kyc_dealer`.`dealer_division_fk`,
`kyc_dealer`.`date_key`,
`kyc_dealer`.`tbl_dt`
from `caas`.`KYC_DEALER`
CREATE VIEW `ng_seamfix_meta_data` AS select
`meta_data`.`basic_data_fk`, 
`meta_data`.`met_part_key2`, 
`meta_data`.`latitude`, 
`meta_data`.`location_accuracy`, 
`meta_data`.`longitude`, 
`meta_data`.`mocked_coordinate`, 
`meta_data`.`realtime_device_id`, 
`meta_data`.`within_geofence`, 
`meta_data`.`enrollment_ref_fk`, 
`meta_data`.`id`,
 `meta_data`.`capture_machine_id`, 
`meta_data`.`confirmation_timestamp`, 
`meta_data`.`app_version`, 
`meta_data`.`state_of_registration`,
`meta_data`.`agent_mobile`, 
`meta_data`.`sync_timestamp`,
`meta_data`.`date_key`,
`meta_data`.`tbl_dt`
FROM `CAAS`.`META_DATA`
CREATE VIEW `ng_seamfix_node` AS select
`node`.`id`, 
`node`.`active`, 
`node`.`black_listed`, 
`node`.`commissioned`, 
`node`.`is_corporate`, 
`node`.`data_list_tag_name`, 
`node`.`deployment_category`, 
`node`.`installation_timestamp`, 
`node`.`installed_by`, 
`node`.`last_installed_update`, 
`node`.`last_sync_time`, 
`node`.`last_updated`, 
`node`.`location`, 
`node`.`mac_address`, 
`node`.`machine_manufacturer`, 
`node`.`machine_model`, 
`node`.`machine_os`, 
`node`.`machine_serial_number`, 
`node`.`migrated`, 
`node`.`network_card_name`, 
`node`.`previous_tag_name`, 
`node`.`remark`, 
`node`.`sync_status`, 
`node`.`purchase_year`, 
`node`.`device_status_fk`, 
`node`.`kit_type_fk`, 
`node`.`state_fk`, 
`node`.`node_manager_fk`, 
`node`.`field_support_agent_fk`, 
`node`.`district_manager_fk`, 
`node`.`node_activity_status_enum`, 
`node`.`device_owner`, 
`node`.`device_type`, 
`node`.`enrollment_ref`, 
`node`.`blacklist_date`, 
`node`.`geotracker_app_version`, 
`node`.`geotracker_install_date`,
`node`.`date_key`,
`node`.`tbl_dt`
FROM `CAAS`.`NODE`
CREATE VIEW `ng_seamfix_node_assignment` AS select
`node_assignment`.`msg_unique_id_enrich` as `csv_row_key`, 
`node_assignment`.`pk`, 
`node_assignment`.`active`, 
`node_assignment`.`create_date`, 
`node_assignment`.`deleted`, 
`node_assignment`.`last_modified`, 
`node_assignment`.`kyc_dealer_fk`, 
`node_assignment`.`state_fk`, 
`node_assignment`.`zone_fk`, 
`node_assignment`.`dm_user_fk`, 
`node_assignment`.`fsa_user_fk`, 
`node_assignment`.`node_fk`, 
`node_assignment`.`area_fk`, 
`node_assignment`.`lga_fk`, 
`node_assignment`.`outlet_fk`, 
`node_assignment`.`territory_fk`
`DATE_KEY`,
`node_assignment`.`tbl_dt`
FROM `CAAS`.`NODE_ASSIGNMENT`
CREATE VIEW `ng_seamfix_passport` AS select
`passport`.`id`, 
`passport`.`face_count`, 
`passport`.`verified_basic_data_fk`, 
`passport`.`basic_data_fk`,
`passport`.`date_key`,
`passport`.`tbl_dt`
FROM `CAAS`.`PASSPORT`
CREATE VIEW `ng_seamfix_sms_activation_request` AS select
`sms_activation_request`.`id`, 
`sms_activation_request`.`activation_timestamp`, 
`sms_activation_request`.`sar_part_key`, 
`sms_activation_request`.`customer_name`, 
`sms_activation_request`.`enrollment_ref`, 
`sms_activation_request`.`is_initiator`, 
`sms_activation_request`.`phone_number`, 
`sms_activation_request`.`receipt_timestamp`, 
`sms_activation_request`.`registration_timestamp`, 
`sms_activation_request`.`sender_number`, 
`sms_activation_request`.`serial_number`, 
`sms_activation_request`.`state_id`, 
`sms_activation_request`.`status`, 
`sms_activation_request`.`unique_id`,
`sms_activation_request`.`phone_number_status_fk`, 
`sms_activation_request`.`crm_bio_update_time`, 
`sms_activation_request`.`crm_update_time`, 
`sms_activation_request`.`agl_status`, 
`sms_activation_request`.`msisdn_update_status`, 
`sms_activation_request`.`msisdn_update_timestamp`, 
`sms_activation_request`.`previous_unique_id`, 
`sms_activation_request`.`registration_type`, 
`sms_activation_request`.`confirmation_status`, 
`sms_activation_request`.`confirmation_timestamp`, 
`sms_activation_request`.`basic_data_id`,
`sms_activation_request`.`date_key`,
`sms_activation_request`.`tbl_dt`
FROM `CAAS`.`SMS_ACTIVATION_REQUEST`
CREATE VIEW `ng_seamfix_state` AS select
`state`.`msg_unique_id_enrich` as `csv_row_key`,
`state`.`id`, 
`state`.`code`, 
`state`.`name`, 
`state`.`country_fk`, 
`state`.`region_fk`, 
`state`.`zone_fk`, 
`state`.`state_id`,
`state`.`date_key`,
`state`.`tbl_dt`
FROM `CAAS`.`STATE`
CREATE VIEW `ng_seamfix_user_id` AS select
`user_id`.`id`, 
`user_id`.`unique_id`, 
`user_id`.`conflict`, 
`user_id`.`description`, 
`user_id`.`active`, 
`user_id`.`blacklisted`, 
`user_id`.`u_part_key`,
`user_id`.`date_key`,
`user_id`.`tbl_dt`
FROM `CAAS`.`USER_ID`
CREATE VIEW `ng_seamfix_wsq_image` AS select
`wsq_image`.`id`, 
`wsq_image`.`compression_ratio`, 
`wsq_image`.`finger`, 
`wsq_image`.`nfiq`, 
`wsq_image`.`reason_code`, 
`wsq_image`.`basic_data_fk`,
`wsq_image`.`date_key`,
`wsq_image`.`tbl_dt`
FROM `CAAS`.`WSQ_IMAGE`
CREATE VIEW `ng_tpp_master_data_view` AS select
`master_data_view`.`msg_unique_id_enrich` as `csv_row_key`, 
`master_data_view`.`audituser`, 
`master_data_view`.`creation_date`, 
`master_data_view`.`distributor_id`, 
`master_data_view`.`user_name`, 
`master_data_view`.`first_name_v`
`DATE_KEY`,
`master_data_view`.`tbl_dt`
FROM `CAAS`.`MASTER_DATA_VIEW`
CREATE EXTERNAL TABLE `node`(
  `id` bigint, 
  `active` string, 
  `black_listed` string, 
  `commissioned` string, 
  `is_corporate` string, 
  `data_list_tag_name` string, 
  `deployment_category` string, 
  `installation_timestamp` string, 
  `installed_by` string, 
  `last_installed_update` string, 
  `last_sync_time` string, 
  `last_updated` string, 
  `location` string, 
  `mac_address` string, 
  `machine_manufacturer` string, 
  `machine_model` string, 
  `machine_os` string, 
  `machine_serial_number` string, 
  `migrated` string, 
  `network_card_name` string, 
  `previous_tag_name` string, 
  `remark` string, 
  `sync_status` string, 
  `purchase_year` string, 
  `device_status_fk` string, 
  `kit_type_fk` string, 
  `state_fk` string, 
  `node_manager_fk` string, 
  `field_support_agent_fk` string, 
  `district_manager_fk` string, 
  `node_activity_status_enum` string, 
  `device_owner` string, 
  `device_type` string, 
  `enrollment_ref` string, 
  `blacklist_date` string, 
  `geotracker_app_version` string, 
  `geotracker_install_date` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:15 14:41:47.485
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/NODE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542525369')
CREATE EXTERNAL TABLE `node_assignment`(
  `pk` bigint, 
  `active` string, 
  `create_date` bigint, 
  `deleted` string, 
  `last_modified` string, 
  `kyc_dealer_fk` string, 
  `state_fk` string, 
  `zone_fk` string, 
  `dm_user_fk` string, 
  `fsa_user_fk` string, 
  `node_fk` string, 
  `area_fk` string, 
  `lga_fk` string, 
  `outlet_fk` string, 
  `territory_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 14:04:19.613
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/NODE_ASSIGNMENT'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542633058')
CREATE EXTERNAL TABLE `passport`(
  `id` bigint, 
  `face_count` string, 
  `verified_basic_data_fk` string, 
  `basic_data_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:18 13:23:32.494
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PASSPORT'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542614284')
CREATE EXTERNAL TABLE `payment_address_general`(
  `identity` string, 
  `description` string, 
  `account` string, 
  `company` string, 
  `party_type` string, 
  `party_type_db` string, 
  `way_id` string, 
  `address_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:11 15:19:41.052
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PAYMENT_ADDRESS_GENERAL'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541946938')
CREATE EXTERNAL TABLE `payment_plan_auth`(
  `company` string, 
  `identity` string, 
  `party_type` string, 
  `party_type_db` string, 
  `invoice_id` string, 
  `authorized` string, 
  `auth_id` string, 
  `series_id` string, 
  `invoice_no` string, 
  `invoice_date` string, 
  `invoice_type` string, 
  `currency` string, 
  `gross_amount` string, 
  `dom_gross_amount` string, 
  `voucher_no_ref` string, 
  `voucher_type_ref` string, 
  `objstate` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 09:46:37.606
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PAYMENT_PLAN_AUTH'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543138659')
CREATE EXTERNAL TABLE `payment_tab`(
  `company` string, 
  `series_id` string, 
  `payment_id` string, 
  `rowversion` string, 
  `reg_date` string, 
  `pay_date` string, 
  `voucher_date` string, 
  `accounting_year` string, 
  `voucher_type` string, 
  `user_group` string, 
  `userid` string, 
  `voucher_no` string, 
  `payment_type_code` string, 
  `note` string, 
  `year_period_key` string, 
  `payment_rollback_status` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:26 15:00:38.306
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PAYMENT_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543241706')
CREATE EXTERNAL TABLE `payment_term`(
  `pay_term_id` string, 
  `description` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:12 09:30:55.061
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PAYMENT_TERM'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542011961')
CREATE EXTERNAL TABLE `price_list`(
  `objversion` string, 
  `part_no` string, 
  `price` double, 
  `price_date` bigint, 
  `price_list_type_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:08 14:50:21.994
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PRICE_LIST'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541685724')
CREATE EXTERNAL TABLE `purch_req_approval`(
  `authorize_id` string, 
  `objversion` string, 
  `requisition_no` int, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 15:17:19.880
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCH_REQ_APPROVAL'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542896519')
CREATE EXTERNAL TABLE `purchase_order`(
  `order_no` string, 
  `invoicing_supplier` string, 
  `date_entered` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 15:48:59.508
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_ORDER'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629100')
CREATE EXTERNAL TABLE `purchase_order_approval`(
  `approver_sign` string, 
  `date_approved` string, 
  `order_no` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 15:28:31.396
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_ORDER_APPROVAL'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542897196')
CREATE EXTERNAL TABLE `purchase_order_hist`(
  `userid` string, 
  `hist_objstate` string, 
  `order_no` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 13:56:36.865
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_ORDER_HIST'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542892123')
CREATE EXTERNAL TABLE `purchase_order_line`(
  `buy_unit_price` string, 
  `date_entered` string, 
  `order_no` string, 
  `requisition_no` string, 
  `invoicing_supplier` string, 
  `state` string, 
  `currency_code` string, 
  `line_no` string, 
  `buy_qty_due` string, 
  `release_no` string, 
  `buy_unit_meas` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:27 09:09:23.550
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_ORDER_LINE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543307506')
CREATE EXTERNAL TABLE `purchase_receipt_tab`(
  `arrival_date` string, 
  `line_no` string, 
  `order_no` string, 
  `qty_arrived` string, 
  `receipt_no` string, 
  `receiver` string, 
  `release_no` string, 
  `qty_inspected` string, 
  `qty_to_inspect` string, 
  `qty_invoiced` string, 
  `approved_date` string, 
  `finally_invoiced_date` string, 
  `rowstate` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:27 09:34:41.696
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_RECEIPT_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543308556')
CREATE EXTERNAL TABLE `purchase_req_line`(
  `buy_unit_price` int, 
  `line_no` string, 
  `requisition_no` string, 
  `vendor_no` string, 
  `original_qty` int, 
  `objstate` string, 
  `objversion` string, 
  `project_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:22 13:47:05.787
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_REQ_LINE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542891074')
CREATE EXTERNAL TABLE `purchase_requisition_tab`(
  `requisitioner_code` string, 
  `requisition_date` string, 
  `rowstate` string, 
  `requisition_no` string, 
  `authorize_id` string, 
  `rowversion` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:18 16:00:12.715
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/PURCHASE_REQUISITION_TAB'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629136')
CREATE EXTERNAL TABLE `return_material_line`(
  `base_sale_unit_price` string, 
  `catalog_desc` string, 
  `contract` string, 
  `date_returned` string, 
  `line_no` string, 
  `order_no` string, 
  `part_no` string, 
  `purchase_order_no` string, 
  `qty_returned_inv` string, 
  `qty_to_return` string, 
  `rel_no` string, 
  `rma_line_no` int, 
  `rma_no` int, 
  `objstate` string, 
  `objversion` string, 
  `sale_unit_price` string, 
  `credit_approver_id` string, 
  `return_reason_code` string, 
  `qty_received` int, 
  `credit_invoice_no` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:21 14:44:42.442
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/RETURN_MATERIAL_LINE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542812257')
CREATE EXTERNAL TABLE `sag_api`(
  `transactiondate` bigint, 
  `srcmodule` string, 
  `destmodule` string, 
  `apiname` string, 
  `spid` string, 
  `total_success` string, 
  `total_failed` string, 
  `error_code_list` string, 
  `successrate` string, 
  `timedelay` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:14 09:00:56.174
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SAG_API'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542183241')
CREATE EXTERNAL TABLE `sales_price_list_part`(
  `price_list_no` string, 
  `catalog_no` string, 
  `min_quantity` int, 
  `valid_from_date` string, 
  `base_price_site` string, 
  `base_price` string, 
  `percentage_offset` int, 
  `amount_offset` int, 
  `rounding` string, 
  `last_updated` string, 
  `sales_price` string, 
  `discount_type` string, 
  `discount` string, 
  `objversion` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:21 15:22:53.138
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SALES_PRICE_LIST_PART'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542812841')
CREATE EXTERNAL TABLE `scream_service`(
  `transactiondate` bigint, 
  `productid` string, 
  `scene` string, 
  `total_success` string, 
  `total_failed` string, 
  `error_code_list` string, 
  `successrate` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:14 08:52:21.758
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SCREAM_SERVICE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542182212')
CREATE EXTERNAL TABLE `sfx_ncc_record`(
  `file_name_t` string, 
  `unique_id` string, 
  `phone_number` string, 
  `date_generated` string, 
  `sync_date` string, 
  `bucket_status` string, 
  `cat_status` string, 
  `archive_name` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:27 14:46:46.063
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SFX_NCC_RECORD'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543326977')
CREATE EXTERNAL TABLE `sms_activation_request`(
  `id` bigint, 
  `activation_timestamp` bigint, 
  `sar_part_key` string, 
  `customer_name` string, 
  `enrollment_ref` string, 
  `is_initiator` string, 
  `phone_number` string, 
  `receipt_timestamp` string, 
  `registration_timestamp` string, 
  `sender_number` string, 
  `serial_number` string, 
  `state_id` string, 
  `status` string, 
  `unique_id` string, 
  `phone_number_status_fk` string, 
  `crm_bio_update_time` string, 
  `crm_update_time` string, 
  `agl_status` string, 
  `msisdn_update_status` string, 
  `msisdn_update_timestamp` string, 
  `previous_unique_id` string, 
  `registration_type` string, 
  `confirmation_status` string, 
  `confirmation_timestamp` string, 
  `basic_data_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `msisdn_key` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 13:08:23.387
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SMS_ACTIVATION_REQUEST'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629672')
CREATE EXTERNAL TABLE `special_data`(
  `id` bigint, 
  `biometricdatatype` string, 
  `reason` string, 
  `basic_data_fk` bigint, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:06 11:09:39.065
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SPECIAL_DATA'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541505601')
CREATE EXTERNAL TABLE `state`(
  `id` bigint, 
  `code` string, 
  `name` string, 
  `country_fk` string, 
  `region_fk` string, 
  `zone_fk` string, 
  `state_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:19 14:25:09.501
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/STATE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542634572')
CREATE EXTERNAL TABLE `supplier_info`(
  `corporate_form` string, 
  `creation_date` string, 
  `name` string, 
  `association_no` string, 
  `c_status` string, 
  `supplier_id` string, 
  `party_type` string, 
  `party` string, 
  `party_type_db` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:19 09:00:32.849
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SUPPLIER_INFO'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542629329')
CREATE EXTERNAL TABLE `supplier_info_address`(
  `address` string, 
  `supplier_id` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:12 13:53:16.296
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/SUPPLIER_INFO_ADDRESS'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542027692')
CREATE EXTERNAL TABLE `tax_item_qry`(
  `identity` string, 
  `party_type` string, 
  `party_type_db` string, 
  `invoice_id` string, 
  `item_id` string, 
  `tax_id` string, 
  `tax_percentage` string, 
  `tax_curr_amount` string, 
  `tax_dom_amount` string, 
  `fee_code` string, 
  `base_curr_amount` string, 
  `base_dom_amount` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 11:28:41.831
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/TAX_ITEM_QRY'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543143001')
CREATE EXTERNAL TABLE `tbl_order_dtls_view`(
  `order_no` string, 
  `order_dtls` string, 
  `status` string, 
  `initiated_by` string, 
  `last_udpatedby` string, 
  `ordered_date` bigint, 
  `last_updated_date` string, 
  `ifs_order_no` string, 
  `rejection_reason` string, 
  `cancellation_reason` string, 
  `inventory_status` string, 
  `distributor_id` string, 
  `total_ordered_amt` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:06 11:46:17.928
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/TBL_ORDER_DTLS_VIEW'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1541505129')
CREATE EXTERNAL TABLE `user_id`(
  `id` bigint, 
  `unique_id` string, 
  `conflict` string, 
  `description` string, 
  `active` string, 
  `blacklisted` string, 
  `u_part_key` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:19 09:46:16.563
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/USER_ID'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542618152')
CREATE EXTERNAL TABLE `ussd_error_code_breakdown`(
  `t_date` bigint, 
  `ussd_site` string, 
  `service_code` string, 
  `callrelease_status` string, 
  `callrelease_status_description` string, 
  `error_code` string, 
  `error_description` string, 
  `records` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:13 15:11:20.640
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/USSD_ERROR_CODE_BREAKDOWN'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542118677')
CREATE EXTERNAL TABLE `ussd_license_usage`(
  `statistic_time` bigint, 
  `ussd_site` string, 
  `total_tps` bigint, 
  `max_tps` bigint, 
  `min_tps` bigint, 
  `average_tps` double, 
  `total_number_of_discarded_requests` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:13 15:25:32.049
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/USSD_LICENSE_USAGE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542119490')
CREATE EXTERNAL TABLE `ussd_traffic_success_rate`(
  `statistics_time` bigint, 
  `ussd_site` string, 
  `service_code` string, 
  `call_times` string, 
  `success_times` string, 
  `fail_times` string, 
  `success_rate` string, 
  `success` string, 
  `ms_failure` string, 
  `abort_by_an_ms` string, 
  `abort_by_an_sp` string, 
  `timeout_for_an_sp_response` string, 
  `timeout_for_an_ms_response` string, 
  `transfer_failure` string, 
  `pps_user_authentication_failure` string, 
  `unknow_releasestatusdesc` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:13 15:01:35.674
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/USSD_TRAFFIC_SUCCESS_RATE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542118055')
CREATE EXTERNAL TABLE `vat_perc`(
  `vat_code_id` string, 
  `description` string, 
  `vat_percent` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint, 
  `event_timestamp_enrich` bigint, 
  `original_timestamp_enrich` string)
COMMENT '
  DDL generation date: 2018:11:25 12:07:57.037
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/VAT_PERC'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1543144632')
CREATE VIEW `vp_ng_agility_cb_pos_transactions` AS /* Presto View */
CREATE VIEW `vp_ng_agility_cb_retail_outlets` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_bank_guarantee` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_budget_period_amount` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_code_f` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_code_g` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_corporate_form` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_cust_ord_customer_ent` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_cust_order_type` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_credit_info` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_details` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_group` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_info_comm_method` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_info_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_order_history` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_order_line_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_customer_order_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_day_trans_detail` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_dba_role_privs` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_dba_tab_privs` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_fnd_user` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_fnd_user_role` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_gen_led_voucher` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_gen_led_voucher_row` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_history_log` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_history_log_attribute` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_identity_invoice_info` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_inventory_part_in_stock` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_inventory_transaction_hist` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_invoice` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_invoice_ledger_item_su_qry` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_ledger_item` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_ledger_item_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_man_supp_invoice` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_man_supp_invoice_item` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_mtn_cust_available_credit` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_payment_address_general` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_payment_plan_auth` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_payment_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_payment_term` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_price_list` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purch_req_approval` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_order` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_order_approval` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_order_hist` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_order_line` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_receipt_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_req_line` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_purchase_requisition_tab` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_return_material_line` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_sales_price_list_part` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_supplier_info` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_supplier_info_address` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_tax_item_qry` AS /* Presto View */
CREATE VIEW `vp_ng_ifs_vat_perc` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_basic_data` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_bfp_failure_log` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_blacklist_history` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_dealer_type` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_dynamic_data` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_enrollment_ref` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_km_user` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_km_user_role` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_kyc_dealer` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_master_data_view` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_meta_data` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_node` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_node_assignment` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_passport` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_sms_activation_request` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_state` AS /* Presto View */
CREATE VIEW `vp_ng_seamfix_user_id` AS /* Presto View */
CREATE VIEW `vp_ng_tpp_master_data_view` AS /* Presto View */
CREATE EXTERNAL TABLE `wsq_image`(
  `id` string, 
  `compression_ratio` string, 
  `finger` string, 
  `nfiq` string, 
  `reason_code` string, 
  `basic_data_fk` string, 
  `file_name` string, 
  `file_offset` bigint, 
  `kamanja_loaded_date` string, 
  `file_mod_date` string, 
  `date_key` int, 
  `msg_unique_id_enrich` bigint)
COMMENT '
  DDL generation date: 2018:11:21 09:13:10.641
'
PARTITIONED BY ( 
  `tbl_dt` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://ngdaas/user/mtn_user/kamanja_test/WSQ_IMAGE'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1542791516')
