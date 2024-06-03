#!/bin/sh

exec scala -cp /nas/share05/tools/presto-jdbc-0.191.jar:/usr/hdp/current/hadoop-client/conf:/home/daasuser/FlareCluster/Flare/lib/system/ExtDependencyLibs_2.11-1.7.0.jar:/home/daasuser/FlareCluster/Flare/lib/system/ExtDependencyLibs2_2.11-1.7.0.jar -savecompiled "$0" "$@"
!#
import java.io.{File, _}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.sys.process.{ProcessLogger, _}
import scala.util.matching.Regex

object InsertionScript {
  var configFile: String = ""
  case class Feed(seq: Int, cols: String,baseFeed: String, condition: String)
  var schema_name = ""
  var dates = Array[Int]()
  var InsertedFeed = "" 
  var table_name = ""
  var base_table_name = ""
  var where_condition = ""
  var fieldList = ""
  val jDBCManager = new JDBCManager("jdbc:presto://10.1.197.145:8999/hive5/flare_8","daasuser",null)
  val InsertIntoTable =
  """ insert into {schema_name}.{table_name} select {field_List},{tbl_dt} from {base_table_name} where tbl_dt={tbl_dt} {where_condition}
  """
  val InsertIntoVoice = """ insert into {schema_name}.{table_name} (select * from (select {field_List},{tbl_dt} from {base_table_name} where a.tbl_dt= {tbl_dt} union all select {where_condition} where r.tbl_dt= {tbl_dt} and event_direction in ('TI','I') and bnum_operator = 'MTNN' ) s LEFT JOIN flare_8.dmc_dump_all c ON ((s.msisdn_key = cast(c.msisdn as bigint)) AND (s.tbl_dt = c.tbl_dt)))))
  """
  val InsertIntoSplit = """ insert into {schema_name}.{table_name} {field_List} , {tbl_dt} as tbl_dt from {base_table_name} where tbl_dt=(select max(tbl_dt) from nigeria.cis_catalogue where tbl_dt<={tbl_dt}) {where_condition} left outer join nigeria.vp_extravalue_split_tmp_test split on (cis.bundle_id=split.product_id)
  """
  val InsertIntoSIM = """ insert into {schema_name}.{table_name} {field_List} from  {base_table_name} where tbl_dt = {tbl_dt} and aggr ='daily' AND msisdn_key <> 0) lo on A.msisdn_key = lo.msisdn_key where a.tbl_dt = {tbl_dt}
  """ 
  val InsertPHY = """ insert into {schema_name}.{table_name} select {field_List} from {base_table_name} left join (select replace(cellglobalid,'-','') cell_id,origintransactionid, voucher_serial_nr, transaction_amt, msisdn_key, tbl_dt from flare_8.cs5_air_refill_ma where trim(voucher_serial_nr) <> '' and tbl_dt = {tbl_dt} ) location {where_condition} and ng.tbl_dt = {tbl_dt}
  """  

  val InsertIntoMFS_REG = """ insert into {schema_name}.{table_name} select {field_List} from (select  distinct split_part(split_part(identities,',',1),'(',2)  msisdn , case when recruited_type in ('EXT', 'USER', 'ID') then recruited_id else '' end agent_id , case when recruited_type in ('MSISDN') then recruited_id else '' end agent_msisdn , case when upper(status) in ('ACTIVE') then 'VALID' when upper(status) in ('BLOCKED','CLOSED','REGISTERED_BLOCKED') then 'INVALID' when upper(status) in ('REGISTERED','REGISTERED_CLOSED') then 'INCOMPLETE' else 'UNKNOWN' end registration_status , case when registration_date <> '' then date_format(from_iso8601_timestamp(registration_date ),'%Y%m%d%H%i%s') else '' end  registration_end_date ,aa.* , bb.* from  {base_table_name} where cast(date_format(from_iso8601_timestamp(original_timestamp_enrich ),'%Y%m%d') as int)   = {tbl_dt} {where_condition})
  """
  val InsertIntoVP = """ insert into {schema_name}.{table_name} select {field_List} from flare_8.{base_table_name} 
  """
  val InsertIntoGEO = """ insert into {schema_name}.{table_name} select {field_List} from ( with pos_accounts as ( select tbl_dt,   from_profile_category profile_category ,  sender_account_id accountid,  cast(sender_msisdn as varchar) pos_msisdn   from {schema_name}.{base_table_name} where tbl_dt = {tbl_dt} and from_profile_category  in ('agent' , 'merchant') union all select tbl_dt,  to_profile_category profile_category ,  receiver_account_id  accountid,  cast(receiver_msisdn as varchar)  pos_msisdn   from {schema_name}.{base_table_name} where tbl_dt = {tbl_dt} and to_profile_category  in ('agent' , 'merchant') ) {where_condition} ) mfs left join (select geo.msisdn_key,dc.cluster from nigeria.geography geo left outer join flare_8.vp_dim_cellsite dc on (geo.bts_mu_site_id=dc.cdr_cgi) where geo.tbl_dt= {tbl_dt} and geo.aggr='daily' and geo.msisdn_key>0) b on (try_cast(mfs.pos_msisdn as bigint)=b.msisdn_key) union all select distinct to_hex(sha1(to_utf8(distinct(coalesce(evd.msisdn,''))))), coalesce(b.cluster,''), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s'), evd.date_key from ( select coalesce (evd.receivermsisdn,'') as msisdn , tbl_dt as date_key from ERS_VEND_NEW evd where tbl_dt= {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile  in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not  in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_IN') and upper(evd.resultstatus)='SUCCESS' UNION ALL select coalesce (evd.sendermsisdn,'') as msisdn , tbl_dt as date_key from ERS_VEND_NEW evd where tbl_dt= {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype   in ('TRANSFER') and transactionprofile  in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not  in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_OUT') and upper(evd.resultstatus)='SUCCESS' UNION ALL select coalesce (evd.sendermsisdn,'') as msisdn , tbl_dt as date_key from ERS_VEND_NEW evd where tbl_dt= {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype   in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype    in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') and upper(evd.resultstatus)='SUCCESS' UNION ALL select coalesce (evd.receivermsisdn,'') as msisdn , tbl_dt as date_key from ERS_VEND_NEW evd where tbl_dt= {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype   in ('TRANSFER') and transactionprofile  in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype    in ('TRANSFER') and transactionprofile not  in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') and upper(evd.resultstatus)='SUCCESS' ) as evd left join (select geo.msisdn_key,dc.cluster from nigeria.geography geo left outer join vp_dim_cellsite dc on (geo.bts_mu_site_id=dc.cdr_cgi) where geo.tbl_dt= {tbl_dt} and geo.aggr='daily' and geo.msisdn_key>0) b on (try_cast(evd.msisdn as bigint)=b.msisdn_key ) 
  """

  val InsertIntoPOS = """ insert into {schema_name}.{table_name} select {field_List},tbl_dt from ( with pos_accounts as (select tbl_dt,   from_profile_category profile_category ,  sender_account_id accountid,  cast(sender_msisdn as varchar) pos_msisdn from engine_room.{base_table_name} where tbl_dt = {tbl_dt} and from_profile_category  in ('agent' , 'merchant') union all select tbl_dt,  to_profile_category profile_category ,  receiver_account_id  accountid,  cast(receiver_msisdn as varchar)  pos_msisdn from engine_room.{base_table_name} where tbl_dt = {tbl_dt} and to_profile_category  in ('agent' , 'merchant') ) {where_condition} ) union all select distinct to_hex(sha1(to_utf8(msisdn))) as msisdn_hash, msisdn, 'electronic_recharge' as product_type, date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s')  as update_datetime,tbl_dt from ( select coalesce (receivermsisdn,'') as msisdn , {tbl_dt} as tbl_dt from ERS_VEND_NEW where tbl_dt =  {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_IN') and upper(resultstatus)='SUCCESS' UNION ALL select coalesce (sendermsisdn,'') as msisdn ,  {tbl_dt} as tbl_dt from ERS_VEND_NEW where tbl_dt =  {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_OUT') and upper(resultstatus)='SUCCESS' UNION ALL select coalesce (sendermsisdn,'') as msisdn ,  {tbl_dt} as tbl_dt from ERS_VEND_NEW where tbl_dt =  {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') and upper(resultstatus)='SUCCESS' UNION ALL select coalesce (receivermsisdn,'') as msisdn ,  {tbl_dt} as tbl_dt from ERS_VEND_NEW where tbl_dt =  {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') and upper(resultstatus)='SUCCESS' )
  """  

  val InsertIntoMFS_ACC = """ insert into {schema_name}.{table_name} (date_key,account_type,accountid, msisdn,kamanja_loaded_date,tbl_dt) with mfs_active_accounts as ( select tbl_dt, kamanja_loaded_date, lower(from_profile_category) account_type , sender_account_id accountid, cast(sender_msisdn as varchar) msisdn from {schema_name}.{base_table_name} where tbl_dt = {tbl_dt}  and from_profile_category <> '' union all select tbl_dt,kamanja_loaded_date, lower(to_profile_category) account_type , receiver_account_id accountid, cast(receiver_msisdn as varchar) msisdn from {schema_name}.{base_table_name} where tbl_dt = {tbl_dt} {where_condition}) {field_List}
  """

  val InsertIntoEVD = """ insert into {schema_name}.{table_name} select case when transaction_type = 'SELL_IN' then to_hex(sha1(to_utf8((receiver_msisdn)))) when transaction_type = 'SELL_OUT' then to_hex(md5(to_utf8(receiver_msisdn))) when transaction_type = 'SELL_THROUGH' then to_hex(sha1(to_utf8((receiver_msisdn)))) end as receiver_msisdn,sender_msisdn,transaction_id,transaction_type,transaction_amount,cell_id,distributor_id ,stock_balance,date,channel,{tbl_dt} from (select {field_List},{tbl_dt} from flare_8.ers_vend_new where transactiontype in ('TRANSFER','TOPUP') and upper(resultstatus)='SUCCESS' and tbl_dt= {tbl_dt}) where {where_condition}
  """
  
  val InsertIntoDistributor = """ insert into {schema_name}.{table_name} select senderresellername,sendermsisdn,hashed_sender_msisdn,{tbl_dt} from ( select {field_List},{tbl_dt} from flare_8.{base_table_name} where senderresellertype = 'hostifdistributor' and channel = 'USSD' and tbl_dt={tbl_dt}) where {where_condition}
  """
  val InsertIntoMFS_STOCK = """ insert into {schema_name}.{table_name} ( select  {field_List} from ( with agent_max_timestamp as ( select distinct (agentmsisdn) agentmsisdn, max(time_stamp) time_stamp , max(kamanja_loaded_date) kamanja_loaded_date ,tbl_dt,profile from( select distinct(msisdn_key)agentmsisdn, max(original_timestamp_enrich) time_stamp ,tbl_dt, instruct_from_fro_user_prf profile, max(kamanja_loaded_date) kamanja_loaded_date from {base_table_name} where tbl_dt = {tbl_dt} and hdr_type = 'RESERVATION'and msisdn_key <>0 and substring(instruct_from_message, 1, 21) not in ('COMMISSIONLIQUIDATION') group by msisdn_key,tbl_dt,instruct_from_fro_user_prf union select distinct(try_cast(instruct_to_accnt_hldr_msisdn as bigint)) agentmsisdn, max(original_timestamp_enrich) time_stamp ,tbl_dt, instruct_to_fro_user_prf profile, max(kamanja_loaded_date) kamanja_loaded_date from {base_table_name} where tbl_dt ={tbl_dt}   and hdr_type = 'RESERVATION' and try_cast(instruct_to_accnt_hldr_msisdn as bigint) <>0 group by instruct_to_accnt_hldr_msisdn, tbl_dt,instruct_to_fro_user_prf )group by agentmsisdn, tbl_dt,profile ), agent_timestamp as (select * from ( select instruct_from_bal_tot_aft,  msisdn_key, instruct_from_fro_id account_id , original_timestamp_enrich  , kamanja_loaded_date, tbl_dt , row_number() OVER (PARTITION BY   original_timestamp_enrich   ORDER BY  instruct_hdr_fid  desc ) rnk from {base_table_name} where tbl_dt = {tbl_dt}  and msisdn_key <>0 and hdr_type ='RESERVATION' and status_code='EXECUTED'  and instruct_hdr_type not in ('COMMISSIONING') union all select instruct_to_bal_tot_aft,  try_cast(instruct_to_accnt_hldr_msisdn as bigint), instruct_to_fro_id account_id, original_timestamp_enrich  , kamanja_loaded_date ,tbl_dt, row_number() OVER (PARTITION BY   original_timestamp_enrich   ORDER BY  instruct_hdr_fid  desc ) rnk from {base_table_name} where tbl_dt = {tbl_dt} and status_code='EXECUTED'  and instruct_to_bal_tot_aft <>0 and instruct_from_accnt_hldr_msisdn in ('') and hdr_type = 'RESERVATION' and instruct_hdr_type in ('TRANSFER') ) where rnk = 1 ) select original_timestamp_enrich ,  agentmsisdn  , account_id  , wallet_type , profile_category , EODBalance ,kamanja_loaded_date ,  tbl_dt from {where_condition}
  """

  val InsertIntoStock = """ insert into {schema_name}.{table_name} (select {field_List} FROM ( select coalesce (receivermsisdn,'') as msisdn , coalesce(receiverbalancevalueafter ,0.0) as balancevalueafter , coalesce(original_timestamp_enrich ,'') as original_timestamp_enrich , kamanja_loaded_date , case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end as transaction_type , {tbl_dt} as tbl_dt from flare_8.ERS_VEND_NEW where tbl_dt = {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_IN') UNION ALL select coalesce (sendermsisdn,'') as msisdn , coalesce(senderbalancevalueafter ,0.0) as balancevalueafter , coalesce(original_timestamp_enrich ,'') as original_timestamp_enrich , kamanja_loaded_date , case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end as transaction_type , {tbl_dt} as tbl_dt from flare_8.ERS_VEND_NEW where tbl_dt = {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_OUT') UNION ALL select coalesce (sendermsisdn,'') as msisdn , coalesce(senderbalancevalueafter ,0.0) as balancevalueafter , coalesce(original_timestamp_enrich ,'') as original_timestamp_enrich , kamanja_loaded_date , case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end as transaction_type , {tbl_dt} as tbl_dt from flare_8.ERS_VEND_NEW where tbl_dt = {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') UNION ALL select coalesce (receivermsisdn,'') as msisdn , coalesce(receiverbalancevalueafter ,0.0) as balancevalueafter , coalesce(original_timestamp_enrich ,'') as original_timestamp_enrich , kamanja_loaded_date , case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end as transaction_type , {tbl_dt} as tbl_dt from flare_8.ERS_VEND_NEW where tbl_dt = {tbl_dt} and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') ))
  """
 val InsertIntoMFS_TRAN = """ insert into {schema_name}.{table_name} with commissions as (select distinct tbl_dt, instruct_hdr_fid, instruct_hdr_type,instruct_hdr_tid,instruct_amount from flare_8.financial_log b where instruct_hdr_type='COMMISSIONING' and b.tbl_dt = {tbl_dt} ), transactions as ( select date_time, STATUS_CODE,hdr_type, instruct_from_fro_user_prf , instruct_to_fro_user_prf , instruct_hdr_type , instruct_hdr_tid ,instruct_hdr_fid, instruct_amount, msisdn_key , tbl_dt ,instruct_from_fro_id , instruct_from_bal_tot_aft ,instruct_to_accnt_hldr_msisdn ,instruct_to_accnt_hldr_id ,instruct_to_bal_tot_aft , instruct_to_ifee , instruct_from_ifee , kamanja_loaded_date , engine_tx_type , instruct_from_message from ( select row_number() OVER (PARTITION BY a.instruct_hdr_tid, msisdn_key, instruct_amount, date_format(from_iso8601_timestamp(a.original_timestamp_enrich ),'%Y%m%d%H%i%s') ORDER BY a.instruct_hdr_tid DESC) rnk, date_format(from_iso8601_timestamp(a.original_timestamp_enrich ),'%Y%m%d%H%i%s') date_time , hdr_type, STATUS_CODE, instruct_from_fro_user_prf,instruct_to_fro_user_prf, instruct_hdr_type,instruct_hdr_tid,instruct_hdr_fid, instruct_amount,msisdn_key,tbl_dt,instruct_from_fro_id, instruct_from_bal_tot_aft,instruct_to_accnt_hldr_msisdn, instruct_to_accnt_hldr_id, instruct_to_bal_tot_aft, instruct_to_ifee, instruct_from_ifee, kamanja_loaded_date ,  case when instruct_hdr_type='CASH_IN' and instruct_from_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'CASH_IN'  when instruct_hdr_type='CASH_OUT' and instruct_from_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'CASH_OUT'  when instruct_hdr_type in ('PAYMENT_SEND','PAYMENT' ) and instruct_to_accnt_hldr_usr_prf in ('YDFS Service Provider Profile', 'Star Times Service Provider Profile') then 'BILL_PAYMENT' when instruct_hdr_type in ('PAYMENT_SEND' ) and instruct_to_accnt_hldr_usr_prf like '%WATU%' then 'BILL_PAYMENT'  when instruct_hdr_type='FLOAT_TRANSFER' and instruct_to_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'FLOAT_CREDIT_TRANSFER'  when instruct_hdr_type='FLOAT_TRANSFER' and instruct_from_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'FLOAT_DEBIT_TRANSFER'  when instruct_hdr_type in ('PAYMENT') and instruct_to_accnt_hldr_usr_name in ('AccessBank-External.sp') and hdr_type = 'RESERVATION' then 'ASSISTED_DEPOSITS'  when instruct_hdr_type = 'PAYMENT' and instruct_to_accnt_hldr_usr_name in ('AccessBank.sp') and hdr_type = 'RESERVATION' then 'PUSH'  when instruct_hdr_type in ('PAYMENT','TRANSFER' ) and instruct_from_accnt_hldr_usr_name in ('AccessBank-NIP.sp') and hdr_type = 'RESERVATION' then 'INWARD-NIP'  when instruct_hdr_type = 'TRANSFER' and instruct_from_accnt_hldr_usr_name in ('AccessBank.sp') and hdr_type = 'RESERVATION' then 'PULL'  when instruct_hdr_type = 'DEPOSIT' and hdr_type = 'RESERVATION' then 'DEPOSIT'  when instruct_hdr_type in ( 'ADJUSTMENT','PAYMENT','TRANSFER') and instruct_from_accnt_hldr_usr_name in ('AccessBank-External.sp') and hdr_type = 'RESERVATION' then 'ASSISTED_WITHWALS'  when instruct_hdr_type = 'TRANSFER_TO_VOUCHER' and instruct_from_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'BULK_TOKEN_CREATION'  when instruct_hdr_type = 'PAYMENT' and instruct_to_accnt_hldr_usr_name in ('airuser.sp','buyairtime') and instruct_to_accnt_hldr_usr_prf in ('MTN Airtime Service Provider Account Holder') and hdr_type = 'RESERVATION' then 'AIRTIME_PURCHASES'  when instruct_hdr_type = 'PAYMENT' and instruct_to_accnt_hldr_usr_name in ('cisuser.sp','dyabundle') and instruct_to_accnt_hldr_usr_prf in ('YDFS Data Bundle Service Provider Account New', 'DYA Data Bundle Service Provider Account New') and hdr_type = 'RESERVATION' then 'DATA_BUNDLE_PURCHASES'  when instruct_hdr_type='CREATE_CASH_VOUCHER' and instruct_from_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'TOKEN_GENERATE_VOUCHER'  when instruct_hdr_type='REDEEM_CASH_VOUCHER' and instruct_from_accnt_hldr_usr_prf not in ('Special Account Holder Profile', 'Youth Account Holder Profile', 'Mass Market Account Holder Profile', 'Fully Banked Account Holder Profile', 'Semi Banked Account Holder Profile') and hdr_type = 'RESERVATION' then 'TOKEN_REDEEM_VOUCHER'  when instruct_hdr_type='TRANSFER_FROM_VOUCHER' and hdr_type = 'RESERVATION' then 'BULK_TOKEN_REDEEM' else 'defaulted'||'-'|| instruct_hdr_type end engine_tx_type ,instruct_from_message from flare_8.financial_log a where a.STATUS_CODE ='EXECUTED' and instruct_hdr_fid in (select instruct_hdr_fid from flare_8.financial_log where hdr_type = 'TXN' and status_code = 'COMMITTED' and tbl_dt = {tbl_dt} ) and a.instruct_hdr_type not in ( 'COMMISSIONING') and ( instruct_from_bal_tot_bfr <> instruct_from_bal_tot_aft or instruct_to_bal_tot_bfr <> instruct_to_bal_tot_aft) and tbl_dt = {tbl_dt} ) ) select {field_List} from {base_table_name} where tbl_dt = {tbl_dt} and aggr ='daily' AND msisdn_key <> 0) loa on ( aa.msisdn_key = loa.msisdn_key) left join ( select cast(msisdn_key as varchar) msisdn_key ,bts_mu_site_id FROM nigeria.geography where tbl_dt = {tbl_dt} {where_condition}    """

  def main(args: Array[String]): Unit = {
    Log.logMsg("Started")
    parsArgs(args)
    if (dates.isEmpty) Log.logError("No date is supplied", true)
    val feedsList = parseMetadata(configFile)
    println(feedsList)
    dates.foreach(date => {
      runForDay(date, feedsList)
    })
  }
  
   def runForDay(date: Int, feedsList: Map[String, Feed]): Unit = {
    Log.logMsg("Started for day:" + date)
    val (feed, data) = getFeedInfo(feedsList)
    val columns = data.cols 
    fieldList = columns 
    base_table_name = data.baseFeed
    where_condition = data.condition
    if (date == 0) Log.logError("No date is supplied", true)
    if (feed == "" )
    Log.logError("Feed couldn't be found in config file", true)
    else 
     {
   	  Log.logMsg(feed + " Feed Metadata found")
          if (feed == "CELLS") populatePartition(feed,InsertIntoVP, date)
	   else if (feed == "POS_GEO_PLACES") populatePartition(feed,InsertIntoGEO, date)
	   else if (feed == "CDR_VOICE") populatePartition(feed,InsertIntoVoice, date)
	   else if (feed == "EVD_TRANSACTIONS") populatePartition(feed,InsertIntoEVD,date)
	   else if (feed == "POS_MSISDN") populatePartition(feed,InsertIntoPOS,date)
	   else if (feed == "MSISDN_DISTRIBUTOR") populatePartition(feed,InsertIntoDistributor,date)
	   else if (feed == "EVD_STOCK_LEVEL") populatePartition(feed,InsertIntoStock,date)
	   else if (feed == "MFS_REGISTRATIONS") populatePartition(feed,InsertIntoMFS_REG,date)
  	   else if (feed == "MFS_TRANSACTIONS") populatePartition(feed,InsertIntoMFS_TRAN,date)
  	   else if (feed == "BUNDLE_SPLIT") populatePartition(feed,InsertIntoSplit,date)
           else if (feed == "MFS_ACCOUNTS") populatePartition(feed,InsertIntoMFS_ACC,date)
	   else if (feed == "MFS_STOCK_LEVEL") populatePartition(feed,InsertIntoMFS_STOCK,date)
	   else if (feed == "PHYSICAL_RECHARGES") populatePartition(feed,InsertPHY,date)
	   else if (feed == "SIM_TRANSACTIONS") populatePartition(feed,InsertIntoSIM,date)
           else populatePartition(feed,InsertIntoTable, date)
     }
}
  
  def extractColumnsNames(feed_name: String) = {
    var (exitValue, stdoutStream, stderrStream) = runCommand(
      Seq("/opt/presto/bin/presto",
          "--server",
          "10.1.197.145:8999",
          "--catalog",
          "hive5",
          "--execute",
          "show columns from " + feed_name))
    val columns = stdoutStream
      .split("\n")
      .map(a => a.split(",")(0).replace("\"", ""))
      .toList
    columns

  }

  def getFeedInfo(feedList: Map[String, Feed]): (String, Feed) = {
    val feed = feedList.getOrElse(InsertedFeed, new Feed(0, "","",""))
    println(feed + " found")
    return (InsertedFeed, feed)
  }

   def populatePartition(feed: String, sqlQuery: String, date: Int) = {
    val query = sqlQuery
      .replace("""{schema_name}""", schema_name)
      .replace("""{table_name}""", feed)
      .replace("""{tbl_dt}""", date.toString)
      .replace("""{base_table_name}""", base_table_name)
      .replace("""{where_condition}""", where_condition)
      .replace("""{field_List}""", fieldList)
    Log.logMsg("Executing Query: " + query)
    jDBCManager.executeQuery(query)
  }
  
   def runCommand(cmd: Seq[String]): (Int, String, String) = {
    val stdoutStream = new ByteArrayOutputStream
    val stderrStream = new ByteArrayOutputStream
    val stdoutWriter = new PrintWriter(stdoutStream)
    val stderrWriter = new PrintWriter(stderrStream)
    val exitValue =
      cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
    stdoutWriter.close()
    stderrWriter.close()
    (exitValue, stdoutStream.toString, stderrStream.toString)
  } 
 
   def parsArgs = (args: Array[String]) => {
    if (args.length >= 2)
      args.sliding(2, 2).toList.collect {
        case Array("--configFile", argDataDate: String) =>
          configFile = argDataDate
        case Array("--feed", argDataDate: String) => InsertedFeed = argDataDate
        case Array("--dates", argDataDates: String) => dates = argDataDates.split(",").map(s => s.trim).filter(s => s.nonEmpty).map(s => s.toInt)
      } else
      Log.logError("No config file is supplied", true)
    if (InsertedFeed.trim == "")
      Log.logError("Please provide a feed ", true)
  }
 
    def parseMetadata(path: String): Map[String, Feed] = {
    var FeedMetadata = new ListBuffer[Feed]()
    var FeedMetadataMap = Map[String, Feed]()
    val config =
      ConfigFactory.parseFile(new File(path)).getConfig("Insertion.LoadCluster")
    val clusterName = config.getString("ClusterName")
    schema_name = config.getString("Schema")
    val allFeeds = config.getConfigList("Feeds")
    allFeeds.foreach(feed => {
      val cols = if (feed.hasPath("cols")) feed.getString("cols") else ""
      val seq = if (feed.hasPath("seq")) feed.getInt("seq") else 0
      val baseName = if (feed.hasPath("base_feed")) feed.getString("base_feed") else ""
      val feedName = if (feed.hasPath("feed")) feed.getString("feed") else ""
      val whereClause = if (feed.hasPath("cond")) feed.getString("cond") else ""
      val feedObj = Feed(seq, cols, baseName, whereClause)
      FeedMetadata += feedObj
      FeedMetadataMap += ((feedName, feedObj))
    })

    FeedMetadataMap
  }
} 
  class JDBCManager(private val url: String,
                  private val user: String,
                  private val pass: String) {
  private val conn: Connection =
    DriverManager.getConnection(this.url, this.user, this.pass)
  private val maxRetry = 1  
 
  def executeSelectQueryOnce(query: String): String = {
    var statement: Statement = null
    var resultSet: ResultSet = null
    if (conn != null && !conn.isClosed) {
      Log.logMsg("generating table statement ")
      statement = conn.createStatement()
      Log.logMsg("executing statement")
      val result = statement.executeQuery(query)
      val cnt = if (result.next) result.getString(1) else ""
      return cnt
    }
    return ""
  }
  
 def executeQuery(query: String): String = {
    var statement: Statement = null
    var retry = 0
    var resultSet: ResultSet = null
    var result = 1
    var status = "Start"
    while (retry < maxRetry) {
      try {
        if (conn != null && !conn.isClosed) {
          Log.logMsg("generating table statement ")
          statement = conn.createStatement()
          Log.logMsg("executing statement")
          result = statement.executeUpdate(query)
          return "success"
        }
      } catch {
        case ex: Throwable => {
          retry = retry + 1
          Log.logMsg("Exception while executing query: %s ".format(query))
          Log.logMsg("Retry: %2d/%2d".format(retry, maxRetry))
          ex.printStackTrace
        }
      }
    }
    status = "failed"
    status
  }

}
object Log {

  val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def logMsg(msg: String): Unit = {
    val time = LocalDateTime.now().format(formatter)
    println(time + " - " + msg)
  }

  def logError(msg: String,
               exit: Boolean,
               throwable: Throwable = null): Unit = {
    val time = LocalDateTime.now().format(formatter)
    System.err.println(time + " - %s".format(msg))
    if (throwable != null) {
      System.err.println(getsearchString(throwable))
    }
    if (exit) {
      System.exit(-1)
    }
  }

  private def getsearchString(throwable: Throwable): String = {
    val sw = new StringWriter
    throwable.printStackTrace(new PrintWriter(sw))
    val exceptionAsString = sw.toString
    sw.close
    exceptionAsString
  }
}

InsertionScript.main(args)
