start transaction;
insert into engine_room.EVD_TRANSACTIONS 
select case when transaction_type = 'SELL_IN' then to_hex(sha1(to_utf8((receiver_msisdn)))) when transaction_type = 'SELL_OUT' then to_hex(md5(to_utf8(receiver_msisdn))) when transaction_type = 'SELL_THROUGH' then to_hex(sha1(to_utf8((receiver_msisdn)))) end as receiver_msisdn,sender_msisdn,transaction_id,transaction_type,transaction_amount,cell_id,distributor_id ,stock_balance,date,channel,yyyymmddRunDate 
from 
(select ersreference transaction_id , regexp_replace( original_timestamp_enrich, '[^0-9]+', '')   date , to_hex(sha1(to_utf8((coalesce (sendermsisdn,''))))) sender_msisdn , coalesce (receivermsisdn,'') receiver_msisdn , case when upper(senderjuridicalname) in ('MTN')  and transactiontype in ('TRANSFER')  and transactionprofile in ('IFS_TRANSFER')   then  'SELL_IN' when upper(senderjuridicalname) not in ('MTN')  and transactiontype in ('TRANSFER')  and transactionprofile not in ('IFS_TRANSFER')   then  'SELL_THROUGH' when  transactiontype in ('TOPUP') and upper(channel) in ('USSD')  then  'SELL_OUT' else 'OTHER' end transaction_type , receiveramountvalue transaction_amount , receivercellid  cell_id , senderjuridicalname distributor_id , senderbalancevaluebefore stock_balance,channel,yyyymmddRunDate 
from flare_8.ers_vend_new 
where transactiontype in ('TRANSFER','TOPUP') and upper(resultstatus)='SUCCESS' and tbl_dt= yyyymmddRunDate) where transaction_type not in ('OTHER')
;commit;
