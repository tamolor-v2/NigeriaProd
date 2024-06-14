start transaction;
insert into engine_room.MSISDN_DISTRIBUTOR
select senderresellername,sendermsisdn,hashed_sender_msisdn,yyyymmddRunDate 
from 
( select distinct senderresellername as senderresellername,sendermsisdn,to_hex(sha1(to_utf8((coalesce (sendermsisdn,'') )))) hashed_sender_msisdn,case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER')  then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end as transaction_type,yyyymmddRunDate 
from flare_8.ers_vend_new 
where senderresellertype = 'hostifdistributor' and channel = 'USSD' and tbl_dt=yyyymmddRunDate) 
where transaction_type in ('SELL_IN')
;commit;
