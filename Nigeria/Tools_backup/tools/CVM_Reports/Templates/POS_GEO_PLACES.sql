start transaction;
insert into engine_room.POS_GEO_PLACES 
select distinct to_hex(sha1(to_utf8(distinct(coalesce(mfs.pos_msisdn,''))))),coalesce(b.cluster,''),cast('' as varchar),cast('' as varchar),cast('' as varchar),cast('' as varchar),cast('' as varchar),cast('' as varchar),cast('' as varchar),date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s'),mfs.tbl_dt from ( with pos_accounts as ( select tbl_dt,   from_profile_category profile_category ,  sender_account_id accountid,  cast(sender_msisdn as varchar) pos_msisdn   
from engine_room.MFS_TRANSACTIONS 
where tbl_dt = yyyymmddRunDate and from_profile_category  in ('agent' , 'merchant') 
union all 
select tbl_dt,  to_profile_category profile_category ,  receiver_account_id  accountid,  cast(receiver_msisdn as varchar)  pos_msisdn
from engine_room.MFS_TRANSACTIONS 
where tbl_dt = yyyymmddRunDate and to_profile_category  in ('agent' , 'merchant') ) select  distinct   tbl_dt , profile_category, accountid, pos_msisdn from pos_accounts ) mfs 
left join 
(select geo.msisdn_key,dc.cluster 
from nigeria.geography geo 
left outer join flare_8.vp_dim_cellsite dc 
on (geo.bts_mu_site_id=dc.cdr_cgi)
where geo.tbl_dt= yyyymmddRunDate and geo.aggr='daily' and geo.msisdn_key>0) b 
on (try_cast(mfs.pos_msisdn as bigint)=b.msisdn_key) 
union all 
select distinct to_hex(sha1(to_utf8(distinct(coalesce(evd.msisdn,''))))), coalesce(b.cluster,''), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), cast('' as varchar), date_parse(date_format(now(),'%Y%m%d %H%i%s'),'%Y%m%d %H%i%s'), evd.date_key from ( select coalesce (evd.receivermsisdn,'') as msisdn , tbl_dt as date_key 
from flare_8.ERS_VEND_NEW evd 
where tbl_dt= yyyymmddRunDate and case when upper(senderjuridicalname) in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile  in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not  in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_IN') and upper(evd.resultstatus)='SUCCESS' 
union all 
select coalesce (evd.sendermsisdn,'') as msisdn , tbl_dt as date_key 
from flare_8.ERS_VEND_NEW evd 
where tbl_dt= yyyymmddRunDate and case when upper(senderjuridicalname) in ('MTN') and transactiontype   in ('TRANSFER') and transactionprofile  in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype in ('TRANSFER') and transactionprofile not  in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_OUT') and upper(evd.resultstatus)='SUCCESS' 
union all 
select coalesce (evd.sendermsisdn,'') as msisdn , tbl_dt as date_key 
from flare_8.ERS_VEND_NEW evd 
where tbl_dt= yyyymmddRunDate and case when upper(senderjuridicalname) in ('MTN') and transactiontype   in ('TRANSFER') and transactionprofile in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype    in ('TRANSFER') and transactionprofile not in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') and upper(evd.resultstatus)='SUCCESS' 
union all 
select coalesce (evd.receivermsisdn,'') as msisdn , tbl_dt as date_key 
from flare_8.ERS_VEND_NEW evd 
where tbl_dt= yyyymmddRunDate 
and case when upper(senderjuridicalname) in ('MTN') and transactiontype   in ('TRANSFER') and transactionprofile  in ('IFS_TRANSFER') then 'SELL_IN' when upper(senderjuridicalname) not in ('MTN') and transactiontype    in ('TRANSFER') and transactionprofile not  in ('IFS_TRANSFER') then 'SELL_THROUGH' when transactiontype in ('TOPUP') then 'SELL_OUT' else 'OTHER' end in ('SELL_THROUGH') and upper(evd.resultstatus)='SUCCESS' ) as evd 
left join 
(select geo.msisdn_key,dc.cluster from 
nigeria.geography geo 
left outer join 
flare_8.vp_dim_cellsite dc 
on (geo.bts_mu_site_id=dc.cdr_cgi) 
where geo.tbl_dt= yyyymmddRunDate and geo.aggr='daily' and geo.msisdn_key>0) b on (try_cast(evd.msisdn as bigint)=b.msisdn_key )
;commit;
