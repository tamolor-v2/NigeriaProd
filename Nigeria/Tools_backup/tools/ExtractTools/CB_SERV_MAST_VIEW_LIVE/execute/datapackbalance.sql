start transaction;
delete from nigeria.datapackbalance where tbl_dt=20190108;
insert into nigeria.datapackbalance
select 
a.msisdn_key,
coalesce(SUM(cast(dedicatedaccountbalance as double)),0.0) data_balance,
SUM(CASE WHEN element_at(wallets.daid_map,try_cast(dedicatedaccountid as int)) = 'CHARGEABLE' THEN 
cast(dedicatedaccountbalance as double) ELSE 0.0 END) data_remaining_pack_balance,
a.tbl_dt
from flare_8.sdp_dmp_da a, 
(select map_agg(daid,datype) as daid_map from (
select 
try_cast(wallet_cd as int) daid,upper(wallet_type) datype,
rank() over (partition by wallet_cd order by isbiz desc) rnk from nigeria.wallets) where rnk=1) wallets
where a.tbl_dt=20190108
group by a.tbl_dt,a.msisdn_key;
commit;
