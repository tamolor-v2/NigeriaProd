start transaction;
drop view CVM_DB.CVM20_VW_REFILL_AND_DATA_BUNDLE_UAT;
create view cvm_db.cvm20_vw_refill_and_data_bundle_uat as
with refil_and_data_bundle as
( 
	select 
		 tbl_dt,date_key,
		 date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') weekid,
		 try_cast(substring(CAST(tbl_dt AS varchar(10)), 7,8) as int)  dayid,
		 msisdn_key,sub_fee totalcharge_money,product_name,channel, upper(substring(day,1,3)) dow, bundle_type
	from (
		select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'DATA' bundle_type
		from nigeria.vng_prepaid_data_bundle   
		union all
		select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'SOCIAL' bundle_type
		from nigeria.vng_daily_social_bundle   
		union all
		select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day , 'COMBO' bundle_type
		from nigeria.vng_xtral_value_bundle    
		union all
		select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day  , 'DATA' bundle_type
		from nigeria.vng_postpaid_data_bundle   
		union all
		select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day  , 'VOICE' bundle_type
		from nigeria.vng_postpaid_voice_bundle   
		union all
		select tbl_dt,date_key,msisdn_key,sub_fee,product_name,channel,day  , 'VOICE' bundle_type
		from nigeria.vng_roaming_bundle    
	) 
)
select * from refil_and_data_bundle;
drop view CVM_DB.CVM20_VW_REFILL_AND_DATA_BUNDLE_TMP1;
create view cvm_db.cvm20_vw_refill_and_data_bundle_tmp1 as 
select 
	msisdn_key, weekid, 
	sum(case when dow = 'MON' then totalcharge_money else 0 end)  mon_sbsc_amt , 
	sum(case when dow = 'TUE' then totalcharge_money else 0 end) tue_sbsc_amt,
	sum(case when  dow = 'WED' then totalcharge_money else 0 end) wed_sbsc_amt,
	sum(case when  dow = 'THU' then totalcharge_money else 0 end) thu_sbsc_amt,
	sum(case when  dow = 'FRI' then totalcharge_money else 0 end) fri_sbsc_amt,
	sum(case when  dow = 'SAT' then totalcharge_money else 0 end) sat_sbsc_amt,
	sum(case when  dow = 'SUN' then totalcharge_money else 0 end) sun_sbsc_amt,
	sum(case when  dow = 'MON' then 1 else 0 end) mon_sbsc_cnt,
	sum(case when  dow = 'TUE' then 1 else 0 end) tue_sbsc_cnt,
	sum(case when  dow = 'WED' then 1 else 0 end) wed_sbsc_cnt,
	sum(case when  dow = 'THU' then 1 else 0 end) thu_sbsc_cnt,
	sum(case when  dow = 'FRI' then 1 else 0 end) fri_sbsc_cnt,
	sum(case when  dow = 'SAT' then 1 else 0 end) sat_sbsc_cnt,
	sum(case when  dow = 'SUN' then 1 else 0 end) sun_sbsc_cnt,
	sum(totalcharge_money) tot_sbsc_amt , 
	count(*) total_bundle_count ,
	sum(case when upper(channel) in ('FB') then totalcharge_money else 0 end) sbsc_FB_amt ,
	sum(case when upper(channel) in ('FB') then 1 else 0 end) sbsc_count_FB ,  
	sum(case when upper(channel) in ('USSD') then 1 else 0 end) sbsc_count_USSD,
	sum(case when upper(channel) in ('USSD') then totalcharge_money else 0 end) sbsc_USSD_amt, 
	sum(case when upper(channel) in ('RS') then 1 else 0 end) sbsc_count_RS,
	sum(case when upper(channel) in ('RS') then totalcharge_money else 0 end) sbsc_RS_amt,
	sum(case when upper(channel) not in ('FB','USSD','RS') then totalcharge_money else 0 end) sbsc_Others_amt,
	sum(case when upper(channel) not in ('FB','USSD','RS') then 1 else 0 end) sbsc_count_Others,
	sum(case when bundle_type = 'DATA' then 1 else 0 end) total_data_bundle_count,  
	sum(case when bundle_type = 'COMBO' then 1 else 0 end) total_combo_bundle_count,   
	sum(case when channel in ('EVD','DYA','VTU') then 1 else 0 end )  sbsc_count_voucher, 
	sum(case when channel in ('EVD','DYA','VTU') then totalcharge_money else 0 end )  sbsc_voucher_amt, 
	sum(case when (channel in ('SMS') or channel like 'SP%SMS%') then 1 else 0 end )  total_sms_bundle_count, 
	sum(case when bundle_type = 'VOICE' then 1 else 0 end)  total_voice_bundle_count, 
	sum(case when channel in ('USSD','SMARTAPP','SP Smart APP','MTNONLINE') then 1 else 0 end )  total_vas_bundle_count,  
	max(case when channel in ('EVD','DYA','VTU') then totalcharge_money else 0 end )  max_rch_deno,  
	max(totalcharge_money) max_sbsc_deno,  
	sum(case when upper(channel) like '%OTHER%' then 1 else 0 end ) total_other_bundle_count 
from cvm_db.cvm20_vw_refill_and_data_bundle_uat a
where 
     (date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') = 
     date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v'))  
group by msisdn_key, weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_RCH_GAP_TMP2;
create view cvm_db.cvm20_rbs_average_rch_gap_tmp2 as 
select 
	msisdn_key, weekid,
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0
	end avg_rchg_gap
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel in ('EVD','DYA','VTU') 
	  and (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = 
	       try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by 	date_key,dayid, channel, bundle_type
)
group by msisdn_key ,weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_SBSC_GAP_TMP1;
create view cvm_db.cvm20_rbs_average_sbsc_gap_tmp1 as 
select 
	msisdn_key, weekid,
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0
	end average_sbsc_gap
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel not in ('EVD','DYA','VTU')  and 
	    (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = 
	     try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by msisdn_key, date_key,dayid, channel, bundle_type
)
group by msisdn_key, weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_SBSC_GAP_VOICE_TMP1;
create view cvm_db.cvm20_rbs_average_sbsc_gap_voice_tmp1 as 
select 
	msisdn_key, weekid,
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0
	end average_sbsc_gap_voice
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel not in ('EVD','DYA','VTU') and bundle_type = 'VOICE' and
	    (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by msisdn_key, date_key,dayid, channel, bundle_type
)
group by msisdn_key,weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_SBSC_GAP_DATA_TMP1;
create view cvm_db.cvm20_rbs_average_sbsc_gap_data_tmp1 as 
select 
	msisdn_key,	weekid,
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0
	end average_sbsc_gap_data
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel not in ('EVD','DYA','VTU') and 
	     bundle_type = 'DATA' and
	    (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by msisdn_key, date_key,dayid, channel, bundle_type
)
group by msisdn_key,weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_SBSC_GAP_SMS_TMP1;
create view cvm_db.cvm20_rbs_average_sbsc_gap_sms_tmp1 as 
select 
	msisdn_key,	weekid, 
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0
	end average_sbsc_gap_sms
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel in ('SMS')  and  
	    (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by msisdn_key, date_key,dayid, channel, bundle_type
)
group by msisdn_key,weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_SBSC_GAP_COMBO_TMP1;
create view cvm_db.cvm20_rbs_average_sbsc_gap_combo_tmp1 as 
select 
	msisdn_key,	weekid,
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0
	end average_sbsc_gap_combo
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel not in ('EVD','DYA','VTU') and   bundle_type = 'COMBO' and
		 (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by msisdn_key, date_key,dayid, channel, bundle_type
)
group by msisdn_key,weekid;
drop view CVM_DB.CVM20_RBS_AVERAGE_SBSC_GAP_VAS_TMP1;
create view cvm_db.cvm20_rbs_average_sbsc_gap_vas_tmp1 as 
select 
	 msisdn_key,	weekid,
	case 
		when (sum(case when rchg_diff > 0 then 1 else 0 end)) > 0  
		then sum(rchg_diff) / sum(case when rchg_diff > 0 then 1 else 0 end)
		else 0 end average_sbsc_gap_vas
from (
	select 
		msisdn_key, date_key, dayid, channel, bundle_type,
		date_format(DATE_TRUNC('week', date_parse(CAST(yyyymmddRunDate AS varchar(10)), '%Y%m%d')),'%v') weekid,
		dayid - LAG(dayid) OVER (PARTITION BY msisdn_key ORDER BY date_key ) rchg_diff
	from cvm_db.cvm20_vw_refill_and_data_bundle_uat a    
	where channel in ('USSD','SMARTAPP','SP Smart APP','MTNONLINE')  and 
	    (try_cast(substring(CAST(date_key AS varchar(8)),1,6) as bigint) = try_cast(substring(CAST(yyyymmddRunDate AS varchar(8)),1,6) as bigint))
	order by msisdn_key, date_key,dayid, channel, bundle_type
)
group by msisdn_key, weekid;
drop view CVM_DB.CVM20_REFILL_AND_DATA_BUNDLE_UAT_TMP2;
create view cvm_db.cvm20_refill_and_data_bundle_uat_tmp2 as 
select 
	coalesce(a.msisdn_key ,b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key,f.msisdn_key,g.msisdn_key) msisdn_key,
	coalesce(a.weekid ,b.weekid,c.weekid,d.weekid,e.weekid,f.weekid,g.weekid,h.weekid) weekid, 
	a.mon_sbsc_amt,a.tue_sbsc_amt,a.wed_sbsc_amt,a.thu_sbsc_amt,a.fri_sbsc_amt,a.sat_sbsc_amt,a.sun_sbsc_amt,
	a.mon_sbsc_cnt,a.tue_sbsc_cnt,a.wed_sbsc_cnt,a.thu_sbsc_cnt,a.fri_sbsc_cnt,a.sat_sbsc_cnt,a.sun_sbsc_cnt,
	a.tot_sbsc_amt,a.total_bundle_count,a.sbsc_FB_amt,a.sbsc_count_FB,a.sbsc_count_USSD,a.sbsc_USSD_amt,
	a.sbsc_count_RS,a.sbsc_RS_amt,a.sbsc_Others_amt,a.sbsc_count_Others,a.total_data_bundle_count,
	a.total_combo_bundle_count,a.sbsc_count_voucher,a.sbsc_voucher_amt,a.total_sms_bundle_count,
	a.total_voice_bundle_count,a.total_vas_bundle_count,a.max_rch_deno,a.max_sbsc_deno,
	a.total_other_bundle_count,
	b.avg_rchg_gap,
	c.average_sbsc_gap,d.average_sbsc_gap_voice,e.average_sbsc_gap_data,
	f.average_sbsc_gap_sms,g.average_sbsc_gap_combo,h.average_sbsc_gap_vas
	from cvm_db.cvm20_vw_refill_and_data_bundle_tmp1 a
    full join cvm_db.cvm20_rbs_average_rch_gap_tmp2 b
    on a.msisdn_key = b.msisdn_key    
    full join cvm_db.cvm20_rbs_average_sbsc_gap_tmp1 c
    on coalesce(a.msisdn_key, b.msisdn_key)  = c.msisdn_key
	full join cvm_db.cvm20_rbs_average_sbsc_gap_voice_tmp1 d
	on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key) = d.msisdn_key
	full join cvm_db.cvm20_rbs_average_sbsc_gap_data_tmp1 e
	on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key) = e.msisdn_key
	full join cvm_db.cvm20_rbs_average_sbsc_gap_sms_tmp1 f
	on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key) = f.msisdn_key
	full join cvm_db.cvm20_rbs_average_sbsc_gap_combo_tmp1 g
	on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key,f.msisdn_key) = g.msisdn_key
	full join cvm_db.cvm20_rbs_average_sbsc_gap_vas_tmp1 h
	on coalesce(a.msisdn_key, b.msisdn_key,c.msisdn_key,d.msisdn_key,e.msisdn_key,f.msisdn_key,g.msisdn_key) = h.msisdn_key;
commit;
