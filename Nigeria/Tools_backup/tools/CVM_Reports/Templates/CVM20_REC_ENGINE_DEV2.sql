start transaction;
insert into cvm_db.CVM20_REC_ENGINE_DEV2 
 select
 msisdn_key
 ,max(case when attribute = 'last_date_vce_offnet' then try_cast(attribute_value as bigint) else null end) last_date_vce_offnet
 ,max(case when attribute = 'last_date_vce_onnet'   then try_cast(attribute_value as bigint) else null end) last_date_vce_onnet
 ,max(case when attribute = 'last_date_vce_int' then try_cast(attribute_value as bigint) else null end) last_date_vce_int
 ,max(case when attribute = 'last_date_vce_roam'  then try_cast(attribute_value as bigint) else null end) last_date_vce_roam
 ,max(case when attribute = 'last_date_sms_offnet' then try_cast(attribute_value as bigint) else null end) last_date_sms_offnet
 ,max(case when attribute = 'last_date_sms_onnet'   then try_cast(attribute_value as bigint) else null end) last_date_sms_onnet
 ,max(case when attribute = 'last_date_sms_int' then try_cast(attribute_value as bigint) else null end) last_date_sms_int
 ,max(case when attribute = 'last_date_sms_roam'   then try_cast(attribute_value as bigint) else null end) last_date_sms_roam
 ,max(case when attribute = 'last_data_date'    then try_cast(attribute_value as bigint) else null end) last_data_date
 ,max(case when attribute = 'last_data_date_roam'   then try_cast(attribute_value as bigint) else null end) last_data_date_roam
 ,max(case when attribute = 'ds_last_sms_in' then try_cast(attribute_value as bigint) else null end) ds_last_sms_in
 ,max(case when attribute = 'ds_last_sms_out'  then try_cast(attribute_value as bigint) else null end) ds_last_sms_out
 ,max(case when attribute = 'ds_last_sms_out'  then try_cast(attribute_value as bigint) else null end) ds_last_sms_rge
 ,max(case when attribute = 'ds_last_voi_in' then try_cast(attribute_value as bigint) else null end) ds_last_voi_in
 ,max(case when attribute = 'ds_last_voi_out'  then try_cast(attribute_value as bigint) else null end) ds_last_voi_out
 ,max(case when attribute = 'ds_last_voi_out'  then try_cast(attribute_value as bigint) else null end) ds_last_voi_rge
 ,max(case when attribute = 'ds_last_gpr_in' then try_cast(attribute_value as bigint) else null end) ds_last_data_in
 ,max(case when attribute = 'ds_last_data_out'   then try_cast(attribute_value as bigint) else null end) ds_last_data_out
 ,max(case when attribute = 'ds_last_data_rge'  then try_cast(attribute_value as bigint) else null end) ds_last_data_rge
 ,max(case when regexp_like(upper(attribute), 'SEND') then try_cast(attribute_value as bigint) else null end) ds_last_airtime_transfer
 ,max(case when regexp_like(upper(attribute), 'RECEIVE')   then try_cast(attribute_value as bigint) else null end) ds_last_airtime_receive
 ,max(case when regexp_like(upper(attribute), 'CRBT|TUNE') then try_cast(attribute_value as bigint) else null end) ds_last_digital_crbt
 ,max(case when regexp_like(upper(attribute), 'CALLER') then try_cast(attribute_value as bigint) else null end) ds_last_digital_Callerfeel
 ,max(case when regexp_like(upper(attribute), 'PHONE') then try_cast(attribute_value as bigint) else null end) ds_last_digital_PhonebookBackup
 ,max(case when attribute = 'ds_last_digital' then try_cast(attribute_value as bigint) else null end) ds_last_digital
 ,max(case when attribute = 'last_digital_rge_dt'  then try_cast(attribute_value as bigint) else null end) last_digital_rge_dt
 ,max(case when attribute = 'last_rge_date' then try_cast(attribute_value as bigint) else null end) last_rge_date
 ,max(case when attribute = 'last_rge_date'  then try_cast(attribute_value as bigint) else null end) last_activity_date
 ,max(case when attribute = 'ds_digital_services_act'  then try_cast(attribute_value as bigint) else null end) ds_digital_services_act
 ,max(case when attribute = 'ds_last_recharge_sbsc' then try_cast(attribute_value as bigint) else null end) ds_last_recharge_sbsc
 ,max(case when attribute = 'ds_last_recharge_sbsc_mymtn'     then try_cast(attribute_value as bigint) else null end) ds_last_recharge_sbsc_mymtn
 ,max(case when attribute = 'ds_last_recharge_sbsc_digital'   then try_cast(attribute_value as bigint) else null end) ds_last_recharge_sbsc_digital
 ,max(case when attribute = 'ds_ayoba_act' then try_cast(attribute_value as bigint) else null end) ds_ayoba_act
 ,max(case when attribute = 'ds_last_momo_rge'  then try_cast(attribute_value as bigint) else null end) ds_last_momo_rge
 ,max(case when attribute = 'ds_momo_act'     then try_cast(attribute_value as bigint) else null end) ds_momo_act
 ,max(case when attribute = 'ds_last_momo_rge'  then try_cast(attribute_value as bigint) else null end) ds_last_momo
 ,max(case when attribute = 'dola'  then try_cast(attribute_value as bigint) else null end) dola
 ,sum(case when attribute = 'act_days_voi_onnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_onnet_moc
 ,sum(case when attribute = 'act_days_voi_offnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_offnet_moc
 ,sum(case when attribute = 'act_days_voi_intl_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_intl_moc
 ,sum(case when attribute = 'act_days_voi_roam_moc'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_roam_moc
 ,sum(case when attribute = 'act_days_voi_all_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_all_moc
 ,sum(case when attribute = 'act_days_voi_onnet_mtc'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_onnet_mtc
 ,sum(case when attribute = 'act_days_voi_offnet_mtc'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_offnet_mtc
 ,sum(case when attribute = 'act_days_voi_intl_mtc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_intl_mtc
 ,sum(case when attribute = 'act_days_voi_roam_mtc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_roam_mtc
 ,sum(case when attribute like 'act_days_voi_%_mtc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_all_mtc
 ,sum(case when attribute = 'act_days_sms_onnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_onnet_moc
 ,sum(case when attribute = 'act_days_sms_offnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_offnet_moc
 ,sum(case when attribute = 'act_days_sms_intl_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_intl_moc
 ,sum(case when attribute = 'act_days_sms_roam_moc'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_roam_moc
 ,sum(case when attribute = 'act_days_sms_all_moc'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_all_moc
 ,sum(case when attribute = 'act_days_sms_all_mtc'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_all_mtc
 ,sum(case when attribute = 'act_days_data'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_data
 ,sum(case when attribute = 'act_days_data_roam'     then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_data_roam
 ,sum(case when attribute = 'act_days_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_moc
 ,sum(case when attribute = 'act_days_mtc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_mtc
 ,sum(case when attribute = 'act_days_rge' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_rge
 ,sum(case when attribute = 'act_days_roam' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_roam
 ,sum(case when attribute = 'number_of_active_periods'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) number_of_active_periods
 ,sum(case when attribute = 'mon_bal' then try_cast(attribute_value as double) else null end) mon_bal
 ,sum(case when attribute = 'tue_bal' then try_cast(attribute_value as double) else null end) tue_bal
 ,sum(case when attribute = 'wed_bal' then try_cast(attribute_value as double) else null end) wed_bal
 ,sum(case when attribute = 'thu_bal' then try_cast(attribute_value as double) else null end) thu_bal
 ,sum(case when attribute = 'fri_bal' then try_cast(attribute_value as double) else null end) fri_bal
 ,sum(case when attribute = 'sat_bal' then try_cast(attribute_value as double) else null end) sat_bal
 ,sum(case when attribute = 'sun_bal' then try_cast(attribute_value as double) else null end) sun_bal
 ,sum(case when attribute = 'bal_avg_daily' then try_cast(attribute_value as double) else null end) bal_avg_daily  
 ,sum(case when attribute = 'bal_days_less_5'    then try_cast(attribute_value as bigint) else null end) bal_days_less_5
 ,sum(case when attribute = 'bal_days_negative'  then try_cast(attribute_value as bigint) else null end) bal_days_negative
 ,sum(case when attribute = 'bal_times_less_5'   then try_cast(attribute_value as bigint) else null end) bal_times_less_5
 ,sum(case when attribute = 'bal_times_negative' then try_cast(attribute_value as bigint) else null end) bal_times_negative
 ,sum(case when attribute = 'Opening_Balance'    then try_cast(attribute_value as double) else null end) Opening_Balance
 ,sum(case when attribute = 'Closing_Balance'    then try_cast(attribute_value as double) else null end) Closing_Balance
 ,sum(case when attribute = 'Momo Opening_bal'   then try_cast(attribute_value as double) else null end) Opening_momo_bal
 ,sum(case when attribute = 'Momo Closing_bal'   then try_cast(attribute_value as double) else null end) Closing_momo_bal
 ,null momo_act_date
 ,null momo_dep_cnt    
 ,null momo_dep_amt    
 ,null momo_wit_cnt    
 ,null momo_wit_amt   
 ,null momo_p2p_trx_cnt
 ,null momo_p2p_trx_amt
 ,null momo_type
 ,sum(case when attribute = 'momo_total_trx_fees' then try_cast(attribute_value as double) else null end)  momo_total_trx_fees
 ,null momo_customer_prof
 ,max(case when attribute = 'momo_last_trx_type'  then attribute_string else null end) momo_last_trx_type
 ,yyyymmddRunDate week_started  
 ,yyyyRunDateWeek week_ended
 ,yyyymmddRunDate week_started  
 from (select
 msisdn_key
 ,attribute
 ,attribute_value
 ,attribute_string
 from  cvm_db.cvm20_attributes
 where tbl_dt = yyyymmddRunDate
 and   attribute_value is not null
 group by msisdn_key,attribute,attribute_value ,attribute_string
)
group by msisdn_key
;commit;
