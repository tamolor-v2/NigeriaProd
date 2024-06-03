start transaction;
insert into cvm_db.CVM20_REC_ENGINE_DEV2
 ( msisdn_key ,last_date_vce_offnet ,last_date_vce_onnet,last_date_vce_int,last_date_vce_roam ,last_date_sms_offnet ,last_date_sms_onnet,
last_date_sms_int,last_date_sms_roam ,last_data_date ,last_data_date_roam,ds_last_sms_in ,ds_last_sms_out,ds_last_sms_rge,ds_last_voi_in ,
ds_last_voi_out,ds_last_voi_rge,ds_last_data_in,ds_last_data_out ,ds_last_data_rge ,ds_last_airtime_transfer ,ds_last_airtime_receive,
ds_last_digital_crbt ,ds_last_digital_callerfeel ,ds_last_digital_phonebookbackup,ds_last_digital,last_digital_rge_dt,last_rge_date,
last_activity_date ,ds_digital_services_act,ds_last_recharge_sbsc,ds_last_recharge_sbsc_mymtn,ds_last_recharge_sbsc_digital,ds_ayoba_act ,
ds_last_momo_rge ,ds_momo_act,ds_last_momo ,dola ,act_days_voi_onnet_moc ,act_days_voi_offnet_moc,act_days_voi_intl_moc,act_days_voi_roam_moc,
act_days_voi_all_moc ,act_days_voi_onnet_mtc ,act_days_voi_offnet_mtc,act_days_voi_intl_mtc,act_days_voi_roam_mtc,act_days_voi_all_mtc ,
act_days_sms_onnet_moc ,act_days_sms_offnet_moc,act_days_sms_intl_moc,act_days_sms_roam_moc,act_days_sms_all_moc ,act_days_sms_all_mtc ,
act_days_data,act_days_data_roam ,act_days_moc ,act_days_mtc ,act_days_rge ,act_days_roam,number_of_active_periods ,mon_bal,tue_bal,wed_bal,
thu_bal,fri_bal,sat_bal,sun_bal,
bal_avg_daily,bal_days_less_5,bal_days_negative,bal_times_less_5 ,bal_times_negative ,opening_balance,closing_balance,opening_momo_bal ,
closing_momo_bal ,  airtime_balance_transfer_in_amt, airtime_balance_transfer_out_amt  , airtime_balance_transfer_in_cnt , airtime_balance_transfer_out_cnt ,
momo_act_date,momo_dep_cnt ,momo_dep_amt ,momo_wit_cnt ,momo_wit_amt ,momo_p2p_trx_cnt ,momo_p2p_trx_amt ,momo_type,
momo_total_trx_fees,momo_customer_prof ,momo_last_trx_type ,start_of_week,end_of_week,tbl_dt  )
select
msisdn_key,  
last_date_vce_offnet   
,last_date_vce_onnet  
,last_date_vce_int    
,last_date_vce_roam   
,last_date_sms_offnet 
,last_date_sms_onnet  
,last_date_sms_int    
,last_date_sms_roam   
,last_data_date    
,last_data_date_roam  
,case when (ds_last_sms_in > 6 or ds_last_sms_in is null )  then 7 else ds_last_sms_in  end   ds_last_sms_in  
,case when (ds_last_sms_out   > 6 or ds_last_sms_out is null )  then 7 else ds_last_sms_out  end  ds_last_sms_out     
,case when (ds_last_sms_rge   > 6 or ds_last_sms_rge is null )  then 7 else ds_last_sms_rge  end  ds_last_sms_rge     
,case when (ds_last_voi_in > 6 or ds_last_voi_in is null ) then 7 else ds_last_voi_in  end  ds_last_voi_in      
,case when (ds_last_voi_out   > 6 or ds_last_voi_out is null )  then 7 else ds_last_voi_out  end  ds_last_voi_out     
,case when (ds_last_voi_rge   > 6 or ds_last_voi_rge is null )  then 7 else ds_last_voi_rge  end  ds_last_voi_rge     
,case when (ds_last_data_in   > 6 or ds_last_data_in is null )  then 7 else ds_last_data_in  end  ds_last_data_in     
,case when (ds_last_data_out     > 6 or ds_last_data_out      is null )  then 7 else ds_last_data_out    end  ds_last_data_out       
,case when (ds_last_data_rge     > 6 or ds_last_data_rge      is null )  then 7 else ds_last_data_rge    end  ds_last_data_rge       
,case when (ds_last_airtime_transfer   > 6 or ds_last_airtime_transfer    is null )  then 7 else ds_last_airtime_transfer  end  ds_last_airtime_transfer     
,case when (ds_last_airtime_receive    > 6 or ds_last_airtime_receive     is null )  then 7 else ds_last_airtime_receive   end  ds_last_airtime_receive   
,case when (ds_last_digital_crbt    > 6 or ds_last_digital_crbt     is null )  then 7 else ds_last_digital_crbt   end  ds_last_digital_crbt      
,case when (ds_last_digital_callerfeel    > 6 or ds_last_digital_callerfeel  is null )  then 7 else ds_last_digital_callerfeel   end  ds_last_digital_callerfeel   
,case when (ds_last_digital_phonebookbackup  > 6 or ds_last_digital_phonebookbackup   is null )  then 7 else ds_last_digital_phonebookbackup end  ds_last_digital_phonebookbackup 
,case when (ds_last_digital      > 6 or ds_last_digital       is null )  then 7 else ds_last_digital     end  ds_last_digital   
,last_digital_rge_dt   
,last_rge_date   
,last_activity_date      
,case when (ds_digital_services_act    > 6 or ds_digital_services_act     is null )  then 7 else ds_digital_services_act   end  ds_digital_services_act   
,case when (ds_last_recharge_sbsc   > 6 or ds_last_recharge_sbsc    is null )  then 7 else ds_last_recharge_sbsc     end  ds_last_recharge_sbsc     
,case when (ds_last_recharge_sbsc_mymtn   > 6 or ds_last_recharge_sbsc_mymtn    is null )  then 7 else ds_last_recharge_sbsc_mymtn  end  ds_last_recharge_sbsc_mymtn  
,case when (ds_last_recharge_sbsc_digital > 6 or ds_last_recharge_sbsc_digital  is null )  then 7 else ds_last_recharge_sbsc_digital   end  ds_last_recharge_sbsc_digital   
,case when (ds_ayoba_act      > 6 or ds_ayoba_act       is null )  then 7 else ds_ayoba_act     end  ds_ayoba_act        
,case when (ds_last_momo_rge     > 6 or ds_last_momo_rge      is null )  then 7 else ds_last_momo_rge    end  ds_last_momo_rge       
,case when (ds_momo_act       > 6 or ds_momo_act        is null )  then 7 else ds_momo_act      end  ds_momo_act      
,case when (ds_last_momo      > 6 or ds_last_momo       is null )  then 7 else ds_last_momo     end  ds_last_momo  
,dola  
, case when ( act_days_voi_onnet_moc > 7 )  then 0   when   act_days_voi_onnet_moc  is null  then 0 else   act_days_voi_onnet_moc  end act_days_voi_onnet_moc   
 ,case when ( act_days_voi_offnet_moc   > 7 )  then 0   when   act_days_voi_offnet_moc is null  then 0 else   act_days_voi_offnet_moc end act_days_voi_offnet_moc    
 ,case when ( act_days_voi_intl_moc  > 7 )  then 0   when   act_days_voi_intl_moc   is null  then 0 else   act_days_voi_intl_moc   end act_days_voi_intl_moc   
 ,case when ( act_days_voi_roam_moc  > 7 )  then 0   when   act_days_voi_roam_moc   is null  then 0 else   act_days_voi_roam_moc   end act_days_voi_roam_moc   
 ,case when ( act_days_voi_all_moc   > 7 )  then 0   when   act_days_voi_all_moc is null  then 0 else   act_days_voi_all_moc    end act_days_voi_all_moc    
 ,case when ( act_days_voi_onnet_mtc > 7 )  then 0   when   act_days_voi_onnet_mtc  is null  then 0 else   act_days_voi_onnet_mtc  end act_days_voi_onnet_mtc  
 ,case when ( act_days_voi_offnet_mtc   > 7 )  then 0   when   act_days_voi_offnet_mtc is null  then 0 else   act_days_voi_offnet_mtc end act_days_voi_offnet_mtc    
 ,case when ( act_days_voi_intl_mtc  > 7 )  then 0   when   act_days_voi_intl_mtc   is null  then 0 else   act_days_voi_intl_mtc   end act_days_voi_intl_mtc   
 ,case when ( act_days_voi_roam_mtc  > 7 )  then 0   when   act_days_voi_roam_mtc   is null  then 0 else   act_days_voi_roam_mtc   end act_days_voi_roam_mtc   
 ,case when ( act_days_voi_all_mtc   > 7 )  then 0   when   act_days_voi_all_mtc is null  then 0 else   act_days_voi_all_mtc    end act_days_voi_all_mtc    
 ,case when ( act_days_sms_onnet_moc > 7 )  then 0   when   act_days_sms_onnet_moc  is null  then 0 else   act_days_sms_onnet_moc  end act_days_sms_onnet_moc  
 ,case when ( act_days_sms_offnet_moc   > 7 )  then 0   when   act_days_sms_offnet_moc is null  then 0 else   act_days_sms_offnet_moc end act_days_sms_offnet_moc    
 ,case when ( act_days_sms_intl_moc  > 7 )  then 0   when   act_days_sms_intl_moc   is null  then 0 else   act_days_sms_intl_moc   end act_days_sms_intl_moc   
 ,case when ( act_days_sms_roam_moc  > 7 )  then 0   when   act_days_sms_roam_moc   is null  then 0 else   act_days_sms_roam_moc   end act_days_sms_roam_moc   
 ,case when ( act_days_sms_all_moc   > 7 )  then 0   when   act_days_sms_all_moc is null  then 0 else   act_days_sms_all_moc    end act_days_sms_all_moc    
 ,case when ( act_days_sms_all_mtc   > 7 )  then 0   when   act_days_sms_all_mtc is null  then 0 else   act_days_sms_all_mtc    end act_days_sms_all_mtc    
 ,case when ( act_days_data    > 7 )  then 0   when   act_days_data     is null  then 0 else   act_days_data     end act_days_data     
 ,case when ( act_days_data_roam  > 7 )  then 0   when   act_days_data_roam   is null  then 0 else   act_days_data_roam   end act_days_data_roam   
 ,case when ( act_days_moc     > 7 )  then 0   when   act_days_moc   is null  then 0 else   act_days_moc      end act_days_moc      
 ,case when ( act_days_mtc     > 7 )  then 0   when   act_days_mtc   is null  then 0 else   act_days_mtc      end act_days_mtc      
 ,case when ( act_days_rge     > 7 )  then 0   when   act_days_rge   is null  then 0 else   act_days_rge      end act_days_rge      
 ,case when ( act_days_roam    > 7 )  then 0   when   act_days_roam     is null  then 0 else   act_days_roam     end act_days_roam  
, number_of_active_periods 
,mon_bal  
,tue_bal  
,wed_bal  
,thu_bal  
,fri_bal  
,sat_bal  
,sun_bal  
,bal_avg_daily   
,bal_days_less_5 
,bal_days_negative  
,bal_times_less_5   
,bal_times_negative 
,Opening_Balance 
,Closing_Balance 
,Opening_momo_bal   
,Closing_momo_bal 
, balance_transfer_in_amount   airtime_balance_transfer_in_amt
, balance_transfer_out_amount  airtime_balance_transfer_out_amt
,try_cast(balance_transfer_in_cnt  as int) airtime_balance_transfer_in_cnt
, try_cast(balance_transfer_out_cnt as int)    airtime_balance_transfer_out_cnt
,momo_act_date  
,momo_dep_cnt   
,momo_dep_amt   
,momo_wit_cnt   
,momo_wit_amt   
,momo_p2p_trx_cnt  
,momo_p2p_trx_amt  
,momo_type   
,momo_total_trx_fees
,momo_customer_prof
,momo_last_trx_type
 , week_started             
 , Week_ended        
 , tbl_dt   
 from 
(
select
 msisdn_key
 ,max(case when attribute = 'last_date_vce_offnet' then try_cast(attribute_value as bigint) else null end)   last_date_vce_offnet
 ,max(case when attribute = 'last_date_vce_onnet'   then try_cast(attribute_value as bigint) else null end) last_date_vce_onnet
 ,max(case when attribute = 'last_date_vce_int' then try_cast(attribute_value as bigint) else null end)  last_date_vce_int
 ,max(case when attribute = 'last_date_vce_roam'  then try_cast(attribute_value as bigint) else null end)   last_date_vce_roam
 ,max(case when attribute = 'last_date_sms_offnet' then try_cast(attribute_value as bigint) else null end)  last_date_sms_offnet
 ,max(case when attribute = 'last_date_sms_onnet'   then try_cast(attribute_value as bigint) else null end) last_date_sms_onnet
 ,max(case when attribute = 'last_date_sms_int' then try_cast(attribute_value as bigint) else null end)  last_date_sms_int
 ,max(case when attribute = 'last_date_sms_roam'   then try_cast(attribute_value as bigint) else null end)  last_date_sms_roam
 ,max(case when attribute = 'last_data_date' then try_cast(attribute_value as bigint) else null end)  last_data_date
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
 ,max(case when attribute = 'last_rge_date' then try_cast(attribute_value as bigint) else null end)  last_rge_date
 ,max(case when attribute = 'last_rge_date'  then try_cast(attribute_value as bigint) else null end)    last_activity_date
 ,max(case when attribute = 'ds_digital_services_act'  then try_cast(attribute_value as bigint) else null end) ds_digital_services_act
 ,max(case when attribute = 'ds_last_recharge_sbsc' then try_cast(attribute_value as bigint) else null end) ds_last_recharge_sbsc
 ,max(case when attribute = 'ds_last_recharge_sbsc_mymtn'  then try_cast(attribute_value as bigint) else null end) ds_last_recharge_sbsc_mymtn
 ,max(case when attribute = 'ds_last_recharge_sbsc_digital'   then try_cast(attribute_value as bigint) else null end) ds_last_recharge_sbsc_digital
 ,max(case when attribute = 'ds_ayoba_act' then try_cast(attribute_value as bigint) else null end) ds_ayoba_act
 ,max(case when attribute = 'ds_last_momo_rge'  then try_cast(attribute_value as bigint) else null end) ds_last_momo_rge
 ,max(case when attribute = 'ds_momo_act'  then try_cast(attribute_value as bigint) else null end) ds_momo_act
 ,max(case when attribute = 'ds_last_momo_rge'  then try_cast(attribute_value as bigint) else null end) ds_last_momo
 ,max(case when attribute = 'dola'  then try_cast(attribute_value as bigint) else null end) dola
 ,sum(case when attribute = 'act_days_voi_onnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_onnet_moc
 ,sum(case when attribute = 'act_days_voi_offnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_offnet_moc
 ,sum(case when attribute = 'act_days_voi_intl_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_intl_moc
 ,sum(case when attribute = 'act_days_voi_roam_moc'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_roam_moc
 ,sum(case when attribute = 'act_days_voi_all_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_all_moc
 ,sum(case when attribute = 'act_days_voi_onnet_mtc'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_onnet_mtc
 ,sum(case when attribute = 'act_days_voi_offnet_mtc'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_offnet_mtc
 ,sum(case when attribute = 'act_days_voi_intl_mtc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_intl_mtc
 ,sum(case when attribute = 'act_days_voi_roam_mtc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_roam_mtc
 ,sum(case when attribute like 'act_days_voi_%_mtc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_voi_all_mtc
 ,sum(case when attribute = 'act_days_sms_onnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_onnet_moc
 ,sum(case when attribute = 'act_days_sms_offnet_moc'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_offnet_moc
 ,sum(case when attribute = 'act_days_sms_intl_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_intl_moc
 ,sum(case when attribute = 'act_days_sms_roam_moc'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_roam_moc
 ,sum(case when attribute = 'act_days_sms_all_moc'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_all_moc
 ,sum(case when attribute = 'act_days_sms_all_mtc'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_sms_all_mtc
 ,sum(case when attribute = 'act_days_data'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_data
 ,sum(case when attribute = 'act_days_data_roam'  then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_data_roam
 ,sum(case when attribute = 'act_days_moc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_moc
 ,sum(case when attribute = 'act_days_mtc' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_mtc
 ,sum(case when attribute = 'act_days_rge' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_rge
 ,sum(case when attribute = 'act_days_roam' then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) act_days_roam
 ,sum(case when attribute = 'number_of_active_periods'   then COALESCE(COALESCE(TRY_CAST(attribute_value AS bigint), 0), 0)  else null end) number_of_active_periods
 ,sum(case when attribute = 'mon_bal' then try_cast(attribute_value as double) else 0 end) mon_bal
 ,sum(case when attribute = 'tue_bal' then try_cast(attribute_value as double) else 0 end) tue_bal
 ,sum(case when attribute = 'wed_bal' then try_cast(attribute_value as double) else 0 end) wed_bal
 ,sum(case when attribute = 'thu_bal' then try_cast(attribute_value as double) else 0 end) thu_bal
 ,sum(case when attribute = 'fri_bal' then try_cast(attribute_value as double) else 0 end) fri_bal
 ,sum(case when attribute = 'sat_bal' then try_cast(attribute_value as double) else 0 end) sat_bal
 ,sum(case when attribute = 'sun_bal' then try_cast(attribute_value as double) else 0 end) sun_bal
 ,sum(case when attribute = 'bal_avg_daily' then try_cast(attribute_value as double) else 0 end)   bal_avg_daily  
 ,sum(case when attribute = 'bal_days_less_5' then try_cast(attribute_value as bigint) else 0 end) bal_days_less_5
 ,sum(case when attribute = 'bal_days_negative'  then try_cast(attribute_value as bigint) else 0 end) bal_days_negative
 ,sum(case when attribute = 'bal_times_less_5'   then try_cast(attribute_value as bigint) else 0 end) bal_times_less_5
 ,sum(case when attribute = 'bal_times_negative' then try_cast(attribute_value as bigint) else 0 end) bal_times_negative
 ,sum(case when attribute = 'Opening_Balance' then try_cast(attribute_value as double) else 0 end) Opening_Balance
 ,sum(case when attribute = 'Closing_Balance' then try_cast(attribute_value as double) else 0 end) Closing_Balance
 ,sum(case when attribute = 'Momo Opening_bal'   then try_cast(attribute_value as double) else 0 end) Opening_momo_bal
 ,sum(case when attribute = 'Momo Closing_bal'   then try_cast(attribute_value as double) else 0 end) Closing_momo_bal
  ,sum(case when attribute = 'balance_transfer_in_amount'   then try_cast(attribute_value as double) else 0 end)  balance_transfer_in_amount    
  ,sum(case when attribute = 'balance_transfer_out_amount'   then try_cast(attribute_value as double) else 0 end)  balance_transfer_out_amount    
  ,sum(case when attribute = 'balance_transfer_in_cnt'   then try_cast(attribute_value as double) else 0 end)  balance_transfer_in_cnt   
  ,sum(case when attribute = 'balance_transfer_out_cnt'   then try_cast(attribute_value as double) else 0 end) balance_transfer_out_cnt   
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
 ,yyyymmddRunDate tbl_dt   
 from (select
 msisdn_key
 ,attribute
 ,attribute_value
 ,attribute_string
 from  cvm_db.CVM20_ATTRIBUTES
 where tbl_dt = yyyymmddRunDate
 and   attribute_value is not null
 group by msisdn_key,attribute,attribute_value ,attribute_string
) 
group by msisdn_key    
) where msisdn_key  in 
(select msisdn_key 
from CVM_DB.cvm20_subs_tmp
where  dola <=180
and tbl_dt = yyyymmddRunDate
);
commit;
