start transaction;
insert into cvm_db.cvm20_usg_data_sms_voi_tmp
( msisdn_key ,YearID ,monthid,WeekID ,week_started ,week_ended ,voi_offnet_out_c_count,voi_offnet_out_nc_count,voi_onnet_out_c_count,voi_onnet_out_nc_count ,voi_int_out_c_count,
voi_int_out_nc_count ,voi_offnet_out_b_count ,voi_offnet_out_nb_count,voi_onnet_out_b_count,voi_onnet_out_nb_count ,voi_int_out_b_count,voi_int_out_nb_count ,
voi_offnet_out_free_count,voi_onnet_out_free_count ,voi_int_out_free_count ,voi_offnet_out_c_sec ,voi_offnet_out_nc_sec,voi_onnet_out_c_sec,voi_onnet_out_nc_sec ,
voi_int_out_c_sec,voi_int_out_nc_sec ,voi_offnet_out_b_sec ,voi_offnet_out_nb_sec,voi_onnet_out_b_sec,voi_onnet_out_nb_sec ,voi_int_out_b_sec,voi_int_out_nb_sec ,
voi_offnet_out_free_sec,voi_onnet_out_free_sec ,voi_int_out_free_sec ,voi_roam_out_free_sec,voi_roam_in_free_sec ,voi_roam_out_sec ,voi_roam_out_free_count,
voi_roam_in_free_count ,voi_roam_out_count ,voi_offnet_in_count,voi_onnet_in_count ,voi_roam_in_count,voi_int_in_count ,voi_offnet_in_sec,voi_onnet_in_sec ,
voi_roam_in_sec,voi_int_in_sec , 
voi_out_sec  ,voi_out_count  ,voi_out_count_bundle , voi_out_duration_bundle ,voi_out_count_payg, voi_out_duration_payg, 
weekday_voi_count,weekday_voi_sec  ,weekend_voi_count,weekend_voi_sec  ,
weekday_voi_onnet_count ,weekday_voi_onnet_sec ,weekend_voi_onnet_count ,weekend_voi_onnet_sec,weekday_voi_offnet_count,
weekday_voi_offnet_sec,weekend_voi_offnet_count,weekend_voi_offnet_sec ,weekday_voi_intl_count,weekday_voi_intl_sec  ,
weekend_voi_intl_count,weekend_voi_intl_sec ,weekday_voi_roam_count,weekday_voi_roam_sec  ,weekend_voi_roam_count,weekend_voi_roam_sec  
,sms_offnet_out_c_count,sms_offnet_out_nc_count  ,sms_onnet_out_c_count ,sms_onnet_out_nc_count
,sms_int_out_c_count,sms_int_out_nc_count,sms_offnet_out_b_count  ,sms_offnet_out_nb_count  
,sms_onnet_out_b_count  ,sms_onnet_out_nb_count  ,sms_int_out_b_count  
,sms_int_out_nb_count,sms_offnet_out_free_count,sms_onnet_out_free_count ,sms_int_out_free_count,sms_roam_out_count 
,sms_roam_out_free_count,sms_out_count ,weekday_sms_out_count  ,weekend_sms_out_count ,sms_out_count_bundle  ,sms_out_count_payg
,data_kb_2g,data_kb_3g,data_kb_4g,data_in_bundle_kb,data_in_payg_kb,data_free_kb,data_roam_kb,data_kb  
,data_kb_expired,data_session_drop_rate,data_session_success_rate 
,tbl_dt 
) 
select msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')  monthid,  
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID, 
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,  
cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_chargeable_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.voice_offnet_ma_events as bigint) ,0))voi_offnet_out_c_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_nonchargeable_events as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_offnet_nb_events as bigint) ,0) ) voi_offnet_out_nc_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_da_chargeable_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.voice_onnet_ma_events as bigint),0)) voi_onnet_out_c_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_da_nonchargeable_events as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_onnet_nb_events  as bigint) ,0) )  voi_onnet_out_nc_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_ma_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.voice_int_da_chargeable_events as bigint),0)) voi_int_out_c_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_da_nonchargeable_events as bigint) ,0) ) voi_int_out_nc_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_chargeable_events as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_offnet_ma_events as bigint),0)) voi_offnet_out_b_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_nb_events as bigint) ,0)) voi_offnet_out_nb_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_ma_events as bigint),0) + coalesce(try_cast(segment5b5_usg.voice_onnet_da_chargeable_events as bigint),0)) voi_onnet_out_b_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_nb_events as bigint) ,0)) voi_onnet_out_nb_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_ma_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.voice_int_da_chargeable_events as bigint),0)) voi_int_out_b_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_nb_events as bigint) ,0) ) voi_int_out_nb_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_bonus_events as bigint) ,0)) voi_offnet_out_free_count
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_da_bonus_events as bigint) ,0) ) voi_onnet_out_free_count
,sum(coalesce( try_cast(segment5b5_usg.voice_int_da_bonus_events as bigint) ,0)) voi_int_out_free_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_ma_sou as bigint),0) + coalesce(try_cast(segment5b5_usg.voice_offnet_da_chargeable_sou as bigint),0) ) voi_offnet_out_c_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_nonchargeable_sou as bigint) , 0) + coalesce(try_cast(segment5b5_usg.voice_offnet_nb_sou as bigint),0)) voi_offnet_out_nc_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_da_chargeable_sou as bigint) ,0) + coalesce( try_cast(segment5b5_usg.voice_onnet_ma_sou as bigint),0) ) voi_onnet_out_c_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_da_nonchargeable_sou as bigint) , 0) + coalesce(try_cast(segment5b5_usg.voice_onnet_nb_sou as bigint),0))voi_onnet_out_nc_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_sou as bigint) ,0) ) voi_int_out_c_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_da_nonchargeable_sou as bigint) , 0) + coalesce(try_cast(segment5b5_usg.voice_int_nb_sou as bigint),0))voi_int_out_nc_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_ma_sou as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_offnet_da_chargeable_sou as bigint),0)) voi_offnet_out_b_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_nb_sou as bigint) ,0)) voi_offnet_out_nb_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_sou as bigint) ,0)) voi_onnet_out_b_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_nb_sou as bigint) ,0)) voi_onnet_out_nb_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_sou as bigint) ,0)) voi_int_out_b_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_nb_sou as bigint) ,0)) voi_int_out_nb_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_bonus_sou as bigint) ,0)) voi_offnet_out_free_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_da_bonus_sou as bigint) ,0)) voi_onnet_out_free_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_da_bonus_sou as bigint) ,0)) voi_int_out_free_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_out_da_bonus_sou as bigint) ,0)) voi_roam_out_free_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_in_da_bonus_sou as bigint) ,0)) voi_roam_in_free_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_outgoing_sou as bigint) ,0)) voi_roam_out_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_out_da_bonus_events as bigint) ,0)) voi_roam_out_free_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_in_da_bonus_events as bigint) ,0)) voi_roam_in_free_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_outgoing_events as bigint) ,0)) voi_roam_out_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_in_events as bigint) ,0)) voi_offnet_in_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_in_events as bigint) ,0)) voi_onnet_in_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_incoming_events as bigint) ,0)) voi_roam_in_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_in_events as bigint) ,0)) voi_int_in_count 
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_in_sou as bigint) ,0)) voi_offnet_in_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_onnet_in_sou as bigint) ,0)) voi_onnet_in_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_roam_incoming_sou as bigint) ,0)) voi_roam_in_sec 
,sum(coalesce( try_cast(segment5b5_usg.voice_int_in_sou as bigint) ,0)) voi_int_in_sec
,sum(coalesce( try_cast(segment5b5_usg.voice_int_in_sou as bigint),0) +  coalesce(try_cast(segment5b5_usg.voice_offnet_sou as bigint),0) + 
     coalesce( try_cast(segment5b5_usg.voice_onnet_sou as bigint),0) + coalesce(try_cast(segment5b5_usg.voice_roam_sou as bigint),0)  )  voi_out_sec  
,sum(coalesce( try_cast(segment5b5_usg.voice_events as bigint),0) ) voi_out_count
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_chargeable_events as bigint),0) +  coalesce( try_cast(segment5b5_usg.voice_onnet_da_chargeable_events as bigint),0) + 
    coalesce ( try_cast(segment5b5_usg.voice_roam_out_da_chargeable_events as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_int_da_chargeable_events as bigint),0)  ) voi_out_count_bundle
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_da_chargeable_sou as bigint),0) +  coalesce( try_cast(segment5b5_usg.voice_onnet_da_chargeable_sou as bigint),0) + 
     coalesce( try_cast(segment5b5_usg.voice_roam_out_da_chargeable_sou as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_int_da_chargeable_sou as bigint),0)  ) voi_out_duration_bundle
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_ma_events as bigint),0) +  coalesce( try_cast(segment5b5_usg.voice_onnet_ma_events as bigint),0) + 
     coalesce( try_cast(segment5b5_usg.voice_roam_out_ma_events as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_int_ma_events as bigint),0)  ) voi_out_count_payg
,sum(coalesce( try_cast(segment5b5_usg.voice_offnet_ma_sou as bigint),0) +  coalesce( try_cast(segment5b5_usg.voice_onnet_ma_sou as bigint),0) + 
     coalesce( try_cast(segment5b5_usg.voice_roam_out_ma_sou as bigint),0) + coalesce( try_cast(segment5b5_usg.voice_int_ma_sou as bigint),0)  ) voi_out_duration_payg 
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('MON','TUE','WED','THU','FRI') then 
coalesce(try_cast(segment5b5_usg.voice_events as bigint),0) else 0 end  )  weekday_voi_count 
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('MON','TUE','WED','THU','FRI') then 
coalesce(try_cast(segment5b5_usg.voice_int_in_sou as bigint),0) +  coalesce(try_cast(segment5b5_usg.voice_offnet_sou  as bigint),0) + 
coalesce(try_cast(segment5b5_usg.voice_onnet_sou as bigint),0) + coalesce(try_cast(segment5b5_usg.voice_roam_sou as bigint),0) else 0 end  )  weekday_voi_sec  
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
coalesce(try_cast(segment5b5_usg.voice_events as bigint),0)  else 0 end  )  weekend_voi_count 
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
(coalesce(try_cast(segment5b5_usg.voice_int_in_sou as bigint),0) +  coalesce(try_cast(segment5b5_usg.voice_offnet_sou as bigint),0) + 
coalesce(try_cast(segment5b5_usg.voice_onnet_sou as bigint),0)  + coalesce(try_cast(segment5b5_usg.voice_roam_sou as bigint),0)  ) else 0 end )  weekend_voi_sec 
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('MON','TUE','WED','THU','FRI') then 
coalesce( try_cast(segment5b5_usg.voice_onnet_events as bigint),0)else 0 end ) weekday_voi_onnet_count
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('MON','TUE','WED','THU','FRI') then 
coalesce( try_cast(segment5b5_usg.voice_onnet_sou  as bigint),0)else 0 end ) weekday_voi_onnet_sec 
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
coalesce( try_cast(segment5b5_usg.voice_onnet_events as bigint),0)  else 0 end ) weekend_voi_onnet_count
,sum(
case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
coalesce( try_cast(segment5b5_usg.voice_onnet_sou  as bigint),0)else 0 end )weekend_voi_onnet_sec  
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then 
 coalesce( try_cast(segment5b5_usg.voice_offnet_events  as bigint),0)  else 0 end )weekday_voi_offnet_count
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then 
 coalesce( try_cast(segment5b5_usg.voice_offnet_sou  as bigint),0)  else 0 end )weekday_voi_offnet_sec
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
 coalesce(  try_cast(segment5b5_usg.voice_offnet_events as bigint),0)  else 0 end ) weekend_voi_offnet_count
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
 coalesce(  try_cast(segment5b5_usg.voice_offnet_sou  as bigint),0)  else 0 end ) weekend_voi_offnet_sec
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('MON','TUE','WED','THU','FRI') then 
 coalesce( try_cast(segment5b5_usg.voice_int_events as bigint) ,0)  else 0 end ) weekday_voi_intl_count
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then 
 coalesce( try_cast(segment5b5_usg.voice_int_sou  as bigint),0)  else 0 end )weekday_voi_intl_sec
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
 coalesce(  try_cast(segment5b5_usg.voice_int_events as bigint),0)  else 0 end ) weekend_voi_intl_count
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
 coalesce( try_cast(segment5b5_usg.voice_int_sou as bigint),0)  else 0 end )weekend_voi_intl_sec
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then 
 coalesce( try_cast(segment5b5_usg.voice_roam_events  as bigint),0)  else 0 end )weekday_voi_roam_count
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in  ('MON','TUE','WED','THU','FRI') then 
 coalesce( try_cast(segment5b5_usg.voice_roam_sou  as bigint),0)  else 0 end ) weekday_voi_roam_sec
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
 coalesce(  try_cast(segment5b5_usg.voice_roam_events as bigint),0)  else 0 end )weekend_voi_roam_count
,sum(
 case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
    coalesce(  try_cast(segment5b5_usg.voice_roam_sou as bigint)  ,0)  else 0 end )weekend_voi_roam_sec
,sum(coalesce( try_cast(segment5b5_usg.sms_offnet_da_chargeable_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.sms_offnet_ma_events as bigint),0)) sms_offnet_out_c_count
,sum(coalesce( try_cast(segment5b5_usg.sms_offnet_da_nonchargeable_events as bigint),0) + coalesce(try_cast( segment5b5_usg.sms_offnet_nb_events as bigint) ,0) ) sms_offnet_out_nc_count
,sum(coalesce( try_cast(segment5b5_usg.sms_onnet_da_chargeable_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.sms_onnet_ma_events as bigint),0))  sms_onnet_out_c_count
,sum(coalesce( try_cast(segment5b5_usg.sms_onnet_da_nonchargeable_events as bigint),0) + coalesce(try_cast(segment5b5_usg.sms_onnet_nb_events as bigint) ,0) )sms_onnet_out_nc_count
,sum(coalesce( try_cast(segment5b5_usg.sms_int_ma_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.sms_int_da_chargeable_events as bigint),0))sms_int_out_c_count
,sum(coalesce( try_cast(segment5b5_usg.sms_int_da_nonchargeable_events as bigint) ,0) )  sms_int_out_nc_count
,sum(coalesce( try_cast(segment5b5_usg.sms_offnet_da_chargeable_events as bigint),0) + coalesce(try_cast(segment5b5_usg.sms_offnet_ma_events as bigint),0))sms_offnet_out_b_count
,sum(coalesce( try_cast(segment5b5_usg.sms_offnet_nb_events as bigint) ,0))  sms_offnet_out_nb_count
,sum(coalesce( try_cast(segment5b5_usg.sms_onnet_ma_events as bigint),0) + coalesce(try_cast(segment5b5_usg.sms_onnet_da_chargeable_events as bigint),0))sms_onnet_out_b_count
,sum(coalesce( try_cast(segment5b5_usg.sms_onnet_nb_events as bigint) ,0))sms_onnet_out_nb_count
,sum(coalesce( try_cast(segment5b5_usg.sms_int_ma_events as bigint) ,0) + coalesce(try_cast(segment5b5_usg.sms_int_da_chargeable_events as bigint),0))sms_int_out_b_count
,sum(coalesce( try_cast(segment5b5_usg.sms_int_nb_events as bigint) ,0) ) sms_int_out_nb_count
,sum(coalesce( try_cast(segment5b5_usg.sms_offnet_da_bonus_events as bigint) ,0))  sms_offnet_out_free_count
,sum(coalesce( try_cast(segment5b5_usg.sms_onnet_da_bonus_events as bigint) ,0) )  sms_onnet_out_free_count
,sum(coalesce( try_cast(segment5b5_usg.sms_int_da_bonus_events as bigint) ,0))  sms_int_out_free_count
,sum(coalesce( try_cast(segment5b5_usg.sms_roam_outgoing_events as bigint) ,0)) sms_roam_out_count
,sum(coalesce( try_cast(segment5b5_usg.sms_roam_out_da_bonus_events as bigint) ,0))sms_roam_out_free_count 
,sum(coalesce( try_cast(segment5b5_usg.sms_events as bigint),0) )  sms_out_count
 ,sum( case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) 
  in ('MON','TUE','WED','THU','FRI') then  coalesce( try_cast(segment5b5_usg.sms_events as bigint),0)  else 0 end  )  weekday_sms_out_count
 ,sum(case when upper(format_datetime(  date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'),'E')) in ('SAT','SUN') then 
 coalesce( try_cast(segment5b5_usg.sms_events  as bigint),0)  else 0 end  )  weekend_sms_out_count
,sum(coalesce( try_cast(segment5b5_usg.sms_offnet_da_chargeable_events  as bigint),0) +  coalesce( try_cast(segment5b5_usg.sms_onnet_da_chargeable_events  as bigint),0)  + 
coalesce( try_cast(segment5b5_usg.sms_roam_out_da_chargeable_events  as bigint),0) + coalesce( try_cast(segment5b5_usg.sms_int_da_chargeable_events  as bigint),0)  )sms_out_count_bundle
,sum(coalesce(try_cast( segment5b5_usg.sms_offnet_ma_events  as bigint),0) +  coalesce( try_cast(segment5b5_usg.sms_onnet_ma_events  as bigint),0)  + 
coalesce( try_cast(segment5b5_usg.sms_roam_out_ma_events  as bigint),0) + coalesce( try_cast(segment5b5_usg.sms_int_ma_events  as bigint),0)  ) sms_out_count_payg
,SUM( data_2g_local_ma_kb+data_2g_local_da_bonus_kb+data_2g_local_da_chargeable_kb+
data_2g_local_da_nonchargeable_kb+data_2g_local_da_unknown_kb  )  data_kb_2g
,SUM( data_3g_local_ma_kb+data_3g_local_da_bonus_kb+data_3g_local_da_chargeable_kb+
data_3g_local_da_nonchargeable_kb+data_3g_local_da_unknown_kb )  data_kb_3g
,sum(  data_4g_local_ma_kb+data_4g_local_da_bonus_kb+data_4g_local_da_chargeable_kb+
data_4g_local_da_nonchargeable_kb+data_4g_local_da_unknown_kb  )  data_kb_4g
,sum( coalesce( try_cast(segment5b5_usg.data_local_da_all_kb as bigint),0)) data_in_bundle_kb
,sum( coalesce( try_cast(segment5b5_usg.data_oob_local_kb as bigint),0)) data_in_payg_kb
,sum( coalesce( try_cast(segment5b5_usg.data_local_da_bonus_kb as bigint),0))data_free_kb
,sum( coalesce( try_cast(segment5b5_usg.data_roam_kb as bigint),0)) data_roam_kb
,sum( coalesce( try_cast(segment5b5_usg.data_local_ma_da_kb as bigint),0))data_kb 
, NULL  data_kb_expired,NULL data_session_drop_rate,NULL  sdata_session_success_rate
 , cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int)
from nigeria.segment5b5_usg
where aggr='daily2' 
and tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
and voice_sou <999999 
group by  msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y'),
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')  ,  
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v')  ,  
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint)  ,  
cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint),
 cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as int); 
commit;
