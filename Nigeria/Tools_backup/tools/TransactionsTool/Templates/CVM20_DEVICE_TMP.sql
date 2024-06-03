start transaction;
insert into cvm_db.cvm20_device_tmp
(msisdn_key,yearid,monthid,weekid,week_started,week_ended,imei,imsi,last_detection,msc_gt,
last_configuration,billing,tac,brand,model_name,device_category,software_os_vendor,software_os_name,
software_os_version,ota,data,weight,screen_size,screen_reso,mms_receiver,wap_support,wapversion,
internet,hardware_gprs,hardware_edge,hardware_hsdpa,hardware_hsupa,camera,camera_reso,
memorycard,gps,video,streaming_video,radiofm,audio_amr,audio_mp3,bluetooth,wlan,ptt,
syncmlds,omadm,nw_2g,nw_3g,nw_4g,hardware_lte,hardware_nfc,sim_form_factor,edge_ind,gprs_ind,
number_of_sim,display_width,display_height,display_depth,display_touchscreen,lte_frequencies,
hardware_wifi,brand_name,tbl_dt ) 
select 
msisdn_key,yearid,monthid,weekid,week_started,week_ended,imei,imsi,last_detection,msc_gt,
last_configuration,billing,tac,brand,model_name,device_category,software_os_vendor,software_os_name,
software_os_version,ota,data,weight,screen_size,screen_reso,mms_receiver,wap_support,wapversion,
internet,hardware_gprs,hardware_edge,hardware_hsdpa,hardware_hsupa,camera,camera_reso,
memorycard,gps,video,streaming_video,radiofm,audio_amr,audio_mp3,bluetooth,wlan,ptt,
syncmlds,omadm,nw_2g,nw_3g,nw_4g,hardware_lte,hardware_nfc,sim_form_factor,edge_ind,gprs_ind,
number_of_sim,display_width,display_height,display_depth,display_touchscreen,lte_frequencies,
hardware_wifi,brand_name,
cast( week_started as int) 
from (
select msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID, 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m') monthid, 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID,  
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started, 
cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
,imei  imei
,imsi imsi
,last_detection  last_detection
,msc_gt  msc_gt
,last_configuration  last_configuration
,billing  billing
,tac  tac
,brand  brand
,model_name  model_name
,device_category  device_category
,software_os_vendor  software_os_vendor
,software_os_name software_os_name
,software_os_version  software_os_version
,ota  ota
,data data
,weight  weight
,screen_size  screen_size
,screen_reso  screen_reso
, mms_receiver  mms_receiver
, wap_support  wap_support
, wapversion wapversion
,internet  internet
,hardware_gprs hardware_gprs
,hardware_edge hardware_edge
,hardware_hsdpa  hardware_hsdpa
,hardware_hsupa  hardware_hsupa
,camera camera
,camera_reso camera_reso
,memorycard  memorycard
,gps gps
,video video
,streaming_video streaming_video
,radiofm radiofm
,audio_amr audio_amr
,audio_mp3 audio_mp3
,bluetooth bluetooth
,wlan wlan
,ptt  ptt
,syncmlds syncmlds
,omadm  omadm
,f_2g nw_2g
,f_3g  nw_3g
,lte  nw_4g
,hardware_lte hardware_lte
,hardware_nfc hardware_nfc
,sim_form_factor  sim_form_factor
,edge edge_ind
,gprs  gprs_ind
,number_of_sim  number_of_sim
,display_width  display_width
,display_height display_height
,display_depth  display_depth
,display_touchscreen  display_touchscreen
,lte_frequencies  lte_frequencies
,hardware_wifi  hardware_wifi
,brand_name brand_name
,row_number() over (partition by msisdn_key order by tbl_dt desc ) rnk
from flare_8.dmc_dump_all 
where tbl_dt in(yyyymmddRunDate,yyyyRunDateWeek)
) where rnk =1 ;
commit;
