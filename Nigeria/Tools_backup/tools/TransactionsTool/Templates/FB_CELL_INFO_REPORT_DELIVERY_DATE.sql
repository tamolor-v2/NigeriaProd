start transaction;
delete from flare_8.fb_cell_info_report_delivery_date where report_month=firstDayOfMonth;
insert into flare_8.fb_cell_info_report_delivery_date
 select 
  'Cell Info Report' as report_name,
  try_cast(date_format(date_parse(cast(report_month  as varchar),'%Y%m%d') -interval '1' month ,'%Y%m%d') as integer) as data_month,
  max(try_cast(date_format(date_parse(substring(date_time,1,19), '%Y-%m-%dT%h:%i:%s'),'%Y%m%d') as integer)) as report_delivery_date,report_month
 from flare_8.fb_cell_info_report_base
 where valid_record=1
  and report_month=firstDayOfMonth
 group by 1,4
 union 
 select 
  'Site Info Report' as report_name,
  try_cast(date_format(date_parse(cast(report_month  as varchar),'%Y%m%d') -interval '1' month ,'%Y%m%d') as integer) as data_month,
  max(try_cast(date_format(date_parse(substring(date_time,1,19), '%Y-%m-%dT%h:%i:%s'),'%Y%m%d') as integer)) as report_delivery_date,report_month
 from flare_8.fb_site_info_report_base
 where valid_record=1 
  and report_month=firstDayOfMonth
 group by 1,4
 union 
 select 
  'Cell QoS Report' as report_name,
  try_cast(date_format(date_parse(cast(report_month  as varchar),'%Y%m%d') -interval '1' month ,'%Y%m%d') as integer) as data_month,
  max(try_cast(date_format(date_parse(substring(date_time,1,19), '%Y-%m-%dT%h:%i:%s'),'%Y%m%d') as integer)) as report_delivery_date,report_month
 from flare_8.fb_cell_qos_report_base 
 where valid_record=1 
  and report_month=firstDayOfMonth
 group by 1,4
 union 
 select 
  'Site Engagement Report' as report_name,
  try_cast(date_format(date_parse(cast(report_month  as varchar),'%Y%m%d') -interval '1' month ,'%Y%m%d') as integer) as data_month,
  max(try_cast(date_format(date_parse(substring(date_time,1,19), '%Y-%m-%dT%h:%i:%s'),'%Y%m%d') as integer)) as report_delivery_date,report_month
 from flare_8.fb_cell_qos_report_base
 where valid_record=1 
  and report_month=firstDayOfMonth
 group by 1,4
 union 
 select 
  'System Engagement Report' as report_name,
  try_cast(date_format(date_parse(cast(report_month  as varchar),'%Y%m%d') -interval '1' month ,'%Y%m%d') as integer) as data_month,
  max(try_cast(date_format(date_parse(substring(date_time,1,19), '%Y-%m-%dT%h:%i:%s'),'%Y%m%d') as integer)) as report_delivery_date,report_month
 from flare_8.fb_cell_qos_report_base
 where valid_record=1 
  and report_month=firstDayOfMonth
 group by 1,4;
commit;
