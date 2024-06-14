start transaction;
delete from CVM_DB.CVM20_BIO_DATA_TMP;
insert into CVM_DB.CVM20_BIO_DATA_TMP
(msisdn_key , yearid,monthid,WeekId,week_started,week_ended,first_name ,last_name ,middle_name,gender,mother_maiden_name,dob ,address,city ,city_desc ,district,district_desc,country,country_desc,occupation ,state_of_origin,lga_of_origin,alternate_number, tbl_dt) 
select 
msisdn_key , yearid,monthid,WeekId,week_started,week_ended,first_name ,
last_name ,middle_name,gender,mother_maiden_name,dob ,address,city ,city_desc ,district,district_desc,country,country_desc,occupation ,state_of_origin,lga_of_origin, alternate_number, cast( week_started as int) 
from (
select 
msisdn_key,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y') YearID,
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m')  monthid, 
date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%v') WeekID,
cast(  date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d')),'%Y%m%d') as bigint) week_started,
cast(date_format(DATE_TRUNC('week', date_parse(CAST(tbl_dt AS varchar(10)), '%Y%m%d'))+ interval '6' day,'%Y%m%d') as bigint)  week_ended
,flare_8.newreg_bioupdt_pool.first_name
,flare_8.newreg_bioupdt_pool.last_name
,flare_8.newreg_bioupdt_pool.middle_name   middle_name
,flare_8.newreg_bioupdt_pool.gender   gender
,flare_8.newreg_bioupdt_pool.mother_maiden_name  mother_maiden_name
,flare_8.newreg_bioupdt_pool.dob   dob
,flare_8.newreg_bioupdt_pool.address  address
,flare_8.newreg_bioupdt_pool.city city
,flare_8.newreg_bioupdt_pool.city_desc  city_desc
,flare_8.newreg_bioupdt_pool.district  district
,flare_8.newreg_bioupdt_pool.district_desc  district_desc
,flare_8.newreg_bioupdt_pool.country country
,flare_8.newreg_bioupdt_pool.country_desc country_desc
,flare_8.newreg_bioupdt_pool.occupation  occupation
,flare_8.newreg_bioupdt_pool.state_of_origin  state_of_origin
,flare_8.newreg_bioupdt_pool.lga_of_origin  lga_of_origin 
, alternate_number  alternate_number
, row_number() over (partition by msisdn_key order by tbl_dt desc ) rnk
from flare_8.newreg_bioupdt_pool
where tbl_dt between yyyymmddRunDate and yyyyRunDateWeek
) where rnk=1;
commit;
