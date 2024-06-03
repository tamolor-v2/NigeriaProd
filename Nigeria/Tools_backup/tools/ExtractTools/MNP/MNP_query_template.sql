select 
port_datetime,
msisdn,
donor_name,
user_id,
user_name,
location_code,
location_description,
department_name,
operation_name
from tt_mso_1.mso_mnp_number_summary
--where port_datetime >= to_date('${date} ${hour}:${minS}:00','YYYYMMDD HH24:MI:SS') AND port_datetime <= to_date('${date} ${hour}:${minE}:59','YYYYMMDD HH24:MI:SS') 
