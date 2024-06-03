select 
transaction_date,
msisdn,
swap_reason,
user_id,
user_name,
new_sim,
old_sim,
user_location,
connect_point,
service_type,
service_status
from tt_mso_1.sim_swap_rpt_vw
--where port_datetime >= to_date('${date} ${hour}:${minS}:00','YYYYMMDD HH24:MI:SS') AND port_datetime <= to_date('${date} ${hour}:${minE}:59','YYYYMMDD HH24:MI:SS') 
