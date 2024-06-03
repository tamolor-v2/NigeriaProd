start transaction;
create table flare_8.FB_FEED_DQ_RPT as 
select 
	'Nigeria' as OpCo,
	a1.tbl_month,
	a1.report_month,
	a1.date_key,
	a1.cell_qos_2g,
	a1.file_received as file_received_cq2g,
	a1.total_records as total_records_cq2g,
	a1.file_received_rag as file_received_rag_cq2g,
	a1.trend_rag as trend_rag_cq2g,
	a2.cell_qos_3g,
	a2.file_received_cq3g,
	a2.total_records_cq3g,
	a2.file_received_rag_cq3g,
	a2.trend_rag_cq3g,
	a3.cell_qos_4g,
	a3.file_received_cq4g,
	a3.total_records_cq4g,
	a3.file_received_rag_cq4g,
	a3.trend_rag_cq4g,
	a4.cell_info_2g,
	a5.cell_info_3g,
	a6.cell_info_4g,
	a7.mtnn_asset_extract_2g_cells,
	a8.mtnn_asset_extract_3g_cells,
	a9.mtnn_asset_extract_4g_cells,
	a10.maps_site_info,
	a11.topology_map,
	a12.mtn_ng_4g_monthly,
	a13.mtn_ng_3g_monthly,
	a14.population_ds,
	a15.gateway_info
from flare_8.fb_CELL_QOS_2G_dq_rpt a1
left join
(select 
	tbl_dt,
	file_received as file_received_cq3g,
	total_records as total_records_cq3g,
	file_received_rag as file_received_rag_cq3g,
	trend_rag as trend_rag_cq3g,
	CELL_QOS_3G	
from flare_8.fb_CELL_QOS_3G_dq_rpt) a2
on a1.tbl_dt=a2.tbl_dt
left join
(select 
	tbl_dt,
	file_received as file_received_cq4g,
	total_records as total_records_cq4g,
	file_received_rag as file_received_rag_cq4g,
	trend_rag as trend_rag_cq4g,
	CELL_QOS_4G	
from flare_8.fb_CELL_QOS_4G_dq_rpt) a3
on a1.tbl_dt=a3.tbl_dt
left join 
(select 
	tbl_dt,
	cell_info_2g
from flare_8.fb_cell_info_2g_dq_rpt) a4
on a1.tbl_dt=a4.tbl_dt
left join
(select 
	tbl_dt,
	tbl_month,
	cell_info_3g	
from flare_8.fb_cell_info_3g_dq_rpt) a5
on a1.tbl_dt=a5.tbl_dt
left join
(select 
	tbl_dt,
	cell_info_4g	
from flare_8.fb_cell_info_4g_dq_rpt) a6
on a1.tbl_dt=a6.tbl_dt
left join
(select 
	tbl_dt,
	mtnn_asset_extract_2g_cells	
from flare_8.fb_asset_extract_2g_dq_rpt) a7
on a1.tbl_dt=a7.tbl_dt
left join
(select 
	tbl_dt,
	mtnn_asset_extract_3g_cells	
from flare_8.fb_asset_extract_3g_dq_rpt) a8
on a1.tbl_dt=a8.tbl_dt
left join
(select 
	tbl_dt,
	mtnn_asset_extract_4g_cells	
from flare_8.fb_asset_extract_4g_dq_rpt) a9
on a1.tbl_dt=a9.tbl_dt
left join
(select 
	tbl_dt,
	MAPS_SITE_INFO	
from flare_8.fb_maps_site_info_dq_rpt) a10
on a1.tbl_dt=a10.tbl_dt
left join
(select 
	tbl_dt,
	topology_map	
from flare_8.fb_topology_map_dq_rpt) a11
on a1.tbl_dt=a11.tbl_dt
left join
(select                
	tbl_dt,
	MTN_NG_4G_MONTHLY	
from flare_8.fb_mtn_ng_4g_monthly_dq_rpt) a12
on a1.tbl_dt=a12.tbl_dt
left join
(select 
	tbl_dt,
	MTN_NG_3G_MONTHLY	
from flare_8.fb_mtn_ng_3g_monthly_dq_rpt) a13
on a1.tbl_dt=a13.tbl_dt
left join
(select 
	tbl_dt,
	population_ds	
from flare_8.fb_population_dq_rpt) a14
on a1.tbl_dt=a14.tbl_dt
left join
(select 
	tbl_dt,
	gateway_info	
from flare_8.fb_gateway_dq_rpt) a15
on a1.tbl_dt=a15.tbl_dt
;
commit;
