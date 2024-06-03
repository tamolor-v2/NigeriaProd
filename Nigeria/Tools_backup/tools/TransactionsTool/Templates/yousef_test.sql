start transaction;
delete from test.fb_cell_info_2g_dq_rpt13_test where report_date >= firstDayOfpreviousMonth;
insert into test.fb_cell_info_2g_dq_rpt13_test select *,tbl_Dt as report_date from test.fb_cell_info_2g_dq_rpt13 where tbl_dt >= firstDayOfpreviousMonth;
commit;
