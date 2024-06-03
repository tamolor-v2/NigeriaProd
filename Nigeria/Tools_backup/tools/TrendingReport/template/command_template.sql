select "feed_name", day_7, day_6, day_5, day_4, day_3, day_2, day_1
union all
select feed_name, day7 , day6 , day5 , day4 , day3 , day2 ,day1 from (
select a.feed_name, day7 , day6 , day5 , day4 , day3 , day2 ,day1 From
(select feed_name, hive_records as day7 from flare_8.final_feed_report where tbl_dt = day_7) a
inner join 
(select feed_name, hive_records as day6 from flare_8.final_feed_report where tbl_dt = day_6) b on a.feed_name = b.feed_name
inner join 
(select feed_name, hive_records as day5 from flare_8.final_feed_report where tbl_dt = day_5) c on a.feed_name = c.feed_name
inner join 
(select feed_name, hive_records as day4 from flare_8.final_feed_report where tbl_dt = day_4) d on a.feed_name = d.feed_name
inner join 
(select feed_name, hive_records as day3 from flare_8.final_feed_report where tbl_dt = day_3) e on a.feed_name = e.feed_name
inner join 
(select feed_name, hive_records as day2 from flare_8.final_feed_report where tbl_dt = day_2) f on a.feed_name = f.feed_name
inner join 
(select feed_name, hive_records as day1 from flare_8.final_feed_report where tbl_dt = day_1) g on a.feed_name = g.feed_name
order by a.feed_name asc)x;
