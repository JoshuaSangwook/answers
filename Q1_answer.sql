select page_id ,visit_date, sum(is_new) as Total_User_Sessions
 from (
    select user_id ,page_id ,visit_date ,visit_dt ,last_event, (unix_timestamp(visit_dt)-unix_timestamp(last_event)) as gap
           , case when (unix_timestamp(visit_dt)-unix_timestamp(last_event)) > 600 or last_event = 0 then 1 else 0 end as is_new
      from (
        select user_id,page_id,visit_date,visit_dt 
        	 , nvl(LAG(visit_dt,1) OVER (PARTITION BY user_id,page_id,visit_date ORDER BY visit_dt) ,0)
               AS last_event
          from ( select *
                        ,concat(to_date(from_unixtime(unix_timestamp(visit_date,'yyyy.MM.dd'))) ," ",visit_time) as visit_dt
                   from SamplePageViews ) a
          ) b
)c
group by page_id,visit_date ;
