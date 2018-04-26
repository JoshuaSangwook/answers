with 
  orderid_count AS (
  select count (distinct orderid) as order_count
    from SampleOrders
 )
 , order_detail_log_count AS (
  select
    s.orderid
   ,o.order_count 
   ,s.productid
   ,count(1) over(partition by s.productid) AS product_count
   from SampleOrders s
   cross join orderid_count o
 )
 , product_pair_with_stat AS (
 
 select
   l1.productid as p1
  ,l2.productid as p2
  ,l1.product_count as p1_count
  ,l2.product_count as p2_count
  ,count(1) as p1_p2_count
  ,l1.order_count as order_count
  from order_detail_log_count as l1
  join order_detail_log_count as l2 on l1.orderid = l2.orderid
 where l1.productid <> l2.productid
 group by l1.productid,l2.productid,l1.product_count,l2.product_count,l1.order_count
 )
 select 
   p1 AS ProudctA
  ,p2 AS ProudctB
  ,p1_p2_count AS Occurrences
  ,100.0*p1_p2_count / order_count AS Support
  ,100.0*p1_p2_count / p1_count    AS Confidence
  ,(100.0*p1_p2_count / p1_count ) / (100.0*p2_count / order_count) AS LiftRatio
  from product_pair_with_stat 
 where (100.0*p1_p2_count / order_count) >= 0.2 
   and (100.0*p1_p2_count / p1_count) >= 0.6
   and ((100.0*p1_p2_count / p1_count ) / (100.0*p2_count / order_count)) >1
 order by p1_p2_count desc limit 10;


