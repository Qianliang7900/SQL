````shell
table: t_price
+-------------+-------------+--------+
| product_id  |     dt      | price  |
+-------------+-------------+--------+
| 101         | 2022-07-01  | 77.50  |
| 101         | 2022-07-02  | NULL   |
| 101         | 2022-07-03  | 88.80  |
| 101         | 2022-07-04  | NULL   |
| 101         | 2022-07-05  | NULL   |
| 102         | 2022-07-01  | 15.00  |
| 102         | 2022-07-02  | NULL   |
| 102         | 2022-07-03  | NULL   |
| 102         | 2022-07-04  | 14.40  |
| 102         | 2022-07-05  | 18.80  |
+-------------+-------------+--------+


问题：商品的价格随时间变化，当天价格为null表示价格未发生变化与前一天一致（多天为null 以此类推）。
现要求展示当天的价格与该价格此次变动前的价格？ 结果如下表：

+-------------+-------------+------------+---------------+
| product_id  |     dt      | now_price  | before_price  |
+-------------+-------------+------------+---------------+
| 101         | 2022-07-01  | 77.50      | NULL          |
| 101         | 2022-07-02  | 77.50      | NULL          |
| 101         | 2022-07-03  | 88.80      | 77.50         |
| 101         | 2022-07-04  | 88.80      | 77.50         |
| 101         | 2022-07-05  | 88.80      | 77.50         |
| 102         | 2022-07-01  | 15.00      | NULL          |
| 102         | 2022-07-02  | 15.00      | NULL          |
| 102         | 2022-07-03  | 15.00      | NULL          |
| 102         | 2022-07-04  | 14.40      | 15.00         |
| 102         | 2022-07-05  | 18.80      | 14.40         |
+-------------+-------------+------------+---------------+

````



<details>
  <summary> 🦄 Click to show  the sql    </summary>
  
  ````sql
-- spark-sql
select 
    product_id
    ,dt
    ,now_price
    ,last_value(lags , true) over(partition by product_id order by dt) as before_price
from 
(
    select 
        product_id
        ,dt
        ,price
        ,last_value(price , true) over(partition by product_id order by dt) as now_price
        ,lag(price,1) over(partition by product_id order by if(price is null ,null,dt) nulls first)  as lags
    from t_price 
) t1
order by product_id,dt ;


````
</details>



***


