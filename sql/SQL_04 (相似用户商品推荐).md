> 1
````shell
table: orders
+----------+-------------+
| user_id  | product_id  |
+----------+-------------+
| 123      | 1           |
| 123      | 2           |
| 123      | 3           |
| 123      | 7           |
| 123      | 8           |
| 456      | 1           |
| 456      | 2           |
| 456      | 4           |
| 789      | 2           |
| 789      | 4           |
| 789      | 7           |
| 789      | 8           |
+----------+-------------+

-- 推荐出每个user_id,和他相似的用户购买过的product
--1、排除自己购买过的
--2、相似用户定义：购买过两种或者两种以上的相同商品。

Result:
+----------+-------------+
| user_id  | product_id  |
+----------+-------------+
| 123      | 4           |
| 456      | 3           |
| 456      | 7           |
| 456      | 8           |
| 789      | 1           |
| 789      | 3           |
+----------+-------------+


````

<details >
  <summary> 🕹 Click to show  the sql    </summary>

````sql

-- spark-sql

with tmp  as (
select distinct u1,u2 from (
    select *,count(u2) over(partition by u1,u2) c1
    from (select
            a.user_id as u1
            ,a.product_id as p1
            ,b.user_id as u2
            ,b.product_id as p2
            from orders a
            join orders b
            on a.user_id <> b.user_id
        ) aa
    where p1=p2
     ) bb
where c1 >= 2
)
select  distinct  tmp.u2 as user_id ,o.product_id as product_id
from orders o
inner join tmp
on o.user_id = tmp.u1
left anti join orders c
on tmp.u2 = c.user_id and o.product_id = c.product_id ;



````

</details>



***
