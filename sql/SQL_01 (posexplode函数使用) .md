> 1 posexplode函数使用

````shell
table: t_abc
+------------+------------+----+
|     a      |     b      | c  |
+------------+------------+----+
| 2022/4/1   | 2022/4/2   | 2  |
| 2022/6/16  | 2022/6/17  | 3  |
+------------+------------+----+


要求：按照c列的数值将该行数据复制多份结果如下图所示：

+------------+------------+----+
|     a      |     b      | c  |
+------------+------------+----+
| 2022/4/1   | 2022/4/2   | 2  |
| 2022/4/1   | 2022/4/2   | 2  |
| 2022/6/16  | 2022/6/17  | 3  |
| 2022/6/16  | 2022/6/17  | 3  |
| 2022/6/16  | 2022/6/17  | 3  |
+------------+------------+----+

````

<details>
  <summary> 🚀 Click to show  the sql    </summary>

````sql

-- spark-sql

select 
    a,b,c
from t_abc
lateral view posexplode(split(space(c-1),'')) tab as pos,val;



````

</details>



***

> 2
````shell
table: ods_contractinfo
+--------------+-------------+------------+-------+
| contract_ID  |    tdate    |   amount   | Term  |
+--------------+-------------+------------+-------+
| AAAA         | 2018-12-21  | 1001.0000  | 12    |
| AAAA         | 2019-03-21  | 2001.0000  | 12    |
| AAAA         | 2019-06-21  | 3001.0000  | 12    |
| AAAA         | 2019-11-21  | 4001.0000  | 12    |
| BBBB         | 2018-12-02  | 1005.0000  | 10    |
| BBBB         | 2019-02-02  | 2006.0000  | 10    |
| BBBB         | 2019-06-02  | 3007.0000  | 10    |
| BBBB         | 2019-09-02  | 4008.0000  | 10    |
+--------------+-------------+------------+-------+



要求：将表ods_contractinfo转换成如下格式, amount 为截止当前日期得累计值。

+--------------+-------------+-------------+-------+
| contract_ID  |    tdate    |   amount    | term  |
+--------------+-------------+-------------+-------+
| AAAA         | 2018-12-21  | 1001.0000   | 12    |
| AAAA         | 2019-01-21  | 1001.0000   | 12    |
| AAAA         | 2019-02-21  | 1001.0000   | 12    |
| AAAA         | 2019-03-21  | 3002.0000   | 12    |
| AAAA         | 2019-04-21  | 3002.0000   | 12    |
| AAAA         | 2019-05-21  | 3002.0000   | 12    |
| AAAA         | 2019-06-21  | 6003.0000   | 12    |
| AAAA         | 2019-07-21  | 6003.0000   | 12    |
| AAAA         | 2019-08-21  | 6003.0000   | 12    |
| AAAA         | 2019-09-21  | 6003.0000   | 12    |
| AAAA         | 2019-10-21  | 6003.0000   | 12    |
| AAAA         | 2019-11-21  | 10004.0000  | 12    |
| BBBB         | 2018-12-02  | 1005.0000   | 10    |
| BBBB         | 2019-01-02  | 1005.0000   | 10    |
| BBBB         | 2019-02-02  | 3011.0000   | 10    |
| BBBB         | 2019-03-02  | 3011.0000   | 10    |
| BBBB         | 2019-04-02  | 3011.0000   | 10    |
| BBBB         | 2019-05-02  | 3011.0000   | 10    |
| BBBB         | 2019-06-02  | 6018.0000   | 10    |
| BBBB         | 2019-07-02  | 6018.0000   | 10    |
| BBBB         | 2019-08-02  | 6018.0000   | 10    |
| BBBB         | 2019-09-02  | 10026.0000  | 10    |
+--------------+-------------+-------------+-------+



````

<details>
  <summary> 🚀 Click to show  the sql    </summary>

````sql

-- spark-sql

with  cte as (
select  contract_ID, add_months(tdate,pos)  as tdate,term from
(
select contract_ID, min(tdate) tdate,term
from ods_contractinfo
group by contract_ID, term
) a
lateral view posexplode(split(space(term-1),'')) tab as pos,val
)
select
   contract_ID
  ,tdate
  ,sum(amount ) over (partition by contract_ID order by tdate) as amount
  ,term
  from (
select
   cte.contract_ID
  ,cte.tdate
  ,a.amount
  ,cte.term
from cte
left join ods_contractinfo a
on cte.contract_ID = a.contract_ID and a.tdate = cte.tdate
) b order by contract_ID
;



````

</details>



***
