````shell
table: ods_games
+--------+--------------+
| games  | games_price  |
+--------+--------------+
| A      | 29.3300      |
| B      | 19.2200      |
| C      | 25.8100      |
| D      | 16.7900      |
| E      | 20.7800      |
| F      | 25.3200      |
+--------+--------------+

````

游戏打包销售折扣
销售平台进行游戏打包促销。将任意个游戏打包为一组，根据游戏数量制定折扣。
打包的游戏数量限定2个至4个。当包含2个游戏时折扣为9折，3个时8折，4个时6折。
问一：计算有多少种(个数)不同的打包组合方式  


```
C(6,2)+C(6,3)+C(6,4)
```
第一次遇到此类问题，记录一下！ (代码只是个人解题思路，并非标准答案,用自关联的方式可能更常规)

````sql
-- spark-sql
select  count(1) from 
(
    select *,nvl(A,0)+nvl(B,0)+nvl(C,0)+nvl(D,0)+nvl(E,0)+nvl(F,0) as cnt  
    from (
             select games  from ods_games
         )a 
    pivot( count(games) for games in ('A','B','C','D','E','F') ) 
    group by A,B,C,D,E,F with cube 
)  aa where cnt in (2,3,4);

````

问二：计算不同数量游戏打包时，价格最高的各游戏名称及其折扣后价格。
即分别为2/3/4个游戏组合时，打包折扣价最高的组合中各个游戏名称，及其打包折扣价。

````sql
--spark-sql
with tmp as (
    select *,nvl(A,0)+nvl(B,0)+nvl(C,0)+nvl(D,0)+nvl(E,0)+nvl(F,0) as cnt  
    from (
             select games  from ods_games
         )a 
    pivot( count(games) for games in ('A','B','C','D','E','F') ) 
    group by A,B,C,D,E,F with cube 
)
select games,sum_amt from 
(
select  
    games,sum_amt
    ,row_number() over (partition by cnt order by  sum_amt desc ) as rn  from 
(
 select
 concat(if(tmp.A is null ,'','A'),if(tmp.B is null,'','B'),if(tmp.C is null,'','C'),if(tmp.D is null,'','D'),if(tmp.E is null,'','E'),if( tmp.F is null ,'','F')) as games
   , case 
 when tmp.cnt = 2 then 0.9*(t1.A*nvl(tmp.A,0)+t1.B*nvl(tmp.B,0)+t1.C*nvl(tmp.C,0)+t1.D*nvl(tmp.D,0)+t1.E*nvl(tmp.E,0)+t1.F*nvl(tmp.F,0))  
    when tmp.cnt = 3 then 0.8*(t1.A*nvl(tmp.A,0)+t1.B*nvl(tmp.B,0)+t1.C*nvl(tmp.C,0)+t1.D*nvl(tmp.D,0)+t1.E*nvl(tmp.E,0)+t1.F*nvl(tmp.F,0)) 
    when tmp.cnt = 4 then 0.6*(t1.A*nvl(tmp.A,0)+t1.B*nvl(tmp.B,0)+t1.C*nvl(tmp.C,0)+t1.D*nvl(tmp.D,0)+t1.E*nvl(tmp.E,0)+t1.F*nvl(tmp.F,0))  
 end as sum_amt ,
 tmp.cnt
 from ( select * from tmp  where cnt in (2,3,4) ) tmp  
 left join (
    select *
    from ods_games
    pivot( sum(games_price) for games in ('A','B','C','D','E','F') ) 
 ) t1 
 ) t2 
) t3 where rn = 1 
 ;

````

