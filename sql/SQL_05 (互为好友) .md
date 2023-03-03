> 1
````shell
table: user
+-------+---------+
| user  | friend  |
+-------+---------+
| B     | A,C,D   |
| A     | B,C,D   |
| C     | A,B     |
+-------+---------+

-- 已知用户好友表user ,求互为共同好友一共有多少对。
--（B的好友包含A，A的好友包含B ,AB为一对）

Result:
+------+
| cnt  |
+------+
| 3    |
+------+


````

<details >
  <summary> 🎇 Click to show  the sql    </summary>

````sql

-- spark-sql

select  
    count(tag) - count( distinct tag )  as cnt
from (
    select 
        if(user<f ,concat_ws('-',user,f),concat_ws('-',f,user))  as tag
    from user 
    lateral view explode(split(friend,',')) as f
) a  ;



````

</details>



***
