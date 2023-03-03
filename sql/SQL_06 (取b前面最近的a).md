````shell
table: t
+------+-----+
| str  | id  |
+------+-----+
| a    | 10  |
| a    | 13  |
| b    | 14  |
| b    | 17  |
| b    | 19  |
| a    | 20  |
| b    | 22  |
+------+-----+

 按id自增排序取出每条str='b' 前面一条最近的a的数据，结果如下：

Result:
+------+-----+------+-----+
| str  | id  | str_2| id_2|
+------+-----+------+-----+
| b    | 14  |  a   | 13  |
| b    | 17  |  a   | 13  |
| b    | 19  |  a   | 13  |
| b    | 22  |  a   | 20  |
+------+-----+------+-----+


````

<details >
  <summary> ✨ Click to show  the sql    </summary>

````sql

-- impala 
-- 方法一

 select 
    str,id,'a' as str_2, m id_2
from (
    select 
        str,id ,max( if(str= 'a',id,0) ) over( order by id  rows between unbounded preceding and 1 preceding ) m
    from t 
    order by id 
) a where str = 'b' ; 


-- impala 
-- 方法二

 select 
    str,id,'a' as str_2, lv id_2
from (
    select 
        str,id ,last_value( if(str= 'a',id,null) ignore nulls  ) over( order by id ) lv
    from t 
    order by id 
) a where str = 'b' ; 


我认为两种方式在思维方式上还是一致的。



````

</details>



***