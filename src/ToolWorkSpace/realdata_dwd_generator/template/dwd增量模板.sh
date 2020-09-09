#! /bin/bash

export HADOOP_USER_NAME=hdfs


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
  start_date=$1
else 
  start_date=`date -d "-1 day" +%F`  
fi

echo "增量提数开始时间:${start_date}"


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
	end_date=$2
else 
	end_date=`date -d "-1 day" +%F`  
fi

echo "增量提数结束时间:${end_date}"


# 1.提取增量数据对应主表主键和增量数据变化时间

hive -e "drop table if exists default.
[partdivider]########################################################表名
_keylist"
hive -e"create table if not exists default.
[partdivider]########################################################表名
_keylist as 
select 

[partdivider]########################################################主键list-a
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_
[partdivider]########################################################表名
_dt a
 where 1 = 2;
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "建表失败"
   exit 1
  else
  echo "建表成功"
fi

  
hive -e "insert into default.
[partdivider]########################################################表名
_keylist
select distinct 

[partdivider]########################################################主键list-a
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_
[partdivider]########################################################表名
_dt a
 where a.OGGDATE between '$start_date' and '$end_date';
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "增量主表识别主键失败"
   exit 1
  else
  echo "增量主表识别主键成功"
fi


# 2.增量对应主表主键根据数据变化时间去重
hive -e "insert overwrite table default.
[partdivider]########################################################表名
_keylist
select 

[partdivider]########################################################主键list-temp
 temp.OGGDATE
  from (select row_number() over(partition by 
[partdivider]########################################################主键list
  ORDER BY OGGDATE desc) AS rank,

[partdivider]########################################################主键list-a
               a.OGGDATE
          from default.
[partdivider]########################################################表名
_keylist a) temp
 where temp.rank = 1;
 "

if [ $? -ne 0 ]; then
  #异常退出
  echo "表default.
[partdivider]########################################################表名
_keylist去重失败"
   exit 1
  else
  echo "表default.
[partdivider]########################################################表名
_keylist去重成功"
fi

# 3.新建增量临时表存储增量数据

#创建增量临时表
hive -e "drop table if exists default.view_dwd_
[partdivider]########################################################表名
;
create table default.view_dwd_
[partdivider]########################################################表名
 like soochow_data.dwd_
[partdivider]########################################################表名
;"

if [ $? -ne 0 ]; then
  #异常退出
  echo "创建增量临时表default.view_dwd_
[partdivider]########################################################表名
失败"
   exit 1
  else
  echo "创建增量临时表default.view_dwd_
[partdivider]########################################################表名
成功"
fi


hive -e "
insert overwrite table default.view_dwd_
[partdivider]########################################################表名

SELECT 

[partdivider]########################################################列名-a

	a.OGGACTION ,
	keylist.OGGDATE ,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd')  PushDate ,		
	from_unixtime(unix_timestamp(),'HH:mm:ss')  PushTime
FROM soochow_data.ods_lis_
[partdivider]########################################################表名
_dt a
	inner join default.
[partdivider]########################################################表名
_keylist keylist 
		on 
[partdivider]########################################################主键list a=keylist
;" 

		
if [ $? -ne 0 ]; then
  #异常退出
  echo "从ods 提数据到dwd表dwd_
[partdivider]########################################################表名
失败"
   exit 1
  else
  echo "从ods 提数据到dwd表dwd_
[partdivider]########################################################表名
成功"
fi


#4.归档数据
hive -e "insert overwrite table soochow_data.dwd_
[partdivider]########################################################表名

select 

[partdivider]########################################################列名-tmp
        tmp.OGGACTION ,
        tmp.OGGDATE ,
        tmp.PushDate ,
        tmp.PushTime 
from (
select row_number() over(partition by 
[partdivider]########################################################主键list
 order by oggdate desc) rank ,temp.*from (
select * from soochow_data.dwd_
[partdivider]########################################################表名

union all
select * from default.view_dwd_
[partdivider]########################################################表名

)temp
)tmp where tmp.rank=1"

if [ $? -ne 0 ]; then
  #异常退出
  echo "归档失败"
   exit 1
  else
  echo "归档成功"
fi

#5.源端数据删除，更新整合层数据的oggaction标志为删除
hive -e "insert overwrite table soochow_data.dwd_
[partdivider]########################################################表名

select 

[partdivider]########################################################列名-tmp

        case when tmp2.
[partdivider]########################################################主键1
 is null or length(tmp2.
[partdivider]########################################################主键1
) = 0 then 'delete' else tmp.OGGACTION end as OGGACTION,
        tmp.OGGDATE ,
        tmp.PushDate ,		
        tmp.PushTime 
from soochow_data.dwd_
[partdivider]########################################################表名
 tmp
left join (select 
[partdivider]########################################################主键1
 from soochow_data.ods_lis_
[partdivider]########################################################表名
_dt) tmp2
on 
[partdivider]########################################################主键list tmp=tmp2
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "源端数据删除，更新整合层数据的oggaction标志失败"
   exit 1
  else
  echo "源端数据删除，更新整合层数据的oggaction标志成功"
fi

[partdivider]########################################################