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

hive -e "drop table if exists default.LXISTRADECONT_keylist"
hive -e"create table if not exists default.LXISTRADECONT_keylist as 
select 
a.RISKCODE,
a.CSNM,
a.DEALNO,
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_LXISTRADECONT_dt a
 where 1 = 2;
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "建表失败"
   exit 1
  else
  echo "建表成功"
fi

  
hive -e "insert into default.LXISTRADECONT_keylist
select distinct 
a.RISKCODE,
a.CSNM,
a.DEALNO,
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_LXISTRADECONT_dt a
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
hive -e "insert overwrite table default.LXISTRADECONT_keylist
select 
temp.RISKCODE,
temp.CSNM,
temp.DEALNO,
 temp.OGGDATE
  from (select row_number() over(partition by RISKCODE,CSNM,DEALNO  ORDER BY OGGDATE desc) AS rank,
a.RISKCODE,
a.CSNM,
a.DEALNO,
               a.OGGDATE
          from default.LXISTRADECONT_keylist a) temp
 where temp.rank = 1;
 "

if [ $? -ne 0 ]; then
  #异常退出
  echo "表default.LXISTRADECONT_keylist去重失败"
   exit 1
  else
  echo "表default.LXISTRADECONT_keylist去重成功"
fi

# 3.新建增量临时表存储增量数据

#创建增量临时表
hive -e "drop table if exists default.view_dwd_LXISTRADECONT;
create table default.view_dwd_LXISTRADECONT like soochow_data.dwd_LXISTRADECONT;"

if [ $? -ne 0 ]; then
  #异常退出
  echo "创建增量临时表default.view_dwd_LXISTRADECONT失败"
   exit 1
  else
  echo "创建增量临时表default.view_dwd_LXISTRADECONT成功"
fi


hive -e "
insert overwrite table default.view_dwd_LXISTRADECONT
SELECT 
a.dealno,
a.csnm,
a.istp,
a.isnm,
a.riskcode,
a.isps,
a.itnm,
a.isog,
a.isat,
a.isfe,
a.ispt,
a.ctes,

	a.OGGACTION ,
	keylist.OGGDATE ,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd')  PushDate ,		
	from_unixtime(unix_timestamp(),'HH:mm:ss')  PushTime
FROM soochow_data.ods_lis_LXISTRADECONT_dt a
	inner join default.LXISTRADECONT_keylist keylist 
		on a.RISKCODE=keylist.RISKCODE
and a.CSNM=keylist.CSNM
and a.DEALNO=keylist.DEALNO
;" 

		
if [ $? -ne 0 ]; then
  #异常退出
  echo "从ods 提数据到dwd表dwd_LXISTRADECONT失败"
   exit 1
  else
  echo "从ods 提数据到dwd表dwd_LXISTRADECONT成功"
fi


#4.归档数据
hive -e "insert overwrite table soochow_data.dwd_LXISTRADECONT
select 
tmp.dealno,
tmp.csnm,
tmp.istp,
tmp.isnm,
tmp.riskcode,
tmp.isps,
tmp.itnm,
tmp.isog,
tmp.isat,
tmp.isfe,
tmp.ispt,
tmp.ctes,
        tmp.OGGACTION ,
        tmp.OGGDATE ,
        tmp.PushDate ,
        tmp.PushTime 
from (
select row_number() over(partition by RISKCODE,CSNM,DEALNO order by oggdate desc) rank ,temp.*from (
select * from soochow_data.dwd_LXISTRADECONT
union all
select * from default.view_dwd_LXISTRADECONT
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
hive -e "insert overwrite table soochow_data.dwd_LXISTRADECONT
select 
tmp.dealno,
tmp.csnm,
tmp.istp,
tmp.isnm,
tmp.riskcode,
tmp.isps,
tmp.itnm,
tmp.isog,
tmp.isat,
tmp.isfe,
tmp.ispt,
tmp.ctes,

        case when tmp2.RISKCODE is null or length(tmp2.RISKCODE) = 0 then 'delete' else tmp.OGGACTION end as OGGACTION,
        tmp.OGGDATE ,
        tmp.PushDate ,		
        tmp.PushTime 
from soochow_data.dwd_LXISTRADECONT tmp
left join (select RISKCODE from soochow_data.ods_lis_LXISTRADECONT_dt) tmp2
on tmp.RISKCODE=tmp2.RISKCODE
and tmp.CSNM=tmp2.CSNM
and tmp.DEALNO=tmp2.DEALNO
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "源端数据删除，更新整合层数据的oggaction标志失败"
   exit 1
  else
  echo "源端数据删除，更新整合层数据的oggaction标志成功"
fi

