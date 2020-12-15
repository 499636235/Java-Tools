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

hive -e "drop table if exists default.LARATEBARWERTPREM_keylist"
hive -e"create table if not exists default.LARATEBARWERTPREM_keylist as 
select 
a.SERIALNO,
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_LARATEBARWERTPREM_dt a
 where 1 = 2;
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "建表失败"
   exit 1
  else
  echo "建表成功"
fi

  
hive -e "insert into default.LARATEBARWERTPREM_keylist
select distinct 
a.SERIALNO,
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_LARATEBARWERTPREM_dt a
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
hive -e "insert overwrite table default.LARATEBARWERTPREM_keylist
select 
temp.SERIALNO,
 temp.OGGDATE
  from (select row_number() over(partition by SERIALNO  ORDER BY OGGDATE desc) AS rank,
a.SERIALNO,
               a.OGGDATE
          from default.LARATEBARWERTPREM_keylist a) temp
 where temp.rank = 1;
 "

if [ $? -ne 0 ]; then
  #异常退出
  echo "表default.LARATEBARWERTPREM_keylist去重失败"
   exit 1
  else
  echo "表default.LARATEBARWERTPREM_keylist去重成功"
fi

# 3.新建增量临时表存储增量数据

#创建增量临时表
hive -e "drop table if exists default.view_dwd_LARATEBARWERTPREM;
create table default.view_dwd_LARATEBARWERTPREM like soochow_data.dwd_LARATEBARWERTPREM;"

if [ $? -ne 0 ]; then
  #异常退出
  echo "创建增量临时表default.view_dwd_LARATEBARWERTPREM失败"
   exit 1
  else
  echo "创建增量临时表default.view_dwd_LARATEBARWERTPREM成功"
fi


hive -e "
insert overwrite table default.view_dwd_LARATEBARWERTPREM
SELECT 
a.SERIALNO,
a.BRANCHTYPE,
a.RISKCODE,
a.SEX,
a.APPAGE,
a.YEAR,
a.PAYINTV,
a.CURYEAR,
a.F01,
a.F02,
a.F03,
a.F04,
a.F05,
a.F06,
a.RATE,
a.OPERATOR,
a.MAKEDATE,
a.MAKETIME,
a.MODIFYDATE,
a.MODIFYTIME,
a.MANAGECOM,
a.BRANCHTYPE2,
a.STARTDATE,
a.ENDDATE,

	a.OGGACTION ,
	keylist.OGGDATE ,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd')  PushDate ,		
	from_unixtime(unix_timestamp(),'HH:mm:ss')  PushTime
FROM soochow_data.ods_lis_LARATEBARWERTPREM_dt a
	inner join default.LARATEBARWERTPREM_keylist keylist 
		on a.SERIALNO=keylist.SERIALNO
;" 

		
if [ $? -ne 0 ]; then
  #异常退出
  echo "从ods 提数据到dwd表dwd_LARATEBARWERTPREM失败"
   exit 1
  else
  echo "从ods 提数据到dwd表dwd_LARATEBARWERTPREM成功"
fi


#4.归档数据
hive -e "insert overwrite table soochow_data.dwd_LARATEBARWERTPREM
select 
tmp.SERIALNO,
tmp.BRANCHTYPE,
tmp.RISKCODE,
tmp.SEX,
tmp.APPAGE,
tmp.YEAR,
tmp.PAYINTV,
tmp.CURYEAR,
tmp.F01,
tmp.F02,
tmp.F03,
tmp.F04,
tmp.F05,
tmp.F06,
tmp.RATE,
tmp.OPERATOR,
tmp.MAKEDATE,
tmp.MAKETIME,
tmp.MODIFYDATE,
tmp.MODIFYTIME,
tmp.MANAGECOM,
tmp.BRANCHTYPE2,
tmp.STARTDATE,
tmp.ENDDATE,
        tmp.OGGACTION ,
        tmp.OGGDATE ,
        tmp.PushDate ,
        tmp.PushTime 
from (
select row_number() over(partition by SERIALNO order by oggdate desc) rank ,temp.*from (
select * from soochow_data.dwd_LARATEBARWERTPREM
union all
select * from default.view_dwd_LARATEBARWERTPREM
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
hive -e "insert overwrite table soochow_data.dwd_LARATEBARWERTPREM
select 
tmp.SERIALNO,
tmp.BRANCHTYPE,
tmp.RISKCODE,
tmp.SEX,
tmp.APPAGE,
tmp.YEAR,
tmp.PAYINTV,
tmp.CURYEAR,
tmp.F01,
tmp.F02,
tmp.F03,
tmp.F04,
tmp.F05,
tmp.F06,
tmp.RATE,
tmp.OPERATOR,
tmp.MAKEDATE,
tmp.MAKETIME,
tmp.MODIFYDATE,
tmp.MODIFYTIME,
tmp.MANAGECOM,
tmp.BRANCHTYPE2,
tmp.STARTDATE,
tmp.ENDDATE,

        case when tmp2.SERIALNO is null or length(tmp2.SERIALNO) = 0 then 'delete' else tmp.OGGACTION end as OGGACTION,
        tmp.OGGDATE ,
        tmp.PushDate ,		
        tmp.PushTime 
from soochow_data.dwd_LARATEBARWERTPREM tmp
left join (select SERIALNO from soochow_data.ods_lis_LARATEBARWERTPREM_dt) tmp2
on tmp.SERIALNO=tmp2.SERIALNO
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "源端数据删除，更新整合层数据的oggaction标志失败"
   exit 1
  else
  echo "源端数据删除，更新整合层数据的oggaction标志成功"
fi

