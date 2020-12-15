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

hive -e "drop table if exists default.LBMISSION_keylist"
hive -e"create table if not exists default.LBMISSION_keylist as 
select 
a.SERIALNO,
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_LBMISSION_dt a
 where 1 = 2;
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "建表失败"
   exit 1
  else
  echo "建表成功"
fi

  
hive -e "insert into default.LBMISSION_keylist
select distinct 
a.SERIALNO,
 a.OGGDATE OGGDATE
  from soochow_data.ods_lis_LBMISSION_dt a
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
hive -e "insert overwrite table default.LBMISSION_keylist
select 
temp.SERIALNO,
 temp.OGGDATE
  from (select row_number() over(partition by SERIALNO  ORDER BY OGGDATE desc) AS rank,
a.SERIALNO,
               a.OGGDATE
          from default.LBMISSION_keylist a) temp
 where temp.rank = 1;
 "

if [ $? -ne 0 ]; then
  #异常退出
  echo "表default.LBMISSION_keylist去重失败"
   exit 1
  else
  echo "表default.LBMISSION_keylist去重成功"
fi

# 3.新建增量临时表存储增量数据

#创建增量临时表
hive -e "drop table if exists default.view_dwd_LBMISSION;
create table default.view_dwd_LBMISSION like soochow_data.dwd_LBMISSION;"

if [ $? -ne 0 ]; then
  #异常退出
  echo "创建增量临时表default.view_dwd_LBMISSION失败"
   exit 1
  else
  echo "创建增量临时表default.view_dwd_LBMISSION成功"
fi


hive -e "
insert overwrite table default.view_dwd_LBMISSION
SELECT 
a.SERIALNO,
a.MISSIONID,
a.SUBMISSIONID,
a.PROCESSID,
a.ACTIVITYID,
a.ACTIVITYSTATUS,
a.MISSIONPROP1,
a.MISSIONPROP2,
a.MISSIONPROP3,
a.MISSIONPROP4,
a.MISSIONPROP5,
a.MISSIONPROP6,
a.MISSIONPROP7,
a.MISSIONPROP8,
a.MISSIONPROP9,
a.MISSIONPROP10,
a.MISSIONPROP11,
a.MISSIONPROP12,
a.MISSIONPROP13,
a.MISSIONPROP14,
a.MISSIONPROP15,
a.MISSIONPROP16,
a.MISSIONPROP17,
a.MISSIONPROP18,
a.MISSIONPROP19,
a.MISSIONPROP20,
a.DEFAULTOPERATOR,
a.LASTOPERATOR,
a.CREATEOPERATOR,
a.MAKEDATE,
a.MAKETIME,
a.MODIFYDATE,
a.MODIFYTIME,
a.INDATE,
a.INTIME,
a.OUTDATE,
a.OUTTIME,
a.MISSIONPROP21,
a.MISSIONPROP22,
a.MISSIONPROP23,
a.MISSIONPROP24,
a.MISSIONPROP25,
a.TIMEID,
a.STANDENDDATE,
a.STANDENDTIME,
a.OPERATECOM,
a.MAINMISSIONID,

	a.OGGACTION ,
	keylist.OGGDATE ,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd')  PushDate ,		
	from_unixtime(unix_timestamp(),'HH:mm:ss')  PushTime
FROM soochow_data.ods_lis_LBMISSION_dt a
	inner join default.LBMISSION_keylist keylist 
		on a.SERIALNO=keylist.SERIALNO
;" 

		
if [ $? -ne 0 ]; then
  #异常退出
  echo "从ods 提数据到dwd表dwd_LBMISSION失败"
   exit 1
  else
  echo "从ods 提数据到dwd表dwd_LBMISSION成功"
fi


#4.归档数据
hive -e "insert overwrite table soochow_data.dwd_LBMISSION
select 
tmp.SERIALNO,
tmp.MISSIONID,
tmp.SUBMISSIONID,
tmp.PROCESSID,
tmp.ACTIVITYID,
tmp.ACTIVITYSTATUS,
tmp.MISSIONPROP1,
tmp.MISSIONPROP2,
tmp.MISSIONPROP3,
tmp.MISSIONPROP4,
tmp.MISSIONPROP5,
tmp.MISSIONPROP6,
tmp.MISSIONPROP7,
tmp.MISSIONPROP8,
tmp.MISSIONPROP9,
tmp.MISSIONPROP10,
tmp.MISSIONPROP11,
tmp.MISSIONPROP12,
tmp.MISSIONPROP13,
tmp.MISSIONPROP14,
tmp.MISSIONPROP15,
tmp.MISSIONPROP16,
tmp.MISSIONPROP17,
tmp.MISSIONPROP18,
tmp.MISSIONPROP19,
tmp.MISSIONPROP20,
tmp.DEFAULTOPERATOR,
tmp.LASTOPERATOR,
tmp.CREATEOPERATOR,
tmp.MAKEDATE,
tmp.MAKETIME,
tmp.MODIFYDATE,
tmp.MODIFYTIME,
tmp.INDATE,
tmp.INTIME,
tmp.OUTDATE,
tmp.OUTTIME,
tmp.MISSIONPROP21,
tmp.MISSIONPROP22,
tmp.MISSIONPROP23,
tmp.MISSIONPROP24,
tmp.MISSIONPROP25,
tmp.TIMEID,
tmp.STANDENDDATE,
tmp.STANDENDTIME,
tmp.OPERATECOM,
tmp.MAINMISSIONID,
        tmp.OGGACTION ,
        tmp.OGGDATE ,
        tmp.PushDate ,
        tmp.PushTime 
from (
select row_number() over(partition by SERIALNO order by oggdate desc) rank ,temp.*from (
select * from soochow_data.dwd_LBMISSION
union all
select * from default.view_dwd_LBMISSION
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
hive -e "insert overwrite table soochow_data.dwd_LBMISSION
select 
tmp.SERIALNO,
tmp.MISSIONID,
tmp.SUBMISSIONID,
tmp.PROCESSID,
tmp.ACTIVITYID,
tmp.ACTIVITYSTATUS,
tmp.MISSIONPROP1,
tmp.MISSIONPROP2,
tmp.MISSIONPROP3,
tmp.MISSIONPROP4,
tmp.MISSIONPROP5,
tmp.MISSIONPROP6,
tmp.MISSIONPROP7,
tmp.MISSIONPROP8,
tmp.MISSIONPROP9,
tmp.MISSIONPROP10,
tmp.MISSIONPROP11,
tmp.MISSIONPROP12,
tmp.MISSIONPROP13,
tmp.MISSIONPROP14,
tmp.MISSIONPROP15,
tmp.MISSIONPROP16,
tmp.MISSIONPROP17,
tmp.MISSIONPROP18,
tmp.MISSIONPROP19,
tmp.MISSIONPROP20,
tmp.DEFAULTOPERATOR,
tmp.LASTOPERATOR,
tmp.CREATEOPERATOR,
tmp.MAKEDATE,
tmp.MAKETIME,
tmp.MODIFYDATE,
tmp.MODIFYTIME,
tmp.INDATE,
tmp.INTIME,
tmp.OUTDATE,
tmp.OUTTIME,
tmp.MISSIONPROP21,
tmp.MISSIONPROP22,
tmp.MISSIONPROP23,
tmp.MISSIONPROP24,
tmp.MISSIONPROP25,
tmp.TIMEID,
tmp.STANDENDDATE,
tmp.STANDENDTIME,
tmp.OPERATECOM,
tmp.MAINMISSIONID,

        case when tmp2.SERIALNO is null or length(tmp2.SERIALNO) = 0 then 'delete' else tmp.OGGACTION end as OGGACTION,
        tmp.OGGDATE ,
        tmp.PushDate ,		
        tmp.PushTime 
from soochow_data.dwd_LBMISSION tmp
left join (select SERIALNO from soochow_data.ods_lis_LBMISSION_dt) tmp2
on tmp.SERIALNO=tmp2.SERIALNO
"

if [ $? -ne 0 ]; then
  #异常退出
  echo "源端数据删除，更新整合层数据的oggaction标志失败"
   exit 1
  else
  echo "源端数据删除，更新整合层数据的oggaction标志成功"
fi

