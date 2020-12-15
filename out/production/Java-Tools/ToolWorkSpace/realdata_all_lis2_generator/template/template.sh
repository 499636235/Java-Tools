#! /bin/bash

export HADOOP_USER_NAME=hdfs

lis_realdata=`hdfs dfs -cat /soochow_properties/ods/DB_Config.properties |sed '/^lis_realdata=&/!d;s/.*=&//'`

original_table="njytablename"

hive_table="ods_lis_njytablename_dt_history"

hive_db="soochow_data"

hive_latest_table="ods_lis_njytablename_dt"

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天

#if [ -n "$1" ] ;then
#    do_date=$1
#else
#   do_date=`date -d "-1 day" +%F`
#fi

do_date = "9999-01-01"

echo "获取数据库连接:${lis_realdata}"

sqoop import ${lis_realdata} --hive-import --hive-database ${hive_db} --hive-table ${hive_table} --fields-terminated-by '\001' --lines-terminated-by '\n' --delete-target-dir -m 1 --hive-drop-import-delims --hive-overwrite --null-string '\\N' --null-non-string '\\N' --input-null-string '\\N' --input-null-non-string '\\N' \
--query "select njycolumnname1
        OGGACTION, \
        OGGDATE \
 FROM ${original_table} where to_char(OGGDATE,'yyyy-MM-dd')<='${do_date}' and \$CONDITIONS " \
--hive-partition-key dt \
--hive-partition-value  ${do_date} \
--target-dir /tmp/${hive_table}

if [ $? -ne 0 ]; then 
  echo "表${hive_table}的${do_date}日前（包括该日）的数据导数失败"
else 
  echo "表${hive_table}的${do_date}日前（包括该日）的数据导数成功"
  hive -e "
    INSERT OVERWRITE TABLE ${hive_db}.${hive_latest_table}
    SELECT njycolumnname2
        OGGACTION,
        OGGDATE
    FROM  ${hive_db}.${hive_table} WHERE dt ='${do_date}';"
    if [ $? -ne 0 ];then
        echo "从${hive_table}导入历史数据(${do_date})至${hive_latest_table}最新表失败"
    else
        echo "从${hive_table}导入历史数据(${do_date})至${hive_latest_table}最新表成功"
    fi
fi

