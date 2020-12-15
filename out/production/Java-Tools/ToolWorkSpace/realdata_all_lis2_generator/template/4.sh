_dt"

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天

#if [ -n "$1" ] ;then
#    do_date=$1
#else
#   do_date=`date -d "-1 day" +%F`
#fi

do_date = "9999-01-01"

echo "获取数据库连接:${lis_realdata}"

sqoop import ${lis_realdata} --hive-import --hive-database ${hive_db} --hive-table ${hive_table} --fields-terminated-by '\001' --lines-terminated-by '\n' --delete-target-dir -m 1 --hive-drop-import-delims --hive-overwrite --null-string '\\N' --null-non-string '\\N' --input-null-string '\\N' --input-null-non-string '\\N' \
--query "select 