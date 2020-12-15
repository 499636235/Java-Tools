
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
    SELECT 