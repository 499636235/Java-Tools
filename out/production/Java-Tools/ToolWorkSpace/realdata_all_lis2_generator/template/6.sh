
        OGGACTION,
        OGGDATE
    FROM  ${hive_db}.${hive_table} WHERE dt ='${do_date}';"
    if [ $? -ne 0 ];then
        echo "从${hive_table}导入历史数据(${do_date})至${hive_latest_table}最新表失败"
    else
        echo "从${hive_table}导入历史数据(${do_date})至${hive_latest_table}最新表成功"
    fi
fi

