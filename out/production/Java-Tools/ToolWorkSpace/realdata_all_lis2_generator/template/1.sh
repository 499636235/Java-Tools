#! /bin/bash

export HADOOP_USER_NAME=hdfs

lis_realdata=`hdfs dfs -cat /soochow_properties/ods/DB_Config.properties |sed '/^lis_realdata=&/!d;s/.*=&//'`

original_table="