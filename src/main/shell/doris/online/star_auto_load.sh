#!/usr/bin/env bash
source /etc/profile

today=`date -d "0 days" +"%Y%m%d"`
file_format="orc"
db_name="hive_vipvpe"
dt=`date -d "$today -1 day" +"%Y%m%d"`
mysql_exec="mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e"
date_dt=`date -d @$1 +%Y%m%d`
db_name=$2
starrocks_tbl_name=$3
preload_tbl_name=$4
if [[ $5 == "true" ]];then
  testMode="_test_by_xuefei"
  # 判断表是否存在，创建表
  test_mode_sql="create table if not exists ${db_name}.${starrocks_tbl_name}${testMode}
                 like ${db_name}.${starrocks_tbl_name}"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$test_mode_sql"
else
  testMode=""
fi

export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

pre_create_db_tab(){
    mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e
}

truncate_tbl(){
  # 该方法中的表是starrocks中的表
  local tbl_name=$1
  local sql="TRUNCATE TABLE $tbl_name;"
  echo "[INFO] starRocks中非分区表数据，删掉原有的数据，sql:$sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"
}

truncate_partition(){
  # 该方法中的表是starrocks中的表
  local tbl_name=$1
  # such as p20211009
  local partition="$2"
  local sql="TRUNCATE TABLE $tbl_name PARTITION $partition;"
  echo "[INFO] starRocks中分区已经存在，删掉原有的数据，sql:$sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"
}

add_partition(){
  # 该方法中的表是starrocks中的表
  local tbl_name=$1
  # such as p20211009
  local partition="$2"
  local date_time=$3
  local next_day=`date -d "$date_time +1 day" +"%Y%m%d"`
  local sql="alter TABLE $tbl_name add PARTITION $partition VALUES [('${date_time}'), ('${next_day}'));"
  echo "[Debug] starRocks中分区不存在，创建分区，sql:$sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"
}

partition_exists(){
  # 该方法中的表是starrocks中的表
  local tbl_name=$1
  # such as p20211009
  local partition=$2
  local sql="show create table $tbl_name"
  local ret=`mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"`
  local p_str=`echo $ret | grep $partition`
  if [[ -z $p_str ]];then
    # 分区不存在
    echo "[Debug | starRocks] $partition 不存在"
    return 1
  else
    echo "[Debug | starRocks] $partition 存在"
    return 0
  fi
}

get_field(){
  # 这边如果有preload表，应该是preload表的元数据
  # 只去掉了dt
  local db_tbl_name=$1
  local field_to_be_removed=$2
  # hive会有乱码
  local cols=`$SPARK_HOME/bin/spark-sql --master local[1] --driver-memory 4G -e "show columns in $db_tbl_name"`
  col_arr=(`echo $cols`)
  column_str=""
  for i in ${col_arr[*]}
  do
    if [[ -n "$field_to_be_removed" && "$i" == "$field_to_be_removed" ]];then
      continue
    elif [[ -z $i ]]; then
      continue
    else
      # echo $i
      if [[ -z $column_str ]];then
        column_str=$i
      else
        column_str="$column_str,$i"
      fi
    fi
  done
  #column_str=`echo $cols | grep  | sed 's/ /,/g'`
}

get_partition_location(){
  # 该方法中的表是preload的表
  local db_tbl_name=$1
  local date_dt=$2

  num_of_partitions=`hive -e "show partitions ${db_tbl_name}" | grep '=' | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'`
  if [[ -z $num_of_partitions ]];then
    ((num_of_partitions=0))
  fi
  echo "[Info] num_of_partitions: $num_of_partitions"

  # 考虑到分流的影响，此处必须是分区的location
  if (( num_of_partitions == 0 ));then
    part_location=`hive -e "desc formatted $db_tbl_name;" | grep Location | awk '{print $NF}'`
  else
    part_location=`hive -e "desc formatted $db_tbl_name partition(dt=$date_dt);" | grep Location | awk '{print $NF}'`
  fi
  if [[ -z $part_location ]];then
    echo "[Error] source表中分区[dt=$date_dt]不存在！"
    exit 1
  fi

  if ((num_of_partitions > 0));then
    for i in `seq 1 ${num_of_partitions}`
      do
        part_location="${part_location}/*"
    done
  else
    part_location="${part_location}/*"
  fi
  echo "[Info] current source_path: $part_location"
}

load_one_partition(){
  local date_dt=$1
  # 判断分区是否存在，如果存在，则先truncate分区的数据
  local tbl_name="$2${testMode}"
  local part_name="p${date_dt}"
  local pre_load_tbl=$3
  echo "[Info] date_dt: $date_dt, tbl_name: $tbl_name, part_name: $part_name, pre_load_tbl: $pre_load_tbl"

  partition_exists "$db_name.$tbl_name" $part_name
  if [[ `echo $?` -ne 0 ]];then
    # 分区不存在,添加分区
    add_partition "$db_name.$tbl_name" $part_name $date_dt $today
  else
    # 分区已存在，truncate原有分区的数据
    truncate_partition "$db_name.$tbl_name" $part_name
  fi

  # load数据
  local current=`date "+%Y-%m-%d %H:%M:%S"`
  local currentTS=`date -d "$current" +%s`
  echo "[INFO] label: load_${tbl_name}_${currentTS}"
  echo "[INFO] current-currentTS:$current-$currentTS'"

  local label_name="load_${tbl_name}_${date_dt}_${currentTS}"

  local load_sql="
    use ${db_name};
    LOAD LABEL ${db_name}.${label_name}
    (
        DATA INFILE('$part_location')
        INTO TABLE ${tbl_name}
        PARTITION (p${date_dt})
        FORMAT AS '$file_format'
        (
            $column_str
        )
        SET (dt = '${date_dt}')
    )
    WITH BROKER hdfs_broker ('username'='hdfs', 'password'='hdfs')
    PROPERTIES
    (
        'timeout' = '3600'
    );"
  echo "[DEBUG] current load_sql: $load_sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$load_sql"

  # 以下是更新load_label到starrocks_auto_load
  local label_num=`mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    select count(1) as count_value from starrocks_auto_load where dest_tbl='$db_name.$tbl_name'
     and date_dt='$date_dt';
  " | grep -v count_value`
  ((label_num=label_num+1))
  mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    insert into starrocks_auto_load (src_db,src_tbl,dest_db,dest_tbl,part,
    date_dt,load_label,load_status,load_times) values
    ('$db_name','$db_name.$pre_load_tbl','$db_name','$db_name.$tbl_name','$part_name',
    '$date_dt','$label_name',1,$label_num);
  "
}

load_non_partitioned(){
  local date_dt=$1
  # 判断分区是否存在，如果存在，则先truncate分区的数据
  local tbl_name="$2${testMode}"
  local part_name="p${date_dt}"
  local pre_load_tbl=$3
  echo "[Info] date_dt: $date_dt, tbl_name: $tbl_name, part_name: $part_name, pre_load_tbl: $pre_load_tbl"

  # truncate data
  truncate_tbl "$db_name.$tbl_name"

  # load数据
  local current=`date "+%Y-%m-%d %H:%M:%S"`
  local currentTS=`date -d "$current" +%s`
  echo "[INFO] label: load_${tbl_name}_${currentTS}"
  echo "[INFO] current-currentTS:$current-$currentTS'"

  local label_name="load_${tbl_name}_${date_dt}_${currentTS}"
  local load_sql="
    use ${db_name};
    LOAD LABEL ${db_name}.${label_name}
    (
        DATA INFILE('$part_location')
        INTO TABLE ${tbl_name}
        FORMAT AS '$file_format'
        (
            $column_str
        )
    )
    WITH BROKER hdfs_broker ('username'='hdfs', 'password'='hdfs')
    PROPERTIES
    (
        'timeout' = '3600'
    );"
  echo "[DEBUG] current load_sql: $load_sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$load_sql"
  if [[ $? -ne 0 ]];then
    echo "[ERROR] Load failed! Skip inserting load_label into starrocks_auto_load!"
  fi
  # 以下是更新load_label到starrocks_auto_load
  local label_num=`mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    select count(1) as count_value from starrocks_auto_load where dest_tbl='$db_name.$tbl_name'
    and date_dt='$date_dt';
  " | grep -v count_value`
  ((label_num=label_num+1))
  mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    insert into starrocks_auto_load (src_db,src_tbl,dest_db,dest_tbl,part,
    date_dt,load_label,load_status,load_times) values
    ('$db_name','$db_name.$pre_load_tbl','$db_name','$db_name.$tbl_name','$part_name',
    '$date_dt','$label_name',1,$label_num);
  "
}

# todo 支持update
get_partition_location "$db_name.$preload_tbl_name" $date_dt
if (( num_of_partitions == 1 ));then
  # 一级分区load
  get_field "$db_name.$preload_tbl_name" "dt"
  load_one_partition $date_dt $starrocks_tbl_name $preload_tbl_name
elif (( num_of_partitions == 0 )); then
  # 非分区load
  get_field "$db_name.$preload_tbl_name"
  load_non_partitioned $date_dt $starrocks_tbl_name $preload_tbl_name
fi

