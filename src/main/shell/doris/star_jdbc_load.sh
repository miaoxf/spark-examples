#!/usr/bin/env bash
source /etc/profile

testMode="_test_by_xuefei"
today=`date -d "0 days" +"%Y%m%d"`
file_format="orc"
db_name="hive_vipvpe"
dt=`date -d "$today -1 day" +"%Y%m%d"`
mysql_exec="mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e"
date_dt=$1
db_name=$2
tbl_name=$3
#"hdfs://bipcluster03/bip/hive_warehouse/hive_vipvpe.db/ads_mer_item_history_multi_hand_price_df/dt=${date_dt}/*"
hdfs_path=$4
export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0


truncate_partition(){
  local tbl_name=$1
  # such as p20211009
  local partition="p$2"
  local sql="TRUNCATE TABLE $tbl_name PARTITION($partition);"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"
}

add_partition(){
  local tbl_name=$1
  # such as p20211009
  local partition="p$2"
  local date_time=$3
  local exec_time=$4
  local sql="alter TABLE $tbl_name add PARTITION($partition) VALUES [('${date_time}'), ('${exec_time}'));"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"
}

partition_exists(){
  local tbl_name=$1
  # such as p20211009
  local partition=$2
  local sql="show create table $tbl_name"
  local ret=`mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"`
  local p_str=`echo $ret | grep $partition`
  if [[ -z $p_str ]];then
    # 分区不存在
    return 1
  else
    return 0
  fi
}

get_field(){
  # 只去掉了dt
  local db_tbl_name=$1
  # hive会有乱码
  local cols=`$SPARK_HOME/bin/spark-sql --master yarn --deploy-mode client --queue root.basic_platform.critical --driver-memory 4G --executor-memory 4G --executor-cores 2 -e "show columns in $db_tbl_name"`
  column_str=`echo $col | sed 's/ /,/g'`
}

get_partition_location(){
  local db_tbl_name=$1
  local date_dt=$2
  # 考虑到分流的影响，此处必须是分区的location
  part_location=`hive -e "desc formatted $db_tbl_name partition(dt=$date_dt);" | grep Location | awk '{print $NF}'`
  local num_of_partitions=`hive -e "show partitions ${db_tbl_name}" | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'`
  if ((num_of_partitions>0));then
    for i in `seq 1 ${num_of_partitions}`
      do
        part_location="${part_location}/*"
    done
  fi
  echo "[Info] current file_path: $part_location"
}

load_one_partition(){
  local date_dt=$1
  local file_path=$2
  # 判断分区是否存在，如果存在，则先truncate分区的数据
  local tbl_name="ads_mer_item_history_multi_hand_price_df${testMode}"
  local part_name="p${date_dt}"
  echo "[Info] date_dt: $date_dt, tbl_name: $tbl_name, part_name: $part_name"

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
  echo "label: load_${tbl_name}_${currentTS}"
  echo "current-currentTS:$current-$currentTS'"

  get_partition_location "$db_name.$tbl_name" $date_dt

  local load_sql="
    use ${db_name};
    LOAD LABEL ${db_name}.load_${tbl_name}_${date_dt}_${currentTS}
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
  echo "[Info] current load_sql: $load_sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$load_sql"
}

# todo 如果有数据，需要先删掉 check
# todo 支持update
get_field "$db_name.$tbl_name"
load_one_partition $date_dt $hdfs_path



