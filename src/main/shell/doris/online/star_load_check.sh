#!/bin/bash
# 该脚本会check具体数据日期的数据

date_dt=`date -d @$1 +%Y%m%d`
# hive_vipvpe
global_db_name=$2
# ads_mer_item_history_multi_hand_price_df
global_tbl_name=$3
preload_tbl_name=$4


get_partition_num(){
  # 该方法中的表是preload的表
  local db_tbl_name=$1

  num_of_partitions=`hive -e "show partitions ${db_tbl_name}" | grep '=' | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'`
  if [[ -z $num_of_partitions ]];then
    ((num_of_partitions=0))
  fi
  export num_of_partitions=$num_of_partitions
  echo "[Info] num_of_partitions: $num_of_partitions"
}

wait_and_listen(){
  # starrocks的库和表
  local db_name=$1
  local tbl_name=$2
  local dest_tbl="$db_name.$tbl_name"
  # 数据日期
  local inner_dt=$3
  # get load_label
  local load_label_sql="
    select load_label from starrocks_auto_load
    where dest_tbl='$dest_tbl' and date_dt='$inner_dt'
    order by load_times desc limit 1;"
  export load_label=`mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    $load_label_sql" | grep -v 'load_label'`

  echo "[INFO] load_label:$load_label"
  if [[ -z $load_label ]];then
    echo "[INFO] load_label_sql: $load_label_sql"
    echo "[WARN] load_label is empty"
    exit 0
  fi

  # load_status如果是2，则不用check
  local check_status_sql="
    select load_status from starrocks_auto_load where load_label='$load_label'
  "
  local init_load_status=`mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
    $check_status_sql" | grep -v load_status`
  if ((init_load_status == 1));then
    echo "[INFO] init_load_status is 1, start to check latest load_status."
  elif ((init_load_status == 2)); then
    echo "[INFO] init_load_status is 2, do not need to check."
    exit 0
  else
    echo "[ERROR] init_load_status is $init_load_status."
    exit 1
  fi
  # wait and listen
  local i=1
  while ((i< 20))
  do
    local result=`mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e"
    use $db_name;
    SHOW LOAD FROM $db_name WHERE LABEL = '$load_label';" | sed -n '2p' | awk '{print $3}'`
    if [[ $result = 'FINISHED' ]]
    then
          echo "[INFO] Attemp $i,load status: $result"
          echo "[INFO] load[load_label:$load_label] FINISHED!"
          break
    elif [[ $result = 'CANCELLED' ]]; then
          echo "[INFO] Attemp $i,load status: $result"
          echo "[ERROR] load[load_label:$load_label] CANCELLED!"
          # load CANCELLED, update mysql
          mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
            update starrocks_auto_load set load_status=3 where load_label=$load_label;
          "
          exit 2
    else
          echo "[INFO] Attemp $i,load status: $result"
    fi
    ((i++))
    sleep 5m
  done
  if [ $i -ge 20 ]
  then
          echo "[ERROR] load[load_label:$load_label]未finish,如果没到达最大重试次数，将会继续重试,重试时间间隔可以适当调大。"
          exit 2
  fi
}

check_count(){
  # 该方法中是starrocks中的库和表
  local inner_db=$1
  local inner_tbl=$2
  local inner_preload_tbl=$3
  local inner_dt=$4

  # 数据校验
  if ((num_of_partitions==0));then
    local star_rocks_count="
        use ${inner_db};
        select count(1) from ${inner_tbl};
    "
    local source_tbl_count="
        select * from ${inner_db}.${inner_preload_tbl}"
  else
    local star_rocks_count="
        use ${inner_db};
        select count(1) from ${inner_tbl} where dt='${inner_dt}';
    "
    local source_tbl_count="
        select * from ${inner_db}.${inner_preload_tbl} where dt='${inner_dt}'"
  fi
  export countSql="$source_tbl_count"
  echo "[Info] source_tbl_count is: ${countSql}"
  echo "[Info] star_rocks_count is: ${star_rocks_count}"

  # count starrocks-data
  local dorisRes=`mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e"
                    $star_rocks_count"`
  export dorisCnt=`echo $dorisRes | awk '{print $2}'`
  echo "[Info] dorisCnt: $dorisCnt"

  # count source-data
  export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
  export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

  $SPARK_HOME/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue root.basic_platform.critical \
  --driver-memory 4G \
  --executor-memory 16G \
  --executor-cores 4 << 'EOF'

      val dorisCnt = sys.env("dorisCnt").toLong
      println("[DEBUG] dorisCnt: " + dorisCnt)
      val countSql = sys.env("countSql").toString

      val oriCnt = spark.sql(countSql).count
      var isEqual = false
      isEqual = dorisCnt == oriCnt
      println(s"[INFO] dorisCnt:${dorisCnt};sourceCount:${oriCnt}")
      if(isEqual) System.exit(0) else System.exit(1)

EOF

  isEqual=$?
  echo "[INFO] isEqual:$isEqual"

  if [ $isEqual = 0 ]
  then
      echo "[INFO] 数据校验(count check)正常"
      # load finished, update mysql
      mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
        update starrocks_auto_load set load_status=2 where load_label='$load_label';
      "
  else
      echo "[ERROR] 数据校验(count check)有异常"
      # load finished, update mysql
      mysql -udemeter -P3306 -p'e12c3fYoJv2VyxPT' -h 10.208.30.215 demeter --default-character-set=utf8 -e "
        update starrocks_auto_load set load_status=4 where load_label='$load_label';
      "
      exit 3
  fi
}

get_partition_num "$global_db_name.$preload_tbl_name"
wait_and_listen $global_db_name $global_tbl_name $date_dt
check_count $global_db_name $global_tbl_name $preload_tbl_name $date_dt
