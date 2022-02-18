#!/usr/bin/env bash
source /etc/profile

today=`date -d "0 days" +"%Y%m%d"`
file_format="orc"
db_name="hive_vipvpe"
dt=`date -d "$today -1 day" +"%Y%m%d"`
mysql_exec="mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e"
date_dt=`date -d @$1 +%Y%m%d`
db_name=$2
tbl_name=$3
if [[ $4 == "true" ]];then
  testMode="_test_by_xuefei"
else
  testMode=""
fi

export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
export SPARK_CONF_DIR=/home/vipshop/conf/spark3_0

pre_create_db_tab(){
    mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e
}

truncate_partition(){
  local tbl_name=$1
  # such as p20211009
  local partition="$2"
  local sql="TRUNCATE TABLE $tbl_name PARTITION $partition;"
  echo "[Debug] starRocks中分区已经存在，删掉原有的数据，sql:$sql"
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uhdfs -p'hdfs321' -e "$sql"
}

add_partition(){
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
  # 只去掉了dt
  local db_tbl_name=$1
  local field_to_be_removed=$2
  # hive会有乱码
  local cols=`$SPARK_HOME/bin/spark-sql --master local[1] --driver-memory 4G -e "show columns in $db_tbl_name"`
  col_arr=(`echo $cols`)
  column_str=""
  for i in ${col_arr[*]}
  do
    if [[ "$i" == "$field_to_be_removed" ]];then
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
  local db_tbl_name=$1
  local date_dt=$2
  # 考虑到分流的影响，此处必须是分区的location
  part_location=`hive -e "desc formatted $db_tbl_name partition(dt=$date_dt);" | grep Location | awk '{print $NF}'`
  if [[ -z $part_location ]];then
    echo "[Error] 分区[dt=$date_dt]不存在！"
    exit 1
  fi
  local num_of_partitions=`hive -e "show partitions ${db_tbl_name}" | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'`
  echo "[Info] num_of_partitions: $num_of_partitions"

  if ((num_of_partitions>0));then
    for i in `seq 1 ${num_of_partitions}`
      do
        part_location="${part_location}/*"
    done
  fi
  echo "[Info] current source_path: $part_location"
}

load_one_partition(){
  local date_dt=$1
  # 判断分区是否存在，如果存在，则先truncate分区的数据
  local tbl_name="$2${testMode}"
  local tbl_name_init=$2
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

  get_partition_location "$db_name.$tbl_name_init" $date_dt

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
get_field "$db_name.$tbl_name" "dt"
load_one_partition $date_dt $tbl_name


add_resource(){
  # spark_resource_1已经创建
  local sql='
      CREATE EXTERNAL RESOURCE "spark_resource_1"
      PROPERTIES
      (
          "type" = "spark",
          "spark.master" = "yarn",
          "spark.submit.deployMode" = "client",
          "spark.hadoop.fs.defaultFS" = "hdfs://bipcluster",
          "spark.hadoop.dfs.nameservices" = "bipcluster",
          "spark.hadoop.dfs.ha.namenodes.bipcluster" = "mynamenode1,mynamenode2",
          "spark.hadoop.dfs.namenode.rpc-address.bipcluster.mynamenode1" = "sd-hadoop-namenode-50-21.idc.vip.com:50070",
          "spark.hadoop.dfs.namenode.rpc-address.bipcluster.mynamenode2" = "sd-hadoop-namenode-50-22.idc.vip.com:50070",
          "spark.hadoop.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
          "spark.hadoop.yarn.resourcemanager.ha.enabled" = "true",
          "spark.hadoop.yarn.resourcemanager.ha.rm-ids" = "rm1,rm2",
          "spark.hadoop.yarn.resourcemanager.address.rm1" = "sd-bigdata-apollo-rm-01.idc.vip.com:8040",
          "spark.hadoop.yarn.resourcemanager.address.rm2" = "sd-bigdata-apollo-rm-02.idc.vip.com:8040",
          "working_dir" = "hdfs://bipcluster/tmp/starrocks",
          "broker" = "hdfs_broker",
          "broker.username" = "hdfs",
          "broker.password" = "hdfs",
          "broker.dfs.nameservices" = "bipcluster",
          "broker.dfs.ha.namenodes.bipcluster" = "mynamenode1,mynamenode2",
          "broker.dfs.namenode.rpc-address.bipcluster.mynamenode1" = "sd-hadoop-namenode-50-21.idc.vip.com:50070",
          "broker.dfs.namenode.rpc-address.bipcluster.mynamenode2" = "sd-hadoop-namenode-50-22.idc.vip.com:50070",
          "broker.dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      );
      GRANT USAGE_PRIV ON RESOURCE "spark_resource_1" TO "hdfs"@"%";
      '
  local drop_resource_sql='
    DROP RESOURCE "spark_resource_1";
  '
  mysql -hgd16-bigdata-apollo-nm-150-181-22.idc.vip.com -P19030 -uroot -e "$sql"
}

load_sql_demo(){
  local sql="
    use hive_vipvpe;
    LOAD LABEL hive_vipvpe.st_price_tag_once_detail_preload_test_by_xuefei_3
    (
        DATA INFILE("hdfs://bipcluster03/bip/hive_warehouse/hive_vipvpe.db/st_price_tag_once_detail_preload")
        INTO TABLE st_price_tag_once_detail_test_by_xuefei
        COLUMNS TERMINATED BY ","
        (v_sku_id,barcode,sn,brand_store_sn,tag_price,update_time,is_haitao,expire_time,is_mp,brand_id,goods_id,spu_id,goods_code,history_dept_name,cur_dept_name,standard_prod_attr,brand_store_name,brand_level,brand_type,goods_name,new_category_3rd_name,goods_level,hot_sales_type,vipshop_price,market_price,benchmark_price,price_effective_time,approval_promo_id,approval_promo_name,approval_plan_type,product_sell_from,price_ratio,product_sell_age,kq_top_n,single_promo,discount_promo,coupon_promo,first_dep_of_owner_id,first_dep_of_owner,sec_dep_of_owner_id,sec_dep_of_owner,vendor_code_list,store_id,prod_spu_id,prod_sku_id,mer_item_no,ptp_tag_ids,his_price_goods_id,price_tag)
    )
    WITH RESOURCE 'spark_resource_1'
    (
        "spark.executor.memory" = "8g",
        "spark.shuffle.compress" = "true"
    )
    PROPERTIES
    (
        "timeout" = "3600"
    );
  "

  local show_load="
    show load from hive_vipvpe where label='st_price_tag_once_detail_preload_test_by_xuefei_3'
    order by CreateTime desc limit 1;
  "
}
