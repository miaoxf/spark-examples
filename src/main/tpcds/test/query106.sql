--测试 dynamicPartitionOverwrite=true
set spark.sql.sources.partitionOverwriteMode='dynamic';
drop table if exists vipdmt.query106_test;
create table if not exists vipdmt.query106_test like vipup.ads_udb_input_pb_common_source_hf ;
insert overwrite table vipdmt.query106_test partition (dt='20220114',hf=1000,label)
select
    table_name
     ,key
     ,pb_value
     ,extra
     ,oa_name
     ,label
from vipup.ads_udb_input_pb_common_source_hf
where dt='20220114' and hf=1000;
insert overwrite table vipdmt.query106_test partition (dt='20220114',hf=1200,label)
select
    table_name
     ,key
     ,pb_value
     ,extra
     ,oa_name
     ,label
from vipup.ads_udb_input_pb_common_source_hf
where dt='20220114' and hf=1200;
set spark.sql.sources.partitionOverwriteMode='static';
