--测试customize partition location
drop table if exists vipdmt.query110_standard;
create table if not exists vipdmt.query110_standard like vipdmt.container_monitor_impl ;
insert overwrite vipdmt.query110_standard partition (dt='20220113') as
select
    ts,
    pid,
    containerid,
    physical_mem_used,
    physical_mem_cap,
    virtual_mem_used,
    virtual_mem_cap,
    request_vcore,
    cpu_useage_pct_per_core,
    cpu_usage_pct_total,
    char_read,
    char_write
from vipdmt.container_monitor_impl where dt='20220113' limit 1;
alter table t1 set location 'hdfs://bipcluster08/bip/hive_warehouse/vipdmt/query110_standard';
insert overwrite vipdmt.query110_standard partition (dt='20220113') as
select
    ts,
    pid,
    containerid,
    physical_mem_used,
    physical_mem_cap,
    virtual_mem_used,
    virtual_mem_cap,
    request_vcore,
    cpu_useage_pct_per_core,
    cpu_usage_pct_total,
    char_read,
    char_write
from vipdmt.container_monitor_impl where dt='20220113' limit 100;