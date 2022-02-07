--测试空数据insert
drop table if exists vipdmt.query108_test;
create table if not exists vipdmt.query108_test like vipdmt.container_monitor_impl ;
insert overwrite table vipdmt.query108_test partition (dt='20220113')
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
from vipdmt.container_monitor_impl where dt='20220113' ;
insert overwrite table vipdmt.query108_test partition (dt='20220114')
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
from vipdmt.container_monitor_impl where dt='20220114' limit 0;