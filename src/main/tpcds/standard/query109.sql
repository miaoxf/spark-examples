--测试create as select
drop table if exists vipdmt.query109_standard;
create table if not exists vipdmt.query109_standard like vipdmt.container_monitor_impl ;
create table vipdmt.query109_standard as
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