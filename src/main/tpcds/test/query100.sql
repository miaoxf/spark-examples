--一级分区(insert overwrite)测试,静态分区方式
drop table if exists vipdmt.query100_test;
create table if not exists vipdmt.query100_test like vipdmt.container_monitor_impl ;
insert overwrite table vipdmt.query100_test partition (dt='20220114')
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
from vipdmt.container_monitor_impl where dt='20220114';