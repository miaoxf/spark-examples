--测试小文件合并
set spark.sql.merge.output.enabled=true
set spark.sql.global.merge.output.enabled=true
drop table if exists vipdmt.query107_test;
create table if not exists vipdmt.query107_test like temp_dmt.dm_demeter_action_desc ;
insert overwrite table vipdmt.query107_test partition(dt='20220114')
select
    action_id
     ,engine
     ,priority_level
     ,progress
     ,offset_unit
     ,name
     ,description
     ,developer
     ,frequency
     ,vipdata_project_id
     ,vipdata_action_id
     ,dept_name
     ,project_name
from temp_dmt.dm_demeter_action_desc
where dt='20220114' ;