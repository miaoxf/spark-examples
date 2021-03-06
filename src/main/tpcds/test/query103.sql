--多级分区(insert into)测试,动态分区方式
drop table if exists vipdmt.query103_test;
create table if not exists vipdmt.query103_test like vipods_vip.dw_bijia_t_bijia_upload_brand_goods_ds ;
insert into table vipdmt.query103_test partition (dt='20220114', hour, hm)
select
    id,
    task_id,
    product_brand,
    product_name,
    product_code,
    product_sn,
    product_price,
    user_name,
    user_number,
    create_time,
    update_time,
    type,
    product_goods_code,
    product_market_price,
    product_key_word,
    product_size,
    product_color,
    product_brand_type,
    size_num,
    first_product_type,
    product_discount_price,
    product_discount_info,
    product_discount_formula,
    pdc_sn_color,
    pdc_sn_size,
    pt_brand_id,
    flag_is_mp,
    is_unit,
    pure_product_name,
    match_mode,
    hour,
    hm
from vipods_vip.dw_bijia_t_bijia_upload_brand_goods_ds where dt='20220114';