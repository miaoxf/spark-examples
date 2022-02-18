  use hive_vipvpe;
 LOAD LABEL hive_vipvpe.load_ads_mer_item_history_multi_hand_price_df_test_by_xuefei_20220125_a
  (
  DATA INFILE('hdfs://bipcluster03/bip/hive_warehouse/hive_vipvpe.db/ads_mer_item_history_multi_hand_price_df/dt=20220125/*')
  INTO TABLE ads_mer_item_history_multi_hand_price_df_test_by_xuefei
  PARTITION (p20220125)
  FORMAT AS 'orc'
  (
  price_effective_time,
  first_dep_of_owner,
  first_dep_of_owner_id,
  sec_dep_of_owner,
  sec_dep_of_owner_id,
  third_cate_id,
  third_cate_name,
  sell_channel,
  prod_spu_id,
  prod_sku_id,
  merchandise_no,
  goods_name,
  goods_no,
  osn,
  mer_item_no,
  barcode,
  brand_store_sn,
  brand_store_name_en,
  brand_store_name_ch,
  is_quota,
  market_price,
  vipshop_price,
  vipshop_start_time,
  vipshop_end_time,
  single_price,
  single_price_promo,
  discount_price,
  discount_price_promo,
  coupon_price,
  coupon_price_promo,
  min_hand_price,
  min_hand_price_promo,
  min_pieces,
  max_pieces,
  yard_no,
  yard_no_start_time,
  vendor_code,
  vendor_code_list,
  store_id,
  is_mp,
  is_haitao,
  data_hm,
  json_version
  )
  SET (dt = '20220125')
  )
  WITH BROKER hdfs_broker ('username'='hdfs', 'password'='hdfs')
  PROPERTIES
  (
  'timeout' = '3600'
  );