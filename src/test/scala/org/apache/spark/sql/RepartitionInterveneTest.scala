package org.apache.spark.sql

import org.apache.spark.sql.RepartitionIntervene.StructFieldEnhance
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class RepartitionInterveneTest extends AnyFunSuite {

  test("sf") {
    val enhances = Seq(StructFieldEnhance(StructField("field1", StringType, false)))
    println(enhances.filter(sf => {
      sf.f.dataType match {
        case StringType => true
        case _ => false
      }
    }))
  }

  test("ceil") {
    val d = Math.ceil(0.01)
    println(d)

    val sql =
      """
        | select
        |    c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, extended_price,
        |    extended_tax, list_price
        | from (select
        |        ss_ticket_number, ss_customer_sk, ca_city bought_city,
        |        sum(ss_ext_sales_price) extended_price,
        |        sum(ss_ext_list_price) list_price,
        |        sum(ss_ext_tax) extended_tax
        |     from store_sales, date_dim, store, household_demographics, customer_address
        |     where store_sales.ss_sold_date_sk = date_dim.d_date_sk
        |        and store_sales.ss_store_sk = store.s_store_sk
        |        and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        |        and store_sales.ss_addr_sk = customer_address.ca_address_sk
        |        and date_dim.d_dom between 1 and 2
        |        and (household_demographics.hd_dep_count = 4 or
        |             household_demographics.hd_vehicle_count = 3)
        |        and date_dim.d_year in (1999,1999+1,1999+2)
        |        and store.s_city in ('Midway','Fairview')
        |     group by ss_ticket_number, ss_customer_sk, ss_addr_sk,ca_city) dn,
        |    customer,
        |    customer_address current_addr
        | where ss_customer_sk = c_customer_sk
        |   and customer.c_current_addr_sk = current_addr.ca_address_sk
        |   and current_addr.ca_city <> bought_city
        | order by c_last_name, ss_ticket_number
        | limit 100
        |""".stripMargin

    val s =
      """
        |insert overwrite table hive_vipvpe.st_price_tag_daily_detail_preload partition (dt='20220206')
        |select v_sku_id,
        |barcode,
        |sn,
        |brand_store_sn,
        |tag_price,
        |update_time,
        |is_haitao,
        |expire_time,
        |is_mp,
        |brand_id,
        |goods_id,
        |spu_id,
        |goods_code,
        |history_dept_name,
        |cur_dept_name,
        |standard_prod_attr,
        |brand_store_name,
        |brand_level,
        |brand_type,
        |goods_name,
        |new_category_3rd_name,
        |goods_level,
        |hot_sales_type,
        |vipshop_price,
        |market_price,
        |benchmark_price,
        |price_effective_time,
        |approval_promo_id,
        |approval_promo_name,
        |approval_plan_type,
        |product_sell_from,
        |price_ratio,
        |product_sell_age,
        |kq_top_n,
        |single_promo,
        |discount_promo,
        |coupon_promo,
        |first_dep_of_owner_id,
        |first_dep_of_owner,
        |sec_dep_of_owner_id,
        |sec_dep_of_owner,
        |vendor_code_list,
        |store_id,
        |prod_spu_id,
        |prod_sku_id,
        |mer_item_no,
        |his_price_goods_id,
        |ptp_tag_ids,
        |price_tag
        |from hive_vipvpe.st_price_tag_daily_detail
        |where dt='20220206'
        |""".stripMargin

    val ss =
      """
        |create temporary view st_price_tag_daily_detail_preload_view_2 as
        |select v_sku_id,
        |barcode,
        |sn,
        |brand_store_sn,
        |tag_price,
        |update_time,
        |is_haitao,
        |expire_time,
        |is_mp,
        |brand_id,
        |goods_id,
        |spu_id,
        |goods_code,
        |history_dept_name,
        |cur_dept_name,
        |standard_prod_attr,
        |brand_store_name,
        |brand_level,
        |brand_type,
        |goods_name,
        |new_category_3rd_name,
        |goods_level,
        |hot_sales_type,
        |vipshop_price,
        |market_price,
        |benchmark_price,
        |price_effective_time,
        |approval_promo_id,
        |approval_promo_name,
        |approval_plan_type,
        |product_sell_from,
        |price_ratio,
        |product_sell_age,
        |kq_top_n,
        |single_promo,
        |discount_promo,
        |coupon_promo,
        |first_dep_of_owner_id,
        |first_dep_of_owner,
        |sec_dep_of_owner_id,
        |sec_dep_of_owner,
        |vendor_code_list,
        |store_id,
        |prod_spu_id,
        |prod_sku_id,
        |mer_item_no,
        |his_price_goods_id,
        |ptp_tag_ids,
        |price_tag
        |from hive_vipvpe.st_price_tag_daily_detail
        |where dt='20220207' limit 1;
        |""".stripMargin

    val schema: StructType = SparkSession.getActiveSession.get.sql("select * from hive_vipvpe.st_price_tag_daily_detail").schema
    schema.getFieldIndex("ptp_tag_ids")
    schema.toSeq.filter(_.name.equalsIgnoreCase("ptp_tag_ids"))
    val sss =
      """
        |insert overwrite table hive_vipvpe.st_price_tag_daily_detail_preload partition (dt='20220207')
        |select v_sku_id,
        |barcode,
        |sn,
        |brand_store_sn,
        |tag_price,
        |update_time,
        |is_haitao,
        |expire_time,
        |is_mp,
        |brand_id,
        |goods_id,
        |spu_id,
        |goods_code,
        |history_dept_name,
        |cur_dept_name,
        |standard_prod_attr,
        |brand_store_name,
        |brand_level,
        |brand_type,
        |goods_name,
        |new_category_3rd_name,
        |goods_level,
        |hot_sales_type,
        |vipshop_price,
        |market_price,
        |benchmark_price,
        |price_effective_time,
        |approval_promo_id,
        |approval_promo_name,
        |approval_plan_type,
        |product_sell_from,
        |price_ratio,
        |product_sell_age,
        |kq_top_n,
        |single_promo,
        |discount_promo,
        |coupon_promo,
        |first_dep_of_owner_id,
        |first_dep_of_owner,
        |sec_dep_of_owner_id,
        |sec_dep_of_owner,
        |vendor_code_list,
        |store_id,
        |prod_spu_id,
        |prod_sku_id,
        |mer_item_no,
        |his_price_goods_id,
        |ptp_tag_ids,
        |price_tag
        |from st_price_tag_daily_detail_preload_view;
        |""".stripMargin


    val abc =
      """
        |insert overwrite table hive_vipvpe.st_price_tag_daily_detail_preload partition (dt='20220207')
        |select v_sku_id,
        |barcode,
        |sn,
        |brand_store_sn,
        |tag_price,
        |update_time,
        |is_haitao,
        |expire_time,
        |is_mp,
        |brand_id,
        |goods_id,
        |spu_id,
        |goods_code,
        |history_dept_name,
        |cur_dept_name,
        |standard_prod_attr,
        |brand_store_name,
        |brand_level,
        |brand_type,
        |goods_name,
        |new_category_3rd_name,
        |goods_level,
        |hot_sales_type,
        |vipshop_price,
        |market_price,
        |benchmark_price,
        |price_effective_time,
        |approval_promo_id,
        |approval_promo_name,
        |approval_plan_type,
        |product_sell_from,
        |price_ratio,
        |product_sell_age,
        |kq_top_n,
        |single_promo,
        |discount_promo,
        |coupon_promo,
        |first_dep_of_owner_id,
        |first_dep_of_owner,
        |sec_dep_of_owner_id,
        |sec_dep_of_owner,
        |vendor_code_list,
        |store_id,
        |prod_spu_id,
        |prod_sku_id,
        |mer_item_no,
        |his_price_goods_id,
        |ptp_tag_ids,
        |price_tag
        |from st_price_tag_daily_detail_preload_view_2;
        |""".stripMargin

    val abc =
      """
        |insert overwrite table hive_vipvpe.st_price_tag_daily_detail_preload partition (dt='20220207')
        |select *
        |from st_price_tag_daily_detail_preload_view_2;
        |""".stripMargin


    val ii =
      """
        |insert overwrite table hive_vipvpe.st_price_tag_daily_detail_preload partition (dt='20220207')
        |select v_sku_id,
        |barcode,
        |sn,
        |brand_store_sn,
        |tag_price,
        |update_time,
        |is_haitao,
        |expire_time,
        |is_mp,
        |brand_id,
        |goods_id,
        |spu_id,
        |goods_code,
        |history_dept_name,
        |cur_dept_name,
        |standard_prod_attr,
        |brand_store_name,
        |brand_level,
        |brand_type,
        |goods_name,
        |new_category_3rd_name,
        |goods_level,
        |hot_sales_type,
        |vipshop_price,
        |market_price,
        |benchmark_price,
        |price_effective_time,
        |approval_promo_id,
        |approval_promo_name,
        |approval_plan_type,
        |product_sell_from,
        |price_ratio,
        |product_sell_age,
        |kq_top_n,
        |single_promo,
        |discount_promo,
        |coupon_promo,
        |first_dep_of_owner_id,
        |first_dep_of_owner,
        |sec_dep_of_owner_id,
        |sec_dep_of_owner,
        |vendor_code_list,
        |store_id,
        |prod_spu_id,
        |prod_sku_id,
        |mer_item_no,
        |his_price_goods_id,
        |ptp_tag_ids,
        |price_tag
        |from hive_vipvpe.st_price_tag_daily_detail
        |where dt='20220207' limit 1
        |""".stripMargin
  }
}
