package org.apache.spark.sql

import org.apache.spark.sql.EcAndFileCombine.hadoopConfDir
import org.scalatest.funsuite.AnyFunSuite

class OrcFileDumpSuite extends AnyFunSuite {
  test("orc dump") {
    System.getenv(hadoopConfDir)
    val spark = SparkSession.builder().getOrCreate()
    spark.sql("show partitions vipst_temp_dataop.ads_bct_task_page_mer_detail").collect().filter(row => {
      val partitionStr = row.get(0)
      if (partitionStr != null && partitionStr.toString.contains("dt=20211022")) true
      else false
    }).length
  }
}
