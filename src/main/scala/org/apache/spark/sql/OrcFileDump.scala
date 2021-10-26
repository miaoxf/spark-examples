package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.EcAndFileCombine.{loadJars, sparkApplicationName, sparkHomePath}
import org.apache.spark.sql.InnerUtils.dumpOrcFileWithSpark

import java.io.File

object OrcFileDump {

  def main(args: Array[String]): Unit = {
    val jars = new java.util.ArrayList[String]()
    loadJars(new File(sparkHomePath.stripSuffix("/") + "/jars"), jars)
    import scala.collection.JavaConverters._
    val conf = new SparkConf()
    var spark: SparkSession = null
    val builder = SparkSession.builder().config(conf)
    conf.set("spark.master", "yarn")
      .set("spark.submit.deployMode", "client")
      .set("queue", "root.basic_platform.critical")
      .set("spark.driver.memory", "6G")
      .set("spark.executor.cores", "4")
      .set("spark.executor.memory", "8G")
      .set("spark.hadoop.hive.exec.dynamic.partition", "true")
      .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nostrick")
      .set("spark.hadoop.hive.exec.max.dynamic.partitions", "2000")
      .setSparkHome(sparkHomePath)
      // todo delete
      .set("SPARK_CONF_DIR", "/home/vipshop/conf/spark3_0")
      .setAppName(sparkApplicationName)
      .setJars(jars.asScala)
    spark = builder.enableHiveSupport().getOrCreate()

    var path = "hdfs://bipcluster03/bip/hive_warehouse/vipreco.db/ads_log_tess_log_parse_real_1d_df/dt=20211020"
    var parallelism = 500
    if (args.size > 0) path = args(0)
    if (args.size > 1) parallelism = args(1).toInt
    println("dump result: " + dumpOrcFileWithSpark(spark, path, parallelism))
  }
}
