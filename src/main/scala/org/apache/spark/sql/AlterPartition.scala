package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.EcAndFileCombine.{loadJars, sparkApplicationName, sparkHomePath}
import org.apache.spark.sql.JobType.Record

import java.io.File
import scala.collection.immutable.Range
import scala.collection.mutable.ArrayBuffer

object AlterPartition {

  var spark: SparkSession = null
  val records = new java.util.HashMap[String, Record]()
  var executeAlter: Boolean = false
  var alterToNewCluster: Boolean = false

  def main(args: Array[String]): Unit = {
    val targetTable = args(0).toString
    alterToNewCluster = args(1).toBoolean
    executeAlter = args(2).toBoolean

    val begin = if (args.size>3) args(3) else 1635303600
    val end = if (args.size>4) args(4) else 1635332400

    val sql =
      s"""
         |select id,location,first_partition,db_name,tbl_name,cluster_old
         | from ${targetTable}
         | where ec_status=2
         | and num_partitions>1 and last_modify_time>=from_unixtime(${begin})
         |  and last_modify_time<=from_unixtime(${end})
         |""".stripMargin

    val jars = new java.util.ArrayList[String]()
    loadJars(new File(sparkHomePath.stripSuffix("/") + "/jars"), jars)
    import scala.collection.JavaConverters._
    val conf = new SparkConf()

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
      .setAppName("alterPartition")
      .setJars(jars.asScala)
    spark = builder.enableHiveSupport().getOrCreate()
    MysqlSingleConn.init()
    InnerLogger.debug("alterPartition", "start to init Mysql")
    val resultSet = MysqlSingleConn.executeQuery(sql.toString)
    while (resultSet.next()) {
      val record = new Record(resultSet.getInt("id"))
      record.location = resultSet.getString("location")
      record.firstPartition = resultSet.getString("first_partition")
      record.dbName = resultSet.getString("db_name")
      record.tblName = resultSet.getString("tbl_name")
      val initCluster = ("//[^/]*/".r findFirstIn record.location).get.replaceAll("/", "")
      val prefix = "hdfs://" + initCluster
      record.destTblLocation = record.location.stripSuffix(record.firstPartition)
      record.sourceTblLocation = record.location.stripSuffix(record.firstPartition)
        .replace(prefix, "hdfs://" + resultSet.getString("cluster_old"))
      records.put(record.getId.toString, record)
    }
    records.forEach((k, v) => {
      val srcTbl = v.dbName + "." + v.tblName
      show(srcTbl, v)
    })


  }

  def show(srcTbl: String, record: Record): Unit = {
    val showPartitionOfSrcTblSql = "show partitions " + srcTbl
    val showPartitionsRows = spark.sql(showPartitionOfSrcTblSql).collect()
    val fineGrainedPartitionSqls = new ArrayBuffer[String]()
    assert(showPartitionsRows != null)
    val allStaticPartitionsRows: Array[Row] = showPartitionsRows.filter(row => {
      val partitionStr = row.get(0)
      if (partitionStr != null && partitionStr.toString.contains(record.firstPartition)) true
      else false
    })
    val staticLocations: Array[String] = allStaticPartitionsRows.map(_.get(0).toString)
    var locationToStaticPartitionSql: Array[Tuple5[String, String, String, ArrayBuffer[String], String]] = null

    locationToStaticPartitionSql = staticLocations.map(location => {
      var fineGrainedPartitionSql = "alter table " + srcTbl + " partition(${par}) " +
        s"set location '${if (alterToNewCluster) record.destTblLocation.stripSuffix("/") else record.sourceTblLocation.stripSuffix("/")}"
      val partitions = location.split("/")
      val buffer = new ArrayBuffer[String]()
      val partitionColumns = new ArrayBuffer[String]()
      for (i <- Range(0, partitions.size)) {
        val part = partitions(i)
        val kv = part.split("=")
        assert(kv.size == 2)
        buffer += kv(0) + "=" + "'" + kv(1) + "'"
        partitionColumns += kv(0)
        fineGrainedPartitionSql = fineGrainedPartitionSql + s"/${kv(0)}=${kv(1)}"
      }
      fineGrainedPartitionSql = fineGrainedPartitionSql.replace("${par}", buffer.mkString(","))
      fineGrainedPartitionSql = fineGrainedPartitionSql + "'"

      Tuple5(location, buffer.mkString(" and "), buffer.mkString(","),
        partitionColumns, fineGrainedPartitionSql)
    })
    locationToStaticPartitionSql.foreach(location2Sql => {
      fineGrainedPartitionSqls += location2Sql._5
    })
    fineGrainedPartitionSqls.foreach(sql => {
      if (executeAlter) {
        spark.sql(sql)
      }
      println("to execute sql:" + sql)
    })

  }
}
