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

  def main(args: Array[String]): Unit = {
    val targetTable = args(0).toString
    val newCluster = args(1)
    executeAlter = args(2).toBoolean
    val sql = if (args.size>3) args(3) else s"select id,location,first_partition,db_name,tbl_name from ${targetTable} where ec_status=2" +
      s" and num_partitions>1 and last_modify_time>from_unixtime(1635303600)"

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
      record.destTblLocation = record.location.replace(prefix, newCluster)
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
        s"set location '${record.destTblLocation.stripSuffix("/")}"
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
