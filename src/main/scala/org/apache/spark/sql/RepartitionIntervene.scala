package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.EcAndFileCombine.{loadJars, sparkHomePath}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


object RepartitionIntervene {
  var spark: SparkSession = null

  def main(args: Array[String]): Unit = {
    val sql = if (args.size > 0) args(0) else "select * from vipscene.ads_dwd_scene_brand_impression_mid_1d where dt=20211129"
    val fraction: Double = if (args.size > 1) args(1).toDouble else 0.1

    import java.io.File
    import scala.collection.JavaConverters._
    val jars = new java.util.ArrayList[String]()
    loadJars(new File(sparkHomePath.stripSuffix("/") + "/jars"), jars)
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
      .setAppName("SampleLargeColumn")
      .setJars(jars.asScala)
    spark = builder.enableHiveSupport().getOrCreate()

    estimateRepartitionField(spark.sql(sql).toDF().rdd, fraction)
    InnerLogger.info("SampleLargeColumn", "start to sample rdd...")

  }

  def getEstimateRepartitionFieldStr(datasource: RDD[Row], fraction: Double = 0.01): String = {
    val ret = estimateRepartitionField(datasource, fraction)
    if (ret.isEmpty) "" else ret.mkString(",")
  }


  def excludeFieldsNotString(potentialFields: Seq[StructFieldEnhance]): Seq[StructFieldEnhance] = {
    potentialFields.filter(sf => {
      sf.f.dataType match {
        case StringType => true
        case _ => false
      }
    })
  }

  def estimateRepartitionField(datasource: RDD[Row], fraction: Double = 0.01): Seq[String] = {
    val sampleRdd = datasource.sample(true, fraction).cache()
    import scala.collection.mutable.ArrayBuffer
    // rule1: estimate and get suitable fields as much as possible
    val potentialFields: Seq[StructFieldEnhance] = estimateFieldsByTotalSize(sampleRdd)

    // rule2: exclude fields which type are not string
    val stringFields: Seq[StructFieldEnhance] = excludeFieldsNotString(potentialFields)

    // rule3: exclude fields which are nearly all distinct values, but
    // we can retain one of those in `potentialFields`, which is used to
    // distribute data more evenly.
    val finalFields = excludeOrRetainFields(sampleRdd, stringFields)
    finalFields.map(_.f.name)
  }

  private def excludeOrRetainFields(sampleRdd: RDD[Row], potentialFields: Seq[StructFieldEnhance]): Seq[StructFieldEnhance] = {
    // todo
    if (potentialFields.size >= 3) {
      Seq(potentialFields(0), potentialFields(1), potentialFields(2))
    } else if (potentialFields.size >= 2) {
      Seq(potentialFields(0), potentialFields(1))
    } else {
      // if size is equal to 1, abort the field, to avoid data skew
      // todo if size is equal to 1, but there are some excluded non-string fields,
      // we should consider those non-string fields.
      Seq()
    }
  }

  private def estimateFieldsByDistinctCount(sampleRdd: RDD[Row]): Seq[StructFieldEnhance] = {
    // exclude field which distinct value are in majority
    // todo
    null
  }

  private def estimateFieldsByTotalSize(sampleRdd: RDD[Row]): Seq[StructFieldEnhance] = {
    val zipRdd = sampleRdd.map(row => {
      val buffer = new ArrayBuffer[Long]()
      for (i <- Range(0, row.length)) {
        if (row.get(i) == null) {
          buffer.append(0)
        } else {
          val strSize = row.get(i).toString.size
          buffer.append(strSize)
        }
      }
      (row.schema, buffer)
    })
    val res = zipRdd.reduce((b1, b2) => {
      val sizeArr = b1._2
      val sizeArr2 = b2._2
      val buffer = new ArrayBuffer[Long]()
      for (i <- Range(0, sizeArr.size)) {
        buffer.append(sizeArr(i) + sizeArr2(i))
      }
      (b1._1, buffer)
    })

    val resArr = res._2.toArray
    val schema = res._1
    java.util.Arrays.sort(resArr)
    val resBuffer = new ArrayBuffer[StructFieldEnhance]()
    import org.apache.spark.sql.types.StructField
    for (i <- Range(0, resArr.length)) {
      val size = resArr(resArr.length - 1 - i)
      resBuffer.append(schema(res._2.indexOf(size)).enhance(size))
    }

    // StructType(Seq(maxSizeField, maxSizeField2, maxSizeField3))
    InnerLogger.info("sort with total size", resBuffer.toString())
    resBuffer
  }

  private implicit class StructFieldEnhance(val f: StructField) {
    var size: Long = 0
    override def toString: String = s"StructField(${f.name},${f.dataType},${f.nullable},${size})"
    def enhance(size: Long): StructFieldEnhance = {
      val enhance = StructFieldEnhance(f)
      enhance.size = size
      enhance
    }
  }

}
