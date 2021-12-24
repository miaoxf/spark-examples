#!/bin/bash
# usage: ./SampleLargeColumn.sh 'select * from vipscene.ads_dwd_scene_brand_impression_mid_1d where dt=20211129' 0.01
SPARK_HOME_IN_SCRIPT="/home/vipshop/platform/spark-3.0.1"
source ~/xuefei/env.sh
export param_sql=$1
export param_fraction=$2

if [[ -z $3 ]];then
  export src_path=""
else
  export src_path=$3
fi

${SPARK_HOME_IN_SCRIPT}/bin/spark-shell \
--master yarn \
--deploy-mode client \
--queue "root.basic_platform.critical" \
--driver-memory 4G \
--executor-memory 8G \
--executor-cores 4 \
--conf 'spark.hadoop.hive.exec.dynamic.partition=true' \
--conf 'spark.hadoop.hive.exec.dynamic.partition.mode=nostrick' \
--conf 'spark.hadoop.hive.exec.max.dynamic.partitions=2000' \  << EOF

    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.Row

    val sql = "${param_sql}"
    val path = "${src_path}"
    println("path:" + path)
    spark.read.orc(path).createOrReplaceTempView("src")

    val fraction: Double = "${param_fraction}".toDouble
    val sampleRdd = spark.sql(sql).toDF().rdd.sample(true, fraction).cache()
    println("SampleLargeColumn", "start to sample rdd...")

    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.sql.types.{StringType, StructField}
    implicit class StructFieldEnhance(val f: StructField) {
      var size: Long = 0
      override def toString: String = s"StructField(\${f.name},\${f.dataType},\${f.nullable},\${size})"
      def enhance(size: Long): StructFieldEnhance = {
        val enhance = StructFieldEnhance(f)
        enhance.size = size
        enhance
      }
    }

    import org.apache.spark.sql.types.{StringType, StructField, StructType}
    private def ifDistinctMostly(sampleRdd: RDD[Row],
                               schema: StructType,
                               potentialFields: Seq[StructFieldEnhance]): Seq[StructFieldEnhance] = {
      if (potentialFields.size < 1) return potentialFields
      val count = sampleRdd.count()

      try {
        potentialFields.foreach(pf => {
          val fieldName = pf.f.name
          val index = schema.getFieldIndex(fieldName)
          val countOfPf1 = sampleRdd.map(_.get(index.get)).distinct().count()
          if (countOfPf1 / count > 0.6) {
            println("StrictCompressionPolicy", s"distinct count of field:[${fieldName}] " +
              s"is in major, so choose coalesce policy!")
            return Seq()
          }
        })
      } catch {
        case e: Exception =>
      }

      potentialFields
    }

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

    // calculate the summary of size, whatever the distinct count
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
    for (i <- Range(0, resArr.length)) {
      val size = resArr(resArr.length - 1 - i)
      resBuffer.append(schema(res._2.indexOf(size)).enhance(size))
    }



    println("ifDistinctMostly:" + ifDistinctMostly(sampleRdd, schema, resBuffer).toString())

    println("sort with total size: " + final.toString())

    println("sort with total size: " + resBuffer.toString())

EOF