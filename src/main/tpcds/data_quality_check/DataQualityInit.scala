//import com.google.common.base.Joiner
//import org.apache.commons.logging.LogFactory
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.storage.StorageLevel
//
//import java.io.{File, PrintWriter}
//import java.text.SimpleDateFormat
//import java.util.Calendar
//import scala.collection.mutable
//import scala.io._
//import scala.collection.JavaConversions._
//import scala.collection.JavaConversions._
//
//
//val status = "test"
//val standard = "standard"
//val targetDir = s"/home/hdfs/hive-testbench/sample-queries-tpcds-spark-quality-${status}"
//val newDir = new File(targetDir)
//val oldDirPath = "/home/hdfs/hive-testbench/sample-queries-tpcds-spark"
//val oldDir = new File(oldDirPath)
//
//def getTblName(fileName: String) = {
//  if (fileName.endsWith("sql")) {
//    val queryName = fileName.split(".sql")(0)
//    (s"vipdmt.${queryName}_${status}", s"vipdmt.${queryName}_${standard}")
//  } else {
//    throw new RuntimeException(s"$fileName can't get table name")
//  }
//}
//
//def getCntAndRdd(tbl: String) = {
//  val sql = s"select * from $tbl"
//  val rdd = spark.sql(sql).collect()
//  val cnt = rdd.size
//  println(s"$tbl cnt:$cnt")
//  (cnt, rdd)
//}
//
//def sortRdd(rdd: Array[Row]) = {
//  val res = rdd.map(r => {
//    r.mkString(",")
//  })
//  res.sortWith(_>_)
//}
//
//
//var i=0
//for (file <- newDir.listFiles()) {
//  if (file.getName.endsWith("sql")) {
//    i=i+1
//    val (testTbl, standardTbl) = getTblName(file.getName)
//    println(s"[$file]:$testTbl--$standardTbl")
//    val (testCnt, testRdd) = getCntAndRdd(testTbl)
//    val (cnt, rdd) = getCntAndRdd(standardTbl)
//
//    if (testCnt != cnt) {
//      throw new RuntimeException(s"[$file query count is not equal to the standard]:" +
//        s"$testTbl cnt:$testCnt--$standardTbl cnt:$cnt")
//    }
//    val compareRes = sortRdd(testRdd).zip(sortRdd(rdd)).map(r => {
//      val (testStr, str) = r
//      if (!testStr.equals(str)) {
//        r
//      } else {
//        null
//      }
//    }).filter(_ != null)
//
//    if (compareRes.isEmpty) {
//      println(s"[$file result is ok]:$testTbl is the same to $standardTbl")
//    } else {
//      compareRes.foreach(r => {
//        val (testStr, str) = r
//        println(s"$testTbl result:$testStr\n $standardTbl result:$str")
//      })
//      throw new RuntimeException(s"[$file query result is not equal to the standard]")
//    }
//    println(s"Finished $i query!")
//  }
//}
//println(s"Check successfully")
