import com.google.common.base.Joiner
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, crc32}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable
import scala.io._
import scala.collection.JavaConversions._
import scala.collection.JavaConversions._
import scala.io.{BufferedSource, Source}
import scala.reflect.ClassTag


val status = "test"
val standard = "standard"
val targetDir = s"/home/hdfs/hive-testbench/sample-queries-tpcds-spark-quality-${status}"
val newDir = new File(targetDir)
val oldDirPath = "/home/hdfs/hive-testbench/sample-queries-tpcds-spark"
val oldDir = new File(oldDirPath)
val datasourcePath = "/home/hdfs/hive-testbench/sample-queries-tpcds-spark-datasource"

val spark = SparkSession.getActiveSession.get

def getSqlInFile(file: File): Option[Tuple2[String, String]] = {
  try {
    if (file.getName.contains("datasource")) {
      val source: BufferedSource = Source.fromFile(file)
      val lines = source.getLines()
      val sqlstr = lines.mkString(" ")
      val sqls = sqlstr.split(";")
      assert(sqls.size == 2)
      assert(!sqls(0).isEmpty)
      assert(!sqls(1).isEmpty)
      return Some(Tuple2(sqls(0), sqls(1)))
    }
  } catch {
    case exception: Exception =>
  }
  None
}

def getTblName(file: File) = {
  val fileName = file.getName
  val sql = getSqlInFile(file)
  if (fileName.endsWith("sql")) {
    val queryName = fileName.split(".sql")(0)
    (s"vipdmt.${queryName}_${status}", s"vipdmt.${queryName}_${standard}", sql)
  } else {
    throw new RuntimeException(s"$fileName can't get table name")
  }
}

def getCntAndRdd(tbl: String, sqlInFile: Option[String], isTest: Boolean) = {
  val sql = if (sqlInFile.isDefined) sqlInFile.get else s"select * from $tbl"
  spark.sql(sql)
}

def hashSortRdd(rdd: RDD[Row]) = {
  import scala.math.Ordering.String
  rdd.map(r => {
    r.mkString(",")
  }).repartition(1600)
    .keyBy(t=>t).sortByKey(true)
}

def sortRdd(rdd: Array[Row]) = {
  val res = rdd.map(r => {
    r.mkString(",")
  })
  res.sortWith(_>_)
}

def checkDataQualityWithCrc(file: File,
                            testTbl: String, standardTbl: String,
                            testDf: DataFrame, standardDf: DataFrame) = {
  val testCnt = testDf.count()
  val cnt = standardDf.count()
  println(s"check the value count of testTbl[$testTbl] and standardTbl[$standardTbl]:")
  println(s"$testTbl cnt:$testCnt")
  println(s"$standardTbl cnt:$cnt")

  if (testCnt != cnt) {
    throw new RuntimeException(s"[$file query count is not equal to the standard]:" +
      s"$testTbl cnt:$testCnt--$standardTbl cnt:$cnt")
  }
  testDf.selectExpr()

  def getResult(dataFrame: DataFrame): Option[Long] = {
    val row =
      dataFrame
        .selectExpr(s"sum(crc32(concat_ws(',', *)))")
        .head()
    if (row.isNullAt(0)) None else Some(row.getLong(0))
  }

  val testCrc = getResult(testDf)
  val standardCrc = getResult(standardDf)
  if (testCrc.isDefined && standardCrc.isDefined) {
    if (testCrc.get == standardCrc.get) {
      println(s"check data quality of testTbl[$testTbl] and standardTbl[$standardTbl]" +
        s" successfully! ")
    } else {
      println(s"check data quality of testTbl[$testTbl] and standardTbl[$standardTbl]" +
        s" failed! crc of  testTbl[$testTbl] is [${testCrc.get}] and" +
        s" standardTbl[$standardTbl] is [${standardCrc.get}]")
    }
  } else {
    println(s"check data quality of testTbl[$testTbl] and standardTbl[$standardTbl]" +
      s" failed with None result of crc.")
  }
}

def checkDataQuality(file: File, testTbl: String,
                     standardTbl: String,
                     testRdd: RDD[Row], rdd: RDD[Row]) = {
  val testCnt = testRdd.count()
  val cnt = rdd.count()
  println(s"check the value count of testTbl[$testTbl] and standardTbl[$standardTbl]:")
  println(s"$testTbl cnt:$testCnt")
  println(s"$standardTbl cnt:$cnt")

  if (testCnt != cnt) {
    throw new RuntimeException(s"[$file query count is not equal to the standard]:" +
      s"$testTbl cnt:$testCnt--$standardTbl cnt:$cnt")
  }
  var compareRes: RDD[((String, String), (String, String))] = null
  try {

    compareRes = hashSortRdd(testRdd).zip(hashSortRdd(rdd)).map(r => {
      val (testStr, str) = r
      if (!testStr.equals(str)) {
        r
      } else {
        null
      }
    }).filter(_ != null)

    println(s"start check the data quality of testTbl[$testTbl] and standardTbl[$standardTbl]:")
    if (compareRes.isEmpty) {
      println(s"[$file result is ok]:$testTbl is the same to $standardTbl")
    } else {
      compareRes.foreach(r => {
        val (testStr, str) = r
        println(s"$testTbl result:$testStr\n $standardTbl result:$str")
      })
      throw new RuntimeException(s"[$file query result is not equal to the standard]")
    }
  } catch {
    case e: Exception => println(s"[ERROR] check failed![$standardTbl | $testTbl]")
  }

}

var i=0

for (file <- newDir.listFiles()) {
  if (file.getName.endsWith("sql")) {
    i=i+1
    val (testTbl, standardTbl, sql) = getTblName(file)
    println(s"[$file]:$testTbl--$standardTbl")

    if (sql.isDefined) {
//      //针对不需要insert类型的datasource，需要单独指定datasource sql
//      val testRdd = getCntAndRdd(testTbl, None, true)
//      val rdd = getCntAndRdd(standardTbl, None, false)
//      checkDataQuality(file, testTbl, standardTbl, testRdd, rdd)
//      println(s"Finished $i query!")
    } else {
      // 原有的check逻辑
      try {
        val testDf = getCntAndRdd(testTbl, None, true)
        val standardDf = getCntAndRdd(standardTbl, None, false)
        checkDataQualityWithCrc(file, testTbl, standardTbl, testDf, standardDf)
        println(s"Finished $i query!")
      } catch {
        case exception: Exception => println(s"[ERROR] failed one check[$standardTbl,$testTbl]")
      }
    }

  }
}
println(s"Check successfully")