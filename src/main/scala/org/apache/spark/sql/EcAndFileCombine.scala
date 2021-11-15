package org.apache.spark.sql

// :require /home/vipshop/platform/spark-3.0.1/jars/jackson-mapper-asl-1.9.13.jar
// :require /home/vipshop/platform/spark-3.0.1/jars/jackson-core-asl-1.9.13.jar
// :require /home/hdfs/xuefei/scala/mysql-connector-java-8.0.11.jar

// :require /home/vipshop/platform/spark-3.0.1/jars/spark-core_2.12-3.0.1-SNAPSHOT.jar
// :require /home/vipshop/platform/spark-3.0.1/jars/spark-sql_2.12-3.0.1-SNAPSHOT.jar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.orc.{OrcFile, Reader}
import org.apache.orc.impl.OrcAcidUtils
import org.apache.orc.tools.FileDump
import org.apache.spark.SparkException
import org.apache.spark.sql.EcAndFileCombine.{batchSize, defaultHadoopConfDir, hadoopConfDir, jobType, onlineTestMode, runCmd, targetMysqlTable}
import org.apache.spark.sql.InnerUtils.dumpOrcFileWithSpark
import org.apache.spark.sql.JobType.{JobType, MID_DT_LOCATION, Record}
import org.apache.spark.sql.MysqlSingleConn.{CMD_EXECUTE_FAILED, DATA_IN_DEST_DIR, INIT_CODE, ORC_DUMP_FAILED, PROCESS_KILLED, SKIP_WORK, SOURCE_IN_SOURCE_DIR, SOURCE_IN_TEMPORARY_DIR, START_SPLIT_FLOW, SUCCESS_CODE, SUCCESS_FILE_MISSING, defaultMySQLConfig}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.util.SerializableConfiguration
import org.codehaus.jackson.map.ObjectMapper

import java.io.{BufferedInputStream, File, FileInputStream}
import java.lang.reflect.Method
import java.net.{URL, URLClassLoader}
import java.sql.{Connection, DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.stream.Collectors
import java.util.{Date, Locale, UUID, stream}
import scala.collection.immutable.Range
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.parsing.json.JSON.parseRaw
import scala.util.parsing.json.{JSONArray, JSONObject}

object JobType extends Enumeration {
  val MYSQL_ID = "id"
  val JOB_ID = "jobId"
  val DB_NAME = "dbName"
  val TBL_NAME = "tblName"
  val LOCATION = "location"
  val FIRST_PARTITION = "firstPartition"
  val STATUS = "status"
  val CLUSTER = "cluster"
  val DT = "dt"
  val MID_TBL_NAME = "midTblName"
  val MID_TBL_LOCATION = "midTblLocation"
  val MID_DT_LOCATION = "midDTLocation"
  val SOURCE_TBL_LOCATION = "sourceTblLocation"
  val DEST_TBL_LOCATION = "destTblLocation"
  val TO_BE_DEL_LOCATION = "toBeDelLocation"
  val BOUND_TO_BE_DEL_LOCATION = "boundToBeDelLocation"
  val TOTAL_FILE_SIZE = "totalFileSize"
  val NUM_OF_PARTITION_LEVEL = "numOfPartitionLevel"
  val LARGEST_FILE_SIZE = "largestFileSize"
  val MAX_PARTITION_BYTES = "maxPartitionBytes"
  val COMPUTED_PARALLELISM = "computedParallelism"
  val INIT_FILE_NUMS = "initFileNums"
  val COMBINED_FILE_NUMS = "combinedFileNums"
  val COUNT_OF_INIT_LOC = "countOfInitLocation"
  val COUNT_OF_MID_LOC = "countOfMidLocation"
  val SUCCESS_FILE_LOCATION = "successFileLocation"
  val SUCCESS_FILE_NAME = ".COMBINE_SUCCESS"
  val INPUT_FORMAT = "InputFormat"
  val OUTPUT_FORMAT = "OutputFormat"
  val ORC_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
  val ORC_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
  val APPLICATION_ID = "applicationId"
  val ENABLE_SPLIT_FLOW = "enableSplitFlow"
  val PARTITION_SQL = "partitionSql"

  abstract class JobStep extends Value {
  }
  val encapsulateWork = new JobStep {
    override def id: Int = 0
  }
  val scheduleWork = new JobStep {
    override def id: Int = 1
  }
  val checkWork = new JobStep {
    override def id: Int = 2
  }

  abstract class InnerValue extends Value {
    val mysqlStatus: String = ""
    val numPartitions: String = "num_partitions"
  }

  type JobType = InnerValue
  val ec = new InnerValue {
    override val mysqlStatus = "ec_status"

    override def id: Int = 0
  }
  val fileCombine = new InnerValue {
    override val mysqlStatus = "combine_status"

    override def id: Int = 1
  }

  class Record() {
    var id: Int = -1
    val jobId: String = UUID.randomUUID().toString
    var dbName: String = ""
    var tblName: String = ""
    var location: String = ""
    var firstPartition: String = ""
    var status: Int = -1
    var cluster: String = ""
    var dt: String = ""
    var midTblName: String = ""
    var midTblLocation: String = ""
    var midDTLocation: String = ""
    var sourceTblLocation: String = ""
    var destTblLocation: String = ""
    var toBeDelLocation: String = ""
    var boundToBeDelLocation: String = ""
    var totalFileSize: Long = -1
    var numOfPartitionLevel: String = ""
    var largestFileSize: String = ""
    var maxPartitionBytes: Long = -1
    var computedParallelism: Long = -1
    var initFileNums: Long = -1
    var successFileLocation: String = ""
    var enableSplitFlow = false

    def getId = id

    def this(rid: Int) = {
      this()
      id = rid
    }

    def toMap: java.util.HashMap[String, String] = {
      val res = new java.util.HashMap[String, String]()
      res.put(JOB_ID, jobId);
      res.put(MYSQL_ID, id.toString);
      res.put(DB_NAME, dbName)
      res.put(TBL_NAME, tblName);
      res.put(LOCATION, location);
      res.put(FIRST_PARTITION, firstPartition)
      res.put(STATUS, status.toString);
      res.put(CLUSTER, cluster);
      res.put(DT, dt)
      res.put(MID_TBL_NAME, midTblName);
      res.put(MID_TBL_LOCATION, midTblLocation)
      res.put(MID_DT_LOCATION, midDTLocation);
      res.put(TO_BE_DEL_LOCATION, toBeDelLocation)
      res.put(BOUND_TO_BE_DEL_LOCATION, boundToBeDelLocation)
      res.put(SOURCE_TBL_LOCATION, sourceTblLocation);
      res.put(TOTAL_FILE_SIZE, totalFileSize.toString)
      res.put(NUM_OF_PARTITION_LEVEL, numOfPartitionLevel);
      res.put(LARGEST_FILE_SIZE, largestFileSize)
      res.put(MAX_PARTITION_BYTES, maxPartitionBytes.toString);
      res.put(COMPUTED_PARALLELISM, computedParallelism.toString)
      res.put(INIT_FILE_NUMS, initFileNums.toString)
      res.put(SUCCESS_FILE_LOCATION, successFileLocation)
      res.put(DEST_TBL_LOCATION, destTblLocation)
      res.put(ENABLE_SPLIT_FLOW, enableSplitFlow.toString)
      res
    }

    override def toString: String = {
      JOB_ID + "->" + jobId + "\t" + MYSQL_ID + "->" + id.toString + "\t" + DB_NAME + "->" + dbName + "\t" +
        TBL_NAME + "->" + tblName + "\t" + LOCATION + "->" + location + "\t" + FIRST_PARTITION + "->" + firstPartition + "\t" +
        STATUS + "->" + status.toString + "\t" + CLUSTER + "->" + cluster + "\t" + DT + "->" + dt + "\t" +
        MID_TBL_NAME + "->" + midTblName + "\t" + MID_TBL_LOCATION + "->" + midTblLocation + "\t" +
        MID_DT_LOCATION + "->" + midDTLocation + "\t" + TO_BE_DEL_LOCATION + "->" + toBeDelLocation + "\t" +
        BOUND_TO_BE_DEL_LOCATION + "->" + boundToBeDelLocation + "\t" +
        SOURCE_TBL_LOCATION + "->" + sourceTblLocation + "\t" + ENABLE_SPLIT_FLOW + "->" + enableSplitFlow + "\t" +
        DEST_TBL_LOCATION + "->" + destTblLocation + "\t" + TOTAL_FILE_SIZE + "->" + totalFileSize.toString + "\t" +
        NUM_OF_PARTITION_LEVEL + "->" + numOfPartitionLevel + "\t" + LARGEST_FILE_SIZE + "->" + largestFileSize + "\t" +
        MAX_PARTITION_BYTES + "->" + maxPartitionBytes.toString + "\t" +
        COMPUTED_PARALLELISM + "->" + computedParallelism.toString + "\t" + INIT_FILE_NUMS + "->" + initFileNums.toString + "\t" +
        SUCCESS_FILE_LOCATION + "->" + successFileLocation
    }

    def toSimpleString: String = {
      JOB_ID + "->" + jobId + "\t" + MYSQL_ID + "->" + id.toString + "\t" + DB_NAME + "->" + dbName + "\t" +
        TBL_NAME + "->" + tblName + "\t" + LOCATION + "->" + location + "\t" + FIRST_PARTITION + "->" + firstPartition + "\t" +
        STATUS + "->" + status.toString + "\t" + CLUSTER + "->" + cluster + "\t" + DT + "->" + dt
    }
  }

  val RecordType = classOf[Record]
}

object InnerLogger {
  private val DEBUG_LEVEL: String = "DEBUG"
  private val INFO_LEVEL: String = "INFO "
  private val WARN_LEVEL: String = "WARN "
  private val ERROR_LEVEL: String = "ERROR"

  /** 四个模块的名称，用于日志中区分 */
  val ENCAP_MOD: String = "encapsulation_job"
  val SCHE_MOD: String = "schedule_job"
  val SPARK_MOD: String = "spark_run"
  val CHECK_MOD: String = "check_task"
  val METRIC_COLLECT: String = "metric_collect"

  private def toConsole(level: String, moduleName: String, msg: String): Unit ={
    // todo 日期格式
    println(s"${level} [${System.currentTimeMillis()} | ${moduleName} | " +
      s"threadId:${Thread.currentThread().getId}] " + msg)
  }

  private def toHive(): Unit = {

  }

  def debug(moduleName: String, msg: String) = toConsole(DEBUG_LEVEL, moduleName, msg)
  def info(moduleName: String, msg: String) = toConsole(INFO_LEVEL, moduleName, msg)
  def warn(moduleName: String, msg: String) = toConsole(WARN_LEVEL, moduleName, msg)
  def error(moduleName: String, msg: String) = toConsole(ERROR_LEVEL, moduleName, msg)
}

object MysqlSingleConn {
  var defaultMySQLConfig: List[String] = List("10.208.30.215", "3306", "demeter",
    "demeter", "e12c3fYoJv2VyxPT")
  val INIT_CODE = 0
  val SUCCESS_CODE = 2
  val CMD_EXECUTE_FAILED = 3
  val SKIP_WORK = 4
  val ORC_DUMP_FAILED = 5
  val PROCESS_KILLED = 6
  val SUCCESS_FILE_MISSING = 7
  val SOURCE_IN_SOURCE_DIR = 10
  val SOURCE_IN_TEMPORARY_DIR = 11
  val DATA_IN_DEST_DIR = 12
  val START_SPLIT_FLOW = 3
  val lock = new ReentrantLock()
  var conn: Connection = null
  Class.forName("com.mysql.jdbc.Driver")

  def initSingleConn(config: Seq[String]): Connection = {
    val conn: Connection = DriverManager.getConnection(s"jdbc:mysql://${config(1)}:${config(2)}/${config(3)}" +
      s"?serverTimeZone=GMT%2B8&user=${config(4)}&password=${config(5)}")
    conn
  }

  def initSingleConn(): Connection = {
    initSingleConn(targetMysqlTable :: defaultMySQLConfig ::: "false" :: Nil)
  }

  def init(): Unit = {
    conn = initSingleConn()
  }

  def getConnection(): Connection = {
    try {
      lock.lock()
      val curConnection: Connection = conn
      if (curConnection.isValid(5)) {
        curConnection
      } else {
        // 获取一个新的连接
        conn = initSingleConn()
        conn
      }
    } finally {
      lock.unlock()
    }
  }

  def executeQuery(sql: String): ResultSet = {
    val statement = getConnection.createStatement()
    statement.executeQuery(sql)
  }

  def updateQuery(sql: String): Int = {
    if (onlineTestMode) {
      return 1
    }
    val statement = getConnection.createStatement()
    statement.executeUpdate(sql)
  }

  //todo runcmd中需要确认这边是0？
  def updateStatus(status: String, value: Any, recordId: Int): Int = {
    if (onlineTestMode) {
      return 1
    }
    val statement = getConnection.createStatement()
    val sql =
      s"""
         |update ${targetMysqlTable} set ${status} = ${value.toString}
         |  where id = ${recordId}
         |""".stripMargin

    InnerLogger.debug("update-mysql", s"execute sql ${sql}")
    statement.executeUpdate(sql)
  }

  def batchUpdateStatus(status: String, value: Int, recordId: Array[Int]): Int = {
    if (onlineTestMode) {
      return 1
    }
    val statement = getConnection.createStatement()
    val ids = recordId.mkString(",")
    val sql =
      s"""
         |update ${targetMysqlTable} set ${status} = ${value}
         |  where id in (${ids})
         |""".stripMargin

    InnerLogger.debug("update-mysql", s"execute sql ${sql}")
    statement.executeUpdate(sql)
  }

  def close(): Unit = {
    if (conn != null) {
      conn.close()
    }
  }
}
object EcAndFileCombine {
  val ecPolicy: String = "RS-6-3-1024k"
  val sparkHomePath = "/home/vipshop/platform/spark-3.0.1"
  val hadoopConfDir = "HADOOP_CONF_DIR"
  val defaultHadoopConfDir = "/home/vipshop/conf"
  val sparkDynamicAllocationMaxExecutors = 400
  val sparkMemoryFraction = 0.6
  val defaultAcquireCores = 4
  val defaultAcquireMem = 8
  val defaultMaxCoresPerExecutor = 15
  val defaultMaxMemPerExecutor = 55
  val defaultDriverMemory = "8G"
  val sparkApplicationName = "ecAndFileCombine"
  val defaultCharset = "UTF-8"
  val tmpParentPath = "/tmp/ec_combine"
  val sparkShellFile = "/tmp/sparkShellForEcAndCombine.sh"
  val JAR_SUFFIX: String = ".jar"
  val ZIP_SUFFIX: String = ".zip"
  val SPLIT_DELIMITER: String = ";"
  var onlineTestMode: Boolean = false
  var actionId: String = _
  var actionSid: String = _
  var targetMysqlTable: String = _
  var yarnQueue: String = _
  var jobType: JobType = _
  var batchSize: Int = 10
  var sparkConcurrency: Int = 1
  var filesTotalThreshold: Long = 20000
  var enableFileSizeOrder: Boolean = true
  var enableFileCountOrder: Boolean = true
  var onlyHandleOneLevelPartition: Boolean = true
  var enableFineGrainedInsertion: Boolean = false
  var onlyCoalesce: Boolean = false
  var enableHandleBucketTable: Boolean = false
  var enableOrcDumpWithSpark: Boolean = false
  var handleFileSizeOrder: String = "asc"
  // 标识是否开启分流,such as: vipdw.tableA,vipdw.tableB
  var enableGobalSplitFlow = false
  var splitFlowCluster = ""
  var targetTableToEcOrCombine: String = ""
  var shutdownSparkContextForcely = false
  var enableMaxRecordsPerFile: Boolean = false
  var fileCombineThreshold: Long = _
  var submitSparkShell: Boolean = _
  var testMode: Boolean = false
  val sdf = new SimpleDateFormat("yyyyMMdd")
  var splitFlowLevel: Int = 1

  //  ExtClasspathLoader.loadClasspath(new File(
  //    "/home/vipshop/platform/spark-3.0.1/jars/spark-sql_2.12-3.0.1-SNAPSHOT.jar"))
  //  // todo ???没法导包？？
  //  Class.forName("org.apache.spark.sql.SparkSession").newInstance()

  def getTable(): String = {
    targetTableToEcOrCombine.split("&").map("'" + _ + "'").mkString(",")
  }

  /** 获取source目录的总的文件数和总大小 */
  def computeSomeMeta(record: Record) = {
    InnerLogger.debug(InnerLogger.ENCAP_MOD, "start to computeSomeMeta...")
    // 获取source目录的总的文件数和总大小
    val countRes = s"hdfs dfs -count ${record.location}".!!
      .split(" ").filter(!_.equals(""))
    val initFileNumsCmd = countRes(1).stripMargin
    val realTotalFileSize = countRes(2).stripMargin.toLong
    val mysqlTotalFileSize = record.totalFileSize
    record.initFileNums = initFileNumsCmd.toLong
    if (realTotalFileSize != record.totalFileSize) {
      // 对mysql中获取的file_size进行修正，如，避免mysql中file_size为0的情况
      record.totalFileSize = realTotalFileSize
    }
    InnerLogger.info(InnerLogger.ENCAP_MOD,
      s"record.initFileNums:${record.initFileNums},record.totalFileSize:${record.totalFileSize}," +
        s"TotalFileSize in mysql is: ${mysqlTotalFileSize}")
  }

  def computeExpectedFileNums(record: Record) = {
    InnerLogger.debug(InnerLogger.ENCAP_MOD, "start to computeExpectedFileNums...")
    var computedParallelism = (record.totalFileSize / record.maxPartitionBytes).toInt
    // 等价于s"hdfs dfs -count ${record.location}" + " | awk -F ' ' '{print $2}'"

    InnerLogger.info(InnerLogger.ENCAP_MOD, s"initial file numbers: ${record.initFileNums}")
    if (record.initFileNums < computedParallelism) computedParallelism = record.initFileNums.toInt
    if (computedParallelism < 1) computedParallelism = 1
    record.computedParallelism = computedParallelism
    InnerLogger.info(InnerLogger.ENCAP_MOD, "spark.default.parallelism: " + computedParallelism)
  }

  def computeMaxPartitionBytes(record: Record) = {
    InnerLogger.debug(InnerLogger.ENCAP_MOD, "start to computeMaxPartitionBytes...")
    if (record.largestFileSize.toLong < fileCombineThreshold)
      record.maxPartitionBytes = fileCombineThreshold
    else
      record.maxPartitionBytes = record.largestFileSize.toLong
  }

  /** 计算record的分区级数 */
  def computeNumOfPartitions(record: Record) = {
    import java.io.{File, FileOutputStream}
    s"mkdir -p ${tmpParentPath}".!
    val tmpFileName = tmpParentPath + "numOfPartitionLevel_" + record.jobId + ".sh"
    val numOfPartitionLevelCmd =
      s"""hive -e "set hive.compute.query.using.stats=false;
         |         set mapreduce.job.queuename=${yarnQueue};
         |show partitions ${record.dbName}.${record.tblName}" | head -n 1 | awk '{s+=gsub(/=/,"&")}END{print s}'
         |""".stripMargin

    //todo 如果文件存在？
    // todo 统一规范到/tmp/ec目录下
    val cmd0 = s"touch ${tmpFileName}"
    val cmd1 = s"chmod 777 ${tmpFileName}"
    runCmd(cmd0, Some(record), "computeMaxSizeOfSingleFile")
    val file = new File(tmpFileName)
    new FileOutputStream(file).write(numOfPartitionLevelCmd.getBytes(defaultCharset))
    runCmd(cmd1, Some(record), "computeMaxSizeOfSingleFile")
    InnerLogger.debug(InnerLogger.ENCAP_MOD, "start to get partition nums...")
    //    val p = Runtime.getRuntime.exec(s"sh ${tmpFileName}")
    //    var numOfPartitionLevel = ""
    //    val reader = new BufferedReader(new InputStreamReader(p.getInputStream))
    //    numOfPartitionLevel = reader.lines().findFirst().get()
    val numOfPartitionLevel = s"sh ${tmpFileName}".!!.stripSuffix("\n").stripMargin
    assert(numOfPartitionLevel.toInt >= 0, "numOfPartitionLevel<0 or numOfPartitionLevel" +
      "can not be converted to int!")
    InnerLogger.info(InnerLogger.ENCAP_MOD, s"numOfPartitionLevel: ${numOfPartitionLevel}")
    record.numOfPartitionLevel = numOfPartitionLevel
  }

  def computeMaxSizeOfSingleFile(record: Record) = {
    // 计算有几级分区
    InnerLogger.debug(InnerLogger.ENCAP_MOD, "start to computeMaxSizeOfSingleFile...")
    var countCmd = s"hdfs dfs -count ${record.location}".stripSuffix("/") + "/*"
    for (i <- Range(1, record.numOfPartitionLevel.toInt)) {
      countCmd = countCmd + "/*"
    }
    InnerLogger.info(InnerLogger.ENCAP_MOD, "countCmd when searching largest file is : " + countCmd)

    // 等价于 countCmd #| "awk '{print $3}'" #| "sort -nr" #| "head -n1" !!
    val avgFileSize = record.totalFileSize / record.initFileNums + ""
    try {
      val fileSizes = countCmd.!!.split("\n").map(_.split(" ").filter(!_.equals(""))(2).toLong)
      record.largestFileSize = java.util.Arrays.stream(fileSizes).max().getAsLong.toString
      if (record.largestFileSize.toDouble > avgFileSize.toDouble * 1.2) {
        record.largestFileSize = avgFileSize
      }
    } catch {
      case e: Exception => record.largestFileSize = avgFileSize
    }

    InnerLogger.info(InnerLogger.ENCAP_MOD, s"largest file size is : ${record.largestFileSize}")
  }

  def runCmd(cmd: String, mysqlId: String, moduleName: String, failStatus: Int = CMD_EXECUTE_FAILED): Unit = {
    val value = cmd.!
    if (value != 0) {
      MysqlSingleConn.updateStatus(jobType.mysqlStatus, failStatus, mysqlId.toInt)
      InnerLogger.error(moduleName, s"execute cmd: [${cmd}] failed!")
      throw new RuntimeException(s"execute cmd: [${cmd}] failed!")
    }
  }

  def runCmd(cmd: String, record: Option[Record], moduleName: String): Unit = {
    val value = cmd.!
    if (value != 0) {
      MysqlSingleConn.updateStatus(jobType.mysqlStatus, 3, record.get.getId)
      InnerLogger.error(moduleName, s"execute cmd: [${cmd}] failed!")
      throw new RuntimeException(s"execute cmd: [${cmd}] failed!")
    }
  }

  def runCmd(cmd: String, record: Option[Record], moduleName: String, isSuccessCmd: String): Unit = {
    val value = cmd.!
    if (value != 0) {
      // 如果cmd失败，再执行isSuccessCmd,如果成功，则认为cmd也是成功，如果失败，则更新mysql
      runCmd(isSuccessCmd, record, moduleName)
    }
  }

  def loadJars(file: File, jars: java.util.ArrayList[String]) {
    //    LOG.info("load Classpath of dir : " + file.getAbsolutePath());
    if (file.isDirectory()) {
      val subFiles: Array[File] = file.listFiles();
      if (subFiles != null) {
        for (subFile <- subFiles) {
          loadJars(subFile, jars)
        }
      }
    } else {
      if (file.getAbsolutePath().endsWith(JAR_SUFFIX) || file.getAbsolutePath().endsWith(ZIP_SUFFIX)) {
        jars.add(file.getAbsolutePath)
      }
    }
  }

  def setTestMode(mysqlConfig: List[String]) = {
    testMode = true
    defaultMySQLConfig = mysqlConfig
  }

  def initParams(params: Array[String]): Unit = {
    val args: ArrayBuffer[String] = new ArrayBuffer[String]()

    // 数坊(spark-jar)参数：
    // -SPARK_ARGUS
    // #{dw.action.id}
    // #{dw.action.schedule.id}
    // bip_cloddata_other_need_ec_list_test
    // root.basic_platform.critical
    // 0
    // 10

    if (params(0).equalsIgnoreCase("true")) {
      // 开启线上测试模式
      onlineTestMode = params(0).toBoolean
      params.zipWithIndex.foreach(v => {
        if (v._2 != 0) {
          args += v._1
        }
      })
      InnerLogger.info("initParams", "start with test mode!")
    } else {
      params.foreach(args += _)
      assert(args != null && args.size >4, "num of params less than 5!")
      assert(args(4).toInt == 0 || args(4).toInt == 1, "jobType must be 0 or 1," +
        " 0 represent ec and 1 represent file_combine!")
      if (args.size > 8) assert(args(8).equalsIgnoreCase("asc")
        || args(8).equalsIgnoreCase("desc"), "handleFileSizeOrder must be asc or desc!")
    }

    actionId = args(0)
    actionSid = args(1)
    targetMysqlTable = args(2)
    yarnQueue = args(3)
    jobType = if (args(4).toInt == 0) JobType.ec else JobType.fileCombine
    batchSize = if (args.size > 5) args(5).toInt else 10
    sparkConcurrency = if (args.size > 6) args(6).toInt else 1
    enableFileSizeOrder = if (args.size > 7) args(7).toBoolean else true
    handleFileSizeOrder = if (args.size > 8) args(8) else "asc"
    enableGobalSplitFlow = if (args.size > 9) args(9).toBoolean else false
    splitFlowCluster = if (args.size > 10) args(10) else ""
    targetTableToEcOrCombine = if (args.size > 11) args(11) else ""
    shutdownSparkContextForcely = if (args.size > 12) args(12).toBoolean else false
    enableMaxRecordsPerFile = if (args.size > 13) args(13).toBoolean else false
    filesTotalThreshold = if (args.size > 14) args(14).toLong else 20000
    onlyHandleOneLevelPartition = if (args.size > 15) args(15).toBoolean else true
    enableFineGrainedInsertion = if (args.size > 16) args(16).toBoolean else false
    onlyCoalesce = if (args.size > 17) args(17).toBoolean else false
    enableHandleBucketTable = if (args.size > 18) args(18).toBoolean else false
    enableFileCountOrder = if (args.size > 19) args(19).toBoolean else true
    splitFlowLevel = if (args.size > 20) args(20).toInt else 1
    enableOrcDumpWithSpark = if (args.size > 21) args(21).toBoolean else false
    fileCombineThreshold = if (args.size > 22) args(22).toLong else 104857600
    submitSparkShell = if (args.size > 23) args(23).toBoolean else true
  }

  def main(args: Array[String]): Unit = {
    // $SPARK_HOME/bin/spark-submit --class org.apache.spark.sql.EcAndFileCombine ./ec-with-dep3.jar
    initParams(args)
    val executor = new EcAndFileCombine
    if (onlineTestMode) {
      // for test
      executor.trigger()
    }
    // 增加hook函数
    Runtime.getRuntime.addShutdownHook(executor.shutdownHook)
    executor.encapsulateWork()
    executor.scheduleWork()
    executor.checkWork()
    Runtime.getRuntime.removeShutdownHook(executor.shutdownHook)
    sys.exit(0)
  }

}

import java.util.concurrent._
class EcAndFileCombine {

  var spark: SparkSession = null
  var curJobs = new ConcurrentHashMap[String, String]()
  val idToFlag = new ConcurrentHashMap[String, Int]()
  val idToRollBackCmd = new ConcurrentHashMap[String, String]()
  // 记录了已经status设置为1的这些record,shutdownHook中可以根据这个map更新status
  var records = new mutable.HashMap[Int, Record]()
  var jobIdToJobStatus = new ConcurrentHashMap[String, java.util.HashMap[String, String]]()
  var circTimes = 1
  var currentStep: JobType.JobStep = JobType.encapsulateWork
  // 无法避免kill -9
  val shutdownHook = new Thread(new Runnable {
    override def run(): Unit = {
      if (curJobs.isEmpty) {
        return
      }
      // 原子性保证②
      idToFlag.forEach((id, flag) => {
        if (flag >= 1 && flag < 3) {
          try {
            val rollbackCmd = idToRollBackCmd.get(id)
            runCmd(rollbackCmd, id, "shutdown_hook")
          } catch {
            case e: Exception =>
          }
        } else if (flag == 3) {
          // todo test
          curJobs.remove(id)
        }
      })

      // 如果有midDTLocation,删掉中间目录
      val mapper = new ObjectMapper()
      curJobs.forEach((id, json) => {
        val map = mapper.readValue(json, classOf[java.util.HashMap[String, String]])
        val midDtLocation = map.get(MID_DT_LOCATION)
        // todo 测试，是否存在mid路径有问题的场景？
        if (!midDtLocation.equals("")) {
          try {
            s"hdfs dfs -rm -r ${midDtLocation}".!
          } catch {
            case e: Exception =>
          }
        }
      })

      MysqlSingleConn.init()
      var ids: Array[Int] = null
      currentStep match {
        case JobType.encapsulateWork =>
          // 更新mysql状态，设置status=0
          // 这边获取records中的mysql记录，而不是curJobs,因为
          // records中的记录一旦封装好，就可以保证退出时status回归为0
          ids = records.map(_._1.toInt).toArray
          MysqlSingleConn.batchUpdateStatus(jobType.mysqlStatus, INIT_CODE, ids)
        case _ =>
          // 更新mysql状态，设置status=6
          import scala.collection.JavaConverters._
          val stream: java.util.stream.Stream[Int] = curJobs.entrySet().stream().map(_.getKey.toInt)
          ids = stream.collect(Collectors.toList[Int]).asScala.toArray
          MysqlSingleConn.batchUpdateStatus(jobType.mysqlStatus, PROCESS_KILLED, ids)
      }
      MysqlSingleConn.close()
    }
  })

  import EcAndFileCombine._
  class EncapsulateJob(record: Record) extends Runnable {

    override def run(): Unit = {
      InnerLogger.debug(InnerLogger.ENCAP_MOD, s"start to " +
        s"encapsulate one record[${record.toSimpleString}]...")
      assert(record != null)
      // source目录如果不存在，则放弃Job，并更新mysql状态为跳过(4)。
      val sourceExist: String = s"hdfs dfs -test -e ${record.location}"
      val code: Int = sourceExist.!
      if (code != 0) {
        // source目录不存在
        InnerLogger.warn(InnerLogger.ENCAP_MOD, s"source目录[${record.location}]不存在，跳过该作业!")
        MysqlSingleConn.updateStatus(jobType.mysqlStatus, SKIP_WORK, record.getId)
        return
      }

      computeNumOfPartitions(record)
      // 如果numOfPartitionLevel>1，那么有多级分区，采用动态分区写入数据
      MysqlSingleConn.updateStatus(jobType.numPartitions, record.numOfPartitionLevel.toInt, record.getId)
      if (onlyHandleOneLevelPartition && record.numOfPartitionLevel.toInt > 1) {
        MysqlSingleConn.updateStatus(jobType.mysqlStatus, 0, record.getId)
        return
      }

      // source目录存在，创建中间表路径
      InnerLogger.debug(InnerLogger.ENCAP_MOD, "start to mkdir of midDtLocation...")
      val midDtLocation = record.midDTLocation
      // mkdir,创建dt级别的location目录
      val cmd = s"hdfs dfs -mkdir -p ${midDtLocation}"
      val cmd2 = s"hdfs dfs -test -e ${midDtLocation}"
      runCmd(cmd, Some(record), InnerLogger.ENCAP_MOD, cmd2)
      jobType match {
        case JobType.ec =>
          // fixme 如果此时，已经有该分区在做小文件合并，怎么处理？
          // set ec policy
          if (cmd2.! == 0) {
            InnerLogger.debug(InnerLogger.ENCAP_MOD, s"directory of midDtLocation[${midDtLocation}] exists.")
          }
          // todo 做过ec了，跳过！
          // todo 分流时,如果做过了ec,不要跳过
          val cmd = s"hdfs ec -setPolicy -path ${record.midTblLocation}" +
            s" -policy ${ecPolicy}"
          InnerLogger.debug(InnerLogger.ENCAP_MOD, s"start to set ec policy[${cmd}]...")
          runCmd(cmd, Some(record), InnerLogger.ENCAP_MOD)
        case JobType.fileCombine =>
          // check当前dt目录是否设置为了ec目录
          val cmd = s"hdfs ec -getPolicy -path ${record.midTblLocation}"
          if (s"${ecPolicy}".equals(cmd.!!)) {
            // 该目录正在做ec，跳过小文件合并（ec时会合并小文件）
            MysqlSingleConn.updateStatus(jobType.mysqlStatus, SKIP_WORK, record.getId)
            return
          }
      }

      computeSomeMeta(record)
      computeMaxSizeOfSingleFile(record)
      computeMaxPartitionBytes(record)
      computeExpectedFileNums(record)

      // 封装job为json
      val mapper = new ObjectMapper()
      val recordStr = mapper.writeValueAsString(record.toMap)
      curJobs.put(record.id.toString, recordStr)
      InnerLogger.info(InnerLogger.ENCAP_MOD, s"record.json: \n${recordStr}")
    }
  }

  def deleteFileIfExist(toDelLocation: String): Unit = {
    try {
      if (s"hdfs dfs -test -e ${toDelLocation}".!!.toInt == 0) {
        s"hdfs dfs -rm -r ${toDelLocation}".!
      }
    } catch {
      case e: Exception =>
    }
  }

  import EcAndFileCombine._
  import JobType._
  class CheckSingleWork(workJson: String) extends Runnable {
    override def run(): Unit = {
      InnerLogger.debug(InnerLogger.CHECK_MOD, s"start to check partition...")
      val mapper = new ObjectMapper()
      val record: java.util.HashMap[String, String] = mapper.readValue(workJson, classOf[java.util.HashMap[String, String]])
      // check success file
      InnerLogger.debug(InnerLogger.CHECK_MOD, "start to check success file...")
      val successFileLocation = record.get(SUCCESS_FILE_LOCATION)
      val cmd = s"hdfs dfs -test -e ${successFileLocation}"
      if (cmd.! != 0) {
        // success file 不存在，删除midLocation
        deleteFileIfExist(record.get(MID_DT_LOCATION))
        InnerLogger.info(InnerLogger.CHECK_MOD,
          s"success file 不存在,midDTLocation[${record.get(MID_DT_LOCATION)}] 成功地被删除或者本就不存在!")
        MysqlSingleConn.updateStatus(jobType.mysqlStatus, SUCCESS_FILE_MISSING, record.get(MYSQL_ID).toInt)
        // todo 类似的return需要加上
        return
      }

      // should dump orc
      val workSchema = s"hdfs dfs -cat ${successFileLocation}".!!
      // todo 如果读取失败？
      val successSchema: java.util.HashMap[String, String] = mapper.readValue(workSchema, classOf[java.util.HashMap[String, String]])
      jobType match {
        case JobType.ec =>
          InnerLogger.debug(InnerLogger.CHECK_MOD, "this is an ec job...")
          val inputFormat = successSchema.get(INPUT_FORMAT)
          val outputFormat = successSchema.get(OUTPUT_FORMAT)
          if (!enableGobalSplitFlow && ORC_INPUT_FORMAT.equalsIgnoreCase(inputFormat)
            && ORC_OUTPUT_FORMAT.equalsIgnoreCase(outputFormat)) {
            // execute orc file dump
            InnerLogger.debug(InnerLogger.CHECK_MOD, s"this is an ec job,start to dump orc file[${successSchema.get(MID_DT_LOCATION)}]...")

            val toDumpPath = successSchema.get(MID_DT_LOCATION)
            if (enableOrcDumpWithSpark) {
              val bool = dumpOrcFileWithSpark(spark, toDumpPath)
              if (!bool) {
                MysqlSingleConn.updateStatus(jobType.mysqlStatus, ORC_DUMP_FAILED, successSchema.get(MYSQL_ID).toInt)
                InnerLogger.error(InnerLogger.CHECK_MOD, s"dump orc file[${toDumpPath}] failed!")
                throw new RuntimeException(s"dump orc file failed!")
              }
            } else {
              val dumpCmd = s"hive --orcfiledump ${toDumpPath}"
              runCmd(dumpCmd, successSchema.get(MYSQL_ID), InnerLogger.CHECK_MOD, ORC_DUMP_FAILED)
            }
            InnerLogger.info(InnerLogger.CHECK_MOD, s"dump orc file[${successSchema.get(MID_DT_LOCATION)}] successfully!")
          }
        case JobType.fileCombine =>
          InnerLogger.debug(InnerLogger.CHECK_MOD, "this is a fileCombine job...")
      }

      // change dir of source and mid
      // 删除success file
      InnerLogger.debug(InnerLogger.CHECK_MOD, s"delete success file[${successSchema.get(SUCCESS_FILE_LOCATION)}]...")
      s"hdfs dfs -rm ${successSchema.get(SUCCESS_FILE_LOCATION)}".!
      InnerLogger.debug(InnerLogger.CHECK_MOD, "start to change dir of source and mid...")
      val toBeDelLocation = successSchema.get(TO_BE_DEL_LOCATION)
      val boundToBeDelLocation = successSchema.get(BOUND_TO_BE_DEL_LOCATION)
      val tempToBeDelLocation = toBeDelLocation.stripSuffix("/") + "/" + successSchema.get(FIRST_PARTITION)
      val renameTempToDelDirCmd = s"hdfs dfs -mv ${tempToBeDelLocation} ${boundToBeDelLocation}"
      val moveSourceCmd = s"hdfs dfs -mv ${successSchema.get(LOCATION)}" +
        s" ${toBeDelLocation}"
      val mkdirCmd = s"hdfs dfs -mkdir -p ${toBeDelLocation}"
      InnerLogger.info(InnerLogger.CHECK_MOD, s"mkdir toBeDelLocation dir: ${mkdirCmd}")
      mkdirCmd.!
      InnerLogger.info(InnerLogger.CHECK_MOD, s"move source location to to_be_deleted dir: ${moveSourceCmd}")
      val moveMidCmd = s"hdfs dfs -mv ${successSchema.get(MID_DT_LOCATION)}" +
        s" ${successSchema.get(DEST_TBL_LOCATION)}"
      if (enableGobalSplitFlow) {
        s"hdfs dfs -mkdir -p ${successSchema.get(DEST_TBL_LOCATION)}".!
        InnerLogger.debug(InnerLogger.CHECK_MOD, s"executed mkdir of destTblLocation:" +
          s"${successSchema.get(DEST_TBL_LOCATION)}")
      }
      // TODO test rollback
      val rollbackCmd = s"hdfs dfs -mv ${successSchema.get(TO_BE_DEL_LOCATION)}/${successSchema.get(FIRST_PARTITION)} " +
        s"${successSchema.get(SOURCE_TBL_LOCATION)}"
      InnerLogger.info(InnerLogger.CHECK_MOD, s"rollbackCmd: ${rollbackCmd}")
      val mysqlId = successSchema.get(MYSQL_ID)
      val firstPartitionArr = successSchema.get(FIRST_PARTITION).split("=")
      assert(firstPartitionArr.size == 2)
      val partitionSql = firstPartitionArr(0) + "='" + firstPartitionArr(1) + "'"
      val alterDtLocationSql = s"alter table ${successSchema.get(DB_NAME) + "." + successSchema.get(TBL_NAME)}" +
        s" partition(${partitionSql}) set location '${successSchema.get(DEST_TBL_LOCATION).stripSuffix("/")}/" +
        s"${successSchema.get(FIRST_PARTITION)}'"

      val fineGrainedPartitionSql = successSchema.get(PARTITION_SQL)
      InnerLogger.debug(InnerLogger.CHECK_MOD, s"fineGrainedPartitionSql:${fineGrainedPartitionSql}")

      // 以下操作具有原子性:①moveSourceCmd之后的命令抛异常 ②moveSourceCmd成功之后jvm随即退出
      idToRollBackCmd.put(mysqlId, rollbackCmd)
      idToFlag.put(mysqlId, 0)
      MysqlSingleConn.updateStatus(jobType.mysqlStatus, SOURCE_IN_SOURCE_DIR, mysqlId.toInt)
      runCmd(moveSourceCmd, mysqlId, InnerLogger.CHECK_MOD)
      // 数据此时在backup/mid_tbl_to_be_deleted/__temporary中
      MysqlSingleConn.updateStatus(jobType.mysqlStatus, SOURCE_IN_TEMPORARY_DIR, mysqlId.toInt)
      // 如果到这一步,下面的命令必须执行成功,否则就需要回滚上一步命令
      try {
        idToFlag.put(mysqlId, 1)
        InnerLogger.info(InnerLogger.CHECK_MOD, s"move mid location to source dir: ${moveMidCmd}")
        runCmd(moveMidCmd, mysqlId, InnerLogger.CHECK_MOD)
        MysqlSingleConn.updateStatus(jobType.mysqlStatus, DATA_IN_DEST_DIR, mysqlId.toInt)
        idToFlag.put(mysqlId, 2)
        InnerLogger.debug(InnerLogger.CHECK_MOD, "start to update mysql...")
        if (enableGobalSplitFlow) {
          MysqlSingleConn.updateStatus("split_flow_status", START_SPLIT_FLOW, mysqlId.toInt)
          // 如果是分流，那么需要修改dt级别的location
          if (fineGrainedPartitionSql != null && !fineGrainedPartitionSql.equals("")) {
            val alterSqls = fineGrainedPartitionSql.split(SPLIT_DELIMITER)
            alterSqls.foreach(sql => {
              InnerLogger.debug(InnerLogger.CHECK_MOD, s"start to alter location:${sql}")
              spark.sql(sql)
            })
          } else {
            spark.sql(alterDtLocationSql)
          }
          val updateSuccessSql =
            s"""
              |update ${targetMysqlTable}
              |set split_flow_status = ${SUCCESS_CODE},${jobType.mysqlStatus} = ${SUCCESS_CODE}
              |where id = ${mysqlId}
              |""".stripMargin

          if (MysqlSingleConn.updateQuery(updateSuccessSql) <= 0){
            InnerLogger.error(InnerLogger.CHECK_MOD,s"update success status failed! [sql: ${updateSuccessSql}]")
            throw new RuntimeException(s"update success status failed! [sql: ${updateSuccessSql}]")
          }

          InnerLogger.debug(InnerLogger.CHECK_MOD, s"split flow and execute alter location of" +
              s" dest dt location:${alterDtLocationSql}")

        } else {
          MysqlSingleConn.updateStatus(jobType.mysqlStatus, SUCCESS_CODE, mysqlId.toInt)
        }
        idToFlag.put(successSchema.get(MYSQL_ID), 3)
      } finally {
        if (idToFlag.get(mysqlId) != 3) {
          // 原子性①:回滚
          runCmd(rollbackCmd, mysqlId, InnerLogger.CHECK_MOD)
        }
      }
      // 以上操作具有原子性

      // 开启分流修改location及cluster
      if (enableGobalSplitFlow) {
        val initCluster = ("//[^/]*/".r findFirstIn successSchema.get(LOCATION)).get.replaceAll("/", "")
        // MysqlSingleConn.updateStatus("path_cluster", "'" + successSchema.get(CLUSTER) + "'", successSchema.get(MYSQL_ID).toInt)
        val destDtLocation = successSchema.get(DEST_TBL_LOCATION).stripSuffix("/") + "/" + successSchema.get(FIRST_PARTITION)
        val fileCountOld = successSchema.get(INIT_FILE_NUMS)
        val fileCount = successSchema.get(COMBINED_FILE_NUMS)
        // location <- destDtLocation
        // 更新location和cluster
        val updateLocationAndClusterSql =
          s"""
             |update ${targetMysqlTable}
             |set path_cluster = '${successSchema.get(CLUSTER)}',location = '${destDtLocation}',cluster_old = '${initCluster}',file_count = ${fileCount},file_count_old = ${fileCountOld}
             |where id = ${successSchema.get(MYSQL_ID)}
             |""".stripMargin
        if (MysqlSingleConn.updateQuery(updateLocationAndClusterSql) <= 0){
          InnerLogger.error(InnerLogger.CHECK_MOD,s"update location and cluster_old failed! [sql: ${updateLocationAndClusterSql}]")
          throw new RuntimeException(s"update location and cluster_old failed! [sql: ${updateLocationAndClusterSql}]")
        } else {
          InnerLogger.debug(InnerLogger.CHECK_MOD, s"split flow and execute alter location of" +
            s" dest dt location:${alterDtLocationSql}")
        }
        // todo drop mid dt location
      }

      // rename tobedelete to boundtobedelete
      // 保证该命令后，没有 mysql或者hdfs的修改操作
      s"hdfs dfs -mkdir -p ${boundToBeDelLocation}".!
      runCmd(renameTempToDelDirCmd, mysqlId, InnerLogger.CHECK_MOD)

      // 更新status为mysql中的最新值。
      val rs = MysqlSingleConn.executeQuery(s"select ${jobType.mysqlStatus} from " +
        s"${targetMysqlTable} where id=${successSchema.get(MYSQL_ID)}")
      if (rs.next()) {
        val status = rs.getInt(1)
        if (status == SUCCESS_CODE) {
          MysqlSingleConn.updateStatus("split_flow_status", SUCCESS_CODE, mysqlId.toInt)
        }
        successSchema.put(jobType.mysqlStatus, status.toString)
      }

      jobIdToJobStatus.put(successSchema.get(JOB_ID), successSchema)
      InnerLogger.info(InnerLogger.CHECK_MOD, s"record[${successSchema.toString}] check successfully")
    }
  }

  /** return 是否继续寻找合适的数据 */
  def encapsulateTargetSizeWork(targetBatchSize: Int, pool: ThreadPoolExecutor): Boolean = {
    // 获取record
    // todo 支持事务吗？
    // todo 如果是非表等数据，过滤掉
    // !targetTableToEcOrCombine.equals("")条件：
    // 分流时，根据main函数的入参，指定需要分流的表名
    // enableGobalSplitFlow && targetTableToEcOrCombine.equals("")条件：
    // 对应开启分流的同时，没有指定需要分流的表，那么，根据split_flow_status = 1来寻找分流的表
    val getIds =
    s"""
       |select group_concat(t.id) from (select id from ${targetMysqlTable}
       |    where ${jobType.mysqlStatus} = 0
       |    ${if (onlyHandleOneLevelPartition) "and " + jobType.numPartitions + " <= 1" else " "}
       |    ${if (!targetTableToEcOrCombine.equals("")) " and concat(db_name, '.', tbl_name) in (" + getTable + ")" else " "}
       |    ${if (enableFileCountOrder && enableGobalSplitFlow && targetTableToEcOrCombine.equals("")) s" and (split_flow_status = ${splitFlowLevel} or (split_flow_status <0 and split_flow_status>-5) ) " else " "}
       |    ${if (enableFileCountOrder) "order by split_flow_status " else " "}
       |    ${if (!enableFileCountOrder && enableFileSizeOrder) "order by file_size " + handleFileSizeOrder else " "}
       |    limit ${targetBatchSize}) t
       |""".stripMargin

    val rs1 = MysqlSingleConn.executeQuery(getIds)
    var ids: String = ""
    var shouldContinue: Boolean = false
    while (rs1.next()) {
      shouldContinue = true
      ids = rs1.getString(1)
      if ("null".equalsIgnoreCase(ids) || ids == null) {
        shouldContinue = false
      }
    }
    InnerLogger.debug(InnerLogger.ENCAP_MOD, s"ids: [${ids}]; sql of getIds:[${getIds}]")
    if (!shouldContinue) {
      InnerLogger.warn(InnerLogger.ENCAP_MOD, s"ids: ${ids}, no suitable record found in mysql,exit!")
      if (curJobs.size() > 0) {
        return false
      }
      sys.exit(0)
    }

    if (!onlineTestMode) {
      var updateSql = ""
      try {
        updateSql =
          s"""
             |update ${targetMysqlTable} set ${jobType.mysqlStatus} = 1 where id in (${ids}) and ${jobType.mysqlStatus} = 0;
             |""".stripMargin
        if (MysqlSingleConn.updateQuery(updateSql) <= 0) {
          InnerLogger.error(InnerLogger.ENCAP_MOD, s"update ${jobType.mysqlStatus} to 1 failed! [sql: ${updateSql}]")
          // sys.exit(1)
          InnerLogger.warn(InnerLogger.ENCAP_MOD, s"execute updateSql[${updateSql}] failed," +
            s" and continue retrying getting datasource for more [${10 - circTimes}] times!")
          return true
        }

      } catch {
        case e: Exception =>
          // updata失败，尝试重新获取数据源
          InnerLogger.warn(InnerLogger.ENCAP_MOD, s"execute updateSql[${updateSql}] failed," +
            s" and continue retrying getting datasource for more [${10 - circTimes}] times!")
          return true
      }
    }

    // !targetTableToEcOrCombine.equals("")条件：
    // 分流时，根据main函数的入参，指定需要分流的表名
    // enableGobalSplitFlow && targetTableToEcOrCombine.equals("")条件：
    // 对应开启分流的同时，没有指定需要分流的表，那么，根据split_flow_status = 1来寻找分流的表
    val getDatasourceSql =
    s"""
       |select id,db_name,tbl_name,location,first_partition,${jobType.mysqlStatus},path_cluster,dt,file_size
       |    from ${targetMysqlTable} where ${if (!onlineTestMode) jobType.mysqlStatus + " = 1 and " else " "} id in (${ids})
       |    ${if (!targetTableToEcOrCombine.equals("")) " and concat(db_name, '.', tbl_name) in (" + getTable + ")" else " "}
       |    ${if (enableFileCountOrder && enableGobalSplitFlow && targetTableToEcOrCombine.equals("")) s" and (split_flow_status = ${splitFlowLevel} or (split_flow_status <0 and split_flow_status>-5) ) " else " "};
       |""".stripMargin
    InnerLogger.debug(InnerLogger.ENCAP_MOD, s"sql to get datasource: ${getDatasourceSql}")
    val rs = MysqlSingleConn.executeQuery(getDatasourceSql)
    while (rs.next()) {
      val record = new Record(rs.getInt("id"))
      record.dbName = rs.getString("db_name")
      record.tblName = rs.getString("tbl_name")
      record.location = rs.getString("location")
      record.firstPartition = rs.getString("first_partition")
      record.status = rs.getInt(jobType.mysqlStatus)
      record.cluster = rs.getString("path_cluster")
      record.dt = rs.getString("dt")
      record.totalFileSize = rs.getLong("file_size")
      record.enableSplitFlow = enableGobalSplitFlow

      // 分流相关
      // 分流的表需要更新mysql中的cluster,或者从外部参数获取
      val initCluster = ("//[^/]*/".r findFirstIn record.location).get.replaceAll("/", "")
      val prefix = "hdfs://" + initCluster
      // 如果在main函数入参中开启分流，且指定了分流的目标集群，那么就会将cluster替换成目标集群
      // 另外，如果mysql数据源中将cluster修改成其他集群，在开启分流的情况下，才会将cluster替换成目标集群
      if (record.enableSplitFlow && !splitFlowCluster.equals("")) {
        record.cluster = splitFlowCluster
      } else if (record.enableSplitFlow && splitFlowCluster.equals("")
        && !initCluster.equals(record.cluster)) {
        record.cluster = initCluster
      }

      record.midTblName = record.tblName + "___ec_or_combine_mid__"
      record.midTblLocation = s"hdfs://${record.cluster}/backup/mid_tbl_to_check/" +
        s"${record.dbName}/${record.tblName}"
      record.midDTLocation = s"${record.midTblLocation}/${record.firstPartition}"
      // 这部分数据可能会有回滚的需求
      record.toBeDelLocation = s"hdfs://${initCluster}/backup/mid_tbl_to_be_deleted/__temporary/" +
        s"${record.dbName}/${record.tblName}"
      // 这部分数据是肯定成功的数据，必然会删除的
      record.boundToBeDelLocation = s"hdfs://${initCluster}/backup/mid_tbl_to_be_deleted/bak_dt=${sdf.format(new Date())}/" +
        s"${record.dbName}/${record.tblName}"
      record.successFileLocation = s"${record.midDTLocation}".stripSuffix("/") + "/.COMBINE_SUCCESS"
      // note 只覆盖了分区表的场景
      record.sourceTblLocation = record.location.stripSuffix(record.firstPartition)
      // 分流相关
      record.destTblLocation =
        record.sourceTblLocation.replace(prefix, s"hdfs://${record.cluster}")

      records.put(record.getId, record)
      InnerLogger.debug(InnerLogger.ENCAP_MOD, s"records.size:${records.size}\tcurrent record : " + record.toString)
    }

    if (!onlineTestMode) {
      val updateActionIdSql =
        s"""
           |update ${targetMysqlTable}
           |set action_id = ${actionId},action_sid = ${actionSid}
           |where ${jobType.mysqlStatus} = 1 and id in (${ids});
           |""".stripMargin
      if (MysqlSingleConn.updateQuery(updateActionIdSql) <= 0) {
        InnerLogger.error(InnerLogger.ENCAP_MOD, s"update actionId and actionSid failed! [sql: ${updateActionIdSql}]")
        sys.exit(1)
      }
    }
    /** step1: get datasource | end ***********************************************************************************/

    /** step2: mkdir and encapsulate job | start **********************************************************************/
    val futureList = new ArrayBuffer[Future[_]]
    val recordsArr = records.values.toArray
    assert(recordsArr != null, "recordsArr should not be null!")
    InnerLogger.info(InnerLogger.ENCAP_MOD, s"records size is :${recordsArr.size};batch size is :${targetBatchSize}")
    for (i <- recordsArr.indices) {
      val future: Future[_] = pool.submit(new EncapsulateJob(recordsArr(i)))
      futureList += future
    }
    futureList.foreach(f =>
      try { f.get() }
      catch {
        // todo 该阶段都结束后，需要汇总执行信息，失败了多少，失败状态。
        case ex: Exception => {
          val msg = if (ex.getCause == null) ex.getMessage + "\n" + ex.getStackTrace.mkString("\n")
          else ex.getMessage + "\n" + ex.getStackTrace.mkString("\n") + "\n" + ex.getCause.toString
          InnerLogger.error(InnerLogger.ENCAP_MOD + " futureList.get", msg)
          // TODO update mysql
        }
      }
    )
    true
  }

  /** encapsulate work of ec or file_combine */
  def encapsulateWork(): Unit = {
    /** step1: get datasource | start *********************************************************************************/
    MysqlSingleConn.init()
    // precheck: 确认是否有数据需要做ec或小文件合并,没有则退出
    val resultSet = MysqlSingleConn.executeQuery("select count(id) from " +
      s"${targetMysqlTable} where ${jobType.mysqlStatus} = 0")
    if (resultSet.next()) {
      val countId = resultSet.getInt(1)
      // 如果没有需要执行的作业，退出
      if (countId <= 0) {
        InnerLogger.warn(InnerLogger.ENCAP_MOD, "no record found in mysql,exit!")
        sys.exit(0)
      }
    }

    val pool: ThreadPoolExecutor = new ThreadPoolExecutor(batchSize, batchSize, 0L,
      TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])
    while (curJobs.size() < batchSize && circTimes <= 10
      && encapsulateTargetSizeWork(batchSize - curJobs.size(), pool)) {
      InnerLogger.info(InnerLogger.ENCAP_MOD, s"continue retrying getting datasource" +
        s" for more [${10 - circTimes}] times!")
      circTimes += 1
      // todo 如果这样，targetSize越来越小，查询的次数越来越多
    }

    // 汇总所有成功的job的执行结果
    // todo 汇总所有失败的job的信息
    val metric = new StringBuffer("")
    curJobs.values().toArray.map(json => {
      val singleMetric = new StringBuffer("\n")
      parseRaw(json.toString).get.asInstanceOf[JSONObject].obj
        .foreach(kv => {
          singleMetric.append(kv._1 + "->" + kv._2 + "\t")
        })
      metric.append("record:" + singleMetric + "\n")
    })

    InnerLogger.info(InnerLogger.ENCAP_MOD + " | metric_aggregate", s"concurrent job numbers: ${curJobs.size()}\t" +
      s"Records-info: \n\t${metric.toString}")
    MysqlSingleConn.close()
    /** step2: mkdir and encapsulate job | end ************************************************************************/
  }

  /** can not work when partition schema is not in show partitions */
  def getPartitionSql(input: String, without: String): Array[String] = {
    val parts = input.split("/")
    val buffer = new ArrayBuffer[String]()
    for (i <- Range(0, parts.size)) {
      buffer.append(parts(i).split("=")(0))
    }
    buffer.toStream.filter(!without.equals(_)).toArray
  }

  def submitSparkApp(maxCores: Int, maxMem: Int, jobArray: String, test: Boolean = false) = {
    // submit spark with shell
    InnerLogger.debug(InnerLogger.SCHE_MOD, "start to submit spark app ...")
    InnerLogger.info(InnerLogger.SCHE_MOD, "submit spark app with spark-jar")

    import org.apache.spark.SparkConf
    val conf = new SparkConf()

    if (test) {
      conf.set("spark.master", "local[1]")
        .setAppName(sparkApplicationName)
    } else {
      val jars = new java.util.ArrayList[String]()
      loadJars(new File(sparkHomePath.stripSuffix("/") + "/jars"), jars)
      import scala.collection.JavaConverters._
      conf.set("spark.master", "yarn")
        .set("spark.submit.deployMode", "client")
        .set("queue", yarnQueue)
        .set("spark.driver.memory", "6G")
        .set("spark.executor.cores", maxCores.toString)
        .set("spark.executor.memory", maxMem + "G")
        .set("spark.hadoop.hive.exec.dynamic.partition", "true")
        .set("spark.hadoop.hive.exec.dynamic.partition.mode", "nostrick")
        .set("spark.hadoop.hive.exec.max.dynamic.partitions", "2000")
        .setSparkHome(sparkHomePath)
        // todo delete
        .set("SPARK_CONF_DIR", "/home/vipshop/conf/spark3_0")
        .setAppName(sparkApplicationName)
        .setJars(jars.asScala)
    }

    val builder = SparkSession.builder().config(conf)
    val mapper = new ObjectMapper()

    if (!test && conf.get("spark.sql.catalogImplementation", "hive").toLowerCase(Locale.ROOT) == "hive") {
      // In the case that the property is not set at all, builder's config
      // does not have this value set to 'hive' yet. The original default
      // behavior is that when there are hive classes, we use hive catalog.
      spark = builder.enableHiveSupport().getOrCreate()
      InnerLogger.info(InnerLogger.SCHE_MOD, "Created Spark session with Hive support")
    } else {
      // In the case that the property is set but not to 'hive', the internal
      // default is 'in-memory'. So the sparkSession will use in-memory catalog.
      spark = builder.getOrCreate()
      InnerLogger.info(InnerLogger.SCHE_MOD, "Created Spark session")
    }

    val applicationId = spark.sparkContext.applicationId
    InnerLogger.info(InnerLogger.SPARK_MOD + " applicationId ", applicationId)

    import scala.collection.immutable.Range
    import scala.collection.mutable.ArrayBuffer
    import scala.util.{Failure, Success, Try}
    /** concat partition spec in insert-sql */
    def getPartitionsInInsertSql(input: Array[String], firstPartitionStr: String): String = {
      if(input != null && input.size > 0) firstPartitionStr + "," + input.mkString(",") else firstPartitionStr
    }

    /** get all partitionName */
    def getDynamicPartitions(input: String, withoutPartition: String = null): Array[String] = {
      val pattern = "PARTITIONED BY[\\s]*\\([^(]*\\)".r
      val partitionedByStr = pattern findFirstIn input
      if (!partitionedByStr.contains("`")) {
        val getParArrStr = "\\([^(]*\\)".r
        val partitionArrStr = getParArrStr findFirstIn partitionedByStr.get
        var stream = partitionArrStr.get.stripPrefix("(").stripSuffix(")").split(",").toStream.map(_.trim)
        if (withoutPartition != null) stream = stream.filter(!withoutPartition.equals(_))
        return stream.toArray
      }
      val getDynamicPar = "`[^`]*`".r
      val partitions = getDynamicPar findAllMatchIn partitionedByStr.get
      var stream = partitions.toStream.map(_.toString().stripSuffix("`").stripPrefix("`"))
      if (withoutPartition != null) stream = stream.filter(!withoutPartition.equals(_))
      stream.toArray
    }

    def scheduleWorkArray(concurrency: Int = 1): Unit = {
      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to scheduleWorkArray...")
      val value = mapper.readValue(jobArray, classOf[java.util.List[String]])
      // todo submit to some other pool,and limit the num of submitted jobs
      var curBatchSize = 0
      var curBatchWorkJson = new ArrayBuffer[String]()

      /** 限制并发创建的小文件数量 */
      def computeConcurrency(workJsons: Array[String]): Int = {
        val concurrentFilesThreshold: Long = filesTotalThreshold
        //hdfs 获取同一个集群中，当前所有的作业，并行度(不是实际的并行度，而是并发产生的小文件数)之和

        var curEstimatedFiles: Long = 0
        var finalEstimatedFiles: Long = 0
        var maxConcurrentFiles: Long = 0
        var curConcurrency = 0
        workJsons.foreach(j => {
          val computedParallelism = mapper.readValue(j, classOf[java.util.HashMap[String, String]])
            .get(COMPUTED_PARALLELISM).toLong
          curEstimatedFiles += computedParallelism
          if (computedParallelism > maxConcurrentFiles) {
            maxConcurrentFiles = computedParallelism
          }
          if (curEstimatedFiles < concurrentFilesThreshold) {
            curConcurrency += 1
            finalEstimatedFiles += computedParallelism
          }
        })
        if (curConcurrency < 1) {
          curConcurrency = 1
          finalEstimatedFiles = maxConcurrentFiles
        }
        // todo 更新finalEstimatedFiles到hdfs?
        curConcurrency
      }

      value.forEach(t => {
        curBatchSize += 1
        curBatchWorkJson += t
        if (curBatchSize >= concurrency) {
          runMultiCombineWorkConc(computeConcurrency(curBatchWorkJson.toArray),
            curBatchWorkJson.toArray)
          curBatchSize = 0
          curBatchWorkJson = new ArrayBuffer[String]()
        }
      })
      if (curBatchWorkJson.size > 0) {
        runMultiCombineWorkConc(curBatchWorkJson.size, curBatchWorkJson.toArray)
      }
      // todo 这边后续改为并发提交job,或者更细粒度地调度同一个combine作业的不同job

    }

    def runSingleCombineWork(value: String): Unit = {
      InnerLogger.debug(InnerLogger.SPARK_MOD, s"start to runSingleCombineWork...value[${value}]")

      val schemaMap = mapper.readValue(value, classOf[java.util.HashMap[String, String]])
      val mysqlId = schemaMap.get(MYSQL_ID)
      val combineId = schemaMap.get(JOB_ID)
      val initFileNums = schemaMap.get(INIT_FILE_NUMS).toLong
      val srcTbl = schemaMap.get(DB_NAME) + "." + schemaMap.get(TBL_NAME)
      val midTblName = schemaMap.get(MID_TBL_NAME)
      val midTblLocation = schemaMap.get(MID_TBL_LOCATION)
      val midDTLocation = schemaMap.get(MID_DT_LOCATION)
      val sourceTblLocation = schemaMap.get(SOURCE_TBL_LOCATION)
      val destTblLocation = schemaMap.get(DEST_TBL_LOCATION)
      val dbName = schemaMap.get(DB_NAME)
      val defaultParallelism = schemaMap.get(COMPUTED_PARALLELISM)
      val maxPartitionBytes = schemaMap.get(MAX_PARTITION_BYTES)

      val firstPartition = schemaMap.get(FIRST_PARTITION)
      val kv = firstPartition.split("=")
      assert(kv.size == 2, "firstPartition 中不包含'=',暂时不支持这种数据!")
      val toDropPartitionField = kv(0)
      // 静态分区，如dt=20210918，获取其field，即dt
      val fieldOfStaticPartition = kv(0)
      // todo 类似于cube的，谓词怎么写
      val partitionPredicate = kv(0) + "='" + kv(1) + "'"

      val numOfPartitionLevel = schemaMap.get(NUM_OF_PARTITION_LEVEL)
      // todo exit改为exception

      val createDataSourceSql = "select * from " + srcTbl + " where " + partitionPredicate
      // 视图等的名字避免相同,加上jobId
      var tempViewName = ("src-" + combineId).replace("-", "0")
      val descViewName = ("desc-" + combineId).replace("-", "0")
      val countSourceSql = s"select count(1) from ${tempViewName}"
      val showTblLikeMidTblSql = "show tables in " + dbName + " like '" + midTblName + "' "
      val createMidTblSql = "create table IF NOT EXISTS " + dbName + "." + midTblName + " like " + srcTbl
      val alterTblLocation = "alter table " + dbName + "." + midTblName + " set LOCATION  '" +midTblLocation + "' "
      val dropFirstPartitionLocation = "alter table " + dbName + "." + midTblName + " drop partition (" + firstPartition + ")"
      val showPartitionOfSrcTblSql = "show partitions " + srcTbl
      // val partitionsInInsertSql = getPartitionsInInsertSql(getPartitionSql(partitionStr, fieldOfStaticPartition), partitionPredicate)
      val showCreateSrcTblSql = s"show create table " + srcTbl
      val descFormattedSrcSql = "desc formatted " + srcTbl
      var insertSql = s"insert overwrite table " + dbName + "." + midTblName + " partition (${partitionSql}) " +
        s"select /*+ repartition(${defaultParallelism}) */ * from " + tempViewName
      val countCheckSql = s"select count(1) from " + dbName + "." + midTblName + " where " + partitionPredicate

      val resultMap = new java.util.HashMap[String, String]()

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to createTempView of source table...")
      val df = spark.sql(createDataSourceSql).drop(toDropPartitionField)
      df.createOrReplaceTempView(tempViewName)

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to count source table...")
      val countSrc: Long = try { spark.sql(countSourceSql).collect().apply(0).get(0).toString.toLong } catch {
        case e: org.apache.orc.FileFormatException =>
          // orc file damaged
          // todo 更新mysql
          MysqlSingleConn.init()
          MysqlSingleConn.updateStatus(jobType.mysqlStatus, ORC_DUMP_FAILED, mysqlId.toInt)
          MysqlSingleConn.close()
          throw e
        case e =>
          throw e
      }

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to alter table to set location of mid table...")
      // 将该表set location到backup，同时将该表的hdfs路径设置ec policy；如果已经存在，则check一下分区有没有数据

      try {
        spark.sql(showTblLikeMidTblSql).toDF().collect().apply(0)
      } catch {
        case ex: java.lang.ArrayIndexOutOfBoundsException =>
          // 表不存在，则创建，并设置hdfs_path的ec policy
          try {
            spark.sql(createMidTblSql)
          } catch {
            case ex: org.apache.spark.sql.catalyst.analysis.NoSuchTableException =>
              // 原始表不存在，则不需要做ec
              InnerLogger.error(InnerLogger.SPARK_MOD ,s"source table " +
                s"[${srcTbl}] may not exist!")
              throw ex
          }
      }
      // todo 这边后续改成一个spark-server时，不要每次都alter table级别的location,可以先check一下location是否和预期的Location相同，相同则不alter
      Try { spark.sql(alterTblLocation) } match {
        case Failure(exception) => {
          InnerLogger.error(InnerLogger.SPARK_MOD, s"execute ${alterTblLocation} failed!")
          throw exception
        }
        case _ =>
      }

      // drop partition location metadata, to avoid wrongFS when inserting data to mid-table,however,with count-check
      // succeeded by mistake,cus counting both records in bip05 and bip06
      try {
        InnerLogger.debug(InnerLogger.SPARK_MOD, s"start to drop partition of mid-table, ${dropFirstPartitionLocation}")
        spark.sql(dropFirstPartitionLocation)
      } catch {
        case e: Exception =>
          // such as ERROR [main] FileUtils: Failed to delete hdfs://...
      }

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to get insert sql...")
      val createTableStr =
        Try { spark.sql(showCreateSrcTblSql).collect.apply(0).get(0).toString } match {
          case Failure(exception) =>
            InnerLogger.error(InnerLogger.SPARK_MOD, s"execute [${showCreateSrcTblSql}] failed!")
            throw exception
          case Success(c) => c
        }

      var dynamicPartitionFields: Array[String] = null
      var partitionStr: String = ""
      var showPartitionsRows: Array[Row] = null
      val parSql = try {
        showPartitionsRows = spark.sql(showPartitionOfSrcTblSql).collect()
        partitionStr = showPartitionsRows.apply(0).get(0).toString
        getPartitionsInInsertSql(getPartitionSql(partitionStr, fieldOfStaticPartition), partitionPredicate)
      } catch {
        case ex: Exception =>
          InnerLogger.warn(InnerLogger.SPARK_MOD, s"execute [${showPartitionOfSrcTblSql}] or " +
            s"[getPartitionsInInsertSql(getPartitionSql(${partitionStr}, " +
            s"${fieldOfStaticPartition}), ${partitionPredicate})] failed, so continue to " +
            s"execute [${showCreateSrcTblSql}] instead!")
          try {
            dynamicPartitionFields = getDynamicPartitions(createTableStr, fieldOfStaticPartition)
            getPartitionsInInsertSql(dynamicPartitionFields, partitionPredicate)
          } catch {
            case ex: Exception =>
              InnerLogger.error(InnerLogger.SPARK_MOD, s"execute [getPartitionsInInsertSql(" +
                s"getDynamicPartitions(${createTableStr}, ${fieldOfStaticPartition}), ${partitionPredicate})] failed!")
              throw ex
          }
      }

      insertSql = insertSql.replace("${partitionSql}", parSql)
      // 这边是否需要set location? 不需要且不能: 多级分区时，这边无法alter，因为新创建的表还没有partition.

      // 获取分桶字段
      val descRows = spark.sql(descFormattedSrcSql).collect()
      val retRows = descRows.filter(r => "Bucket Columns".equalsIgnoreCase(r.getString(0)))
      var bucketColumns: Array[String] = null
      if (retRows.size != 0) {
        val bucketColumnsStr = retRows.apply(0).getString(1)
        bucketColumns = bucketColumnsStr.stripSuffix("]").stripPrefix("[")
          .replaceAll("`", "").split(",").map(_.trim)
      }

      // 是否是分桶表，或者是多级分区表
      var repartitionByBucketOrPartition = false
      // 是否都是静态分区，且不是分桶表
      var allStaticPartition = true

      val SPARK_SQL_FILES_MAXPARTITIONBYTES = spark.conf.getOption("spark.sql.files.maxPartitionBytes")
      val SPARK_SQL_SHUFFLE_PARTITIONS = spark.conf.getOption("spark.sql.shuffle.partitions")

      def resetConf(key: String, value: Option[String]): Unit = {
        if (value.isDefined) spark.conf.set(key, value.get)
        else spark.conf.unset(key)
      }

      Try(numOfPartitionLevel.toInt) match {
        case Success(value) =>
          if (value < 2) allStaticPartition = false
          repartitionByBucketOrPartition = true
        case _ =>
      }
      if (bucketColumns != null && bucketColumns.size > 0) {
        allStaticPartition = false
        repartitionByBucketOrPartition = true
        if (!enableHandleBucketTable) {
          InnerLogger.warn(InnerLogger.SPARK_MOD, s"this is a bucket table[${srcTbl}],skip inserting!")
          return
        }
      }
      var maxRecordsPerFile: Long = 0
      InnerLogger.debug(InnerLogger.SPARK_MOD, s"repartitionByBucketOrPartition: ${repartitionByBucketOrPartition}")

      var locationToStaticPartitionSql: Array[Tuple5[String, String, String, ArrayBuffer[String], String]] = null
      try {
        // 所有分区都是静态分区的场景下，针对最细粒度的分区进行合并
        val fineGrainedPartitionSqls = new ArrayBuffer[String]()
        assert(showPartitionsRows != null)
        val allStaticPartitionsRows: Array[Row] = showPartitionsRows.filter(row => {
          val partitionStr = row.get(0)
          if (partitionStr != null && partitionStr.toString.contains(firstPartition)) true
          else false
        })
        val staticLocations: Array[String] = allStaticPartitionsRows.map(_.get(0).toString)
        locationToStaticPartitionSql = staticLocations.map(location => {
          var fineGrainedPartitionSql = "alter table " + srcTbl + " partition(${par}) " +
            s"set location '${destTblLocation.stripSuffix("/")}"
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
        if (!fineGrainedPartitionSqls.isEmpty) {
          resultMap.put(PARTITION_SQL, fineGrainedPartitionSqls.mkString(SPLIT_DELIMITER))
          InnerLogger.debug(InnerLogger.SPARK_MOD, s"fineGrainedPartitionSqls:" +
            s"${fineGrainedPartitionSqls.mkString(SPLIT_DELIMITER)}")
        }
      } catch {
        case e: Exception =>
          InnerLogger.warn(InnerLogger.SPARK_MOD, "get locationToStaticPartitionSql failed!" +
            "\n" + e.getStackTrace.mkString("\n"))
      }

      spark.conf.set("spark.sql.shuffle.partitions", defaultParallelism)

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to insert data into mid table...")
      try {
        // defaultParallelism本身就是不大于initFileNums
        if (onlyCoalesce || initFileNums == defaultParallelism.toLong) {
          // 特定场景下可以改为coalesce，避免shuffle。
          insertSql = insertSql.replace("repartition", "coalesce")
          InnerLogger.info(InnerLogger.SPARK_MOD, s"start to execute insertion with coalesce: spark.sql(${insertSql})")
          spark.sql(insertSql)
        } else if (enableFineGrainedInsertion && allStaticPartition) {
          // 可以找到所有的静态分区，按照最细粒度合并
          // 动态分区仍然开启，通过动态分区的方式insert，但是overwrite模式改成动态的!
          // todo 考虑并发执行
          // todo 这个参数会影响同时并发跑的其他job！
          assert(locationToStaticPartitionSql != null)
          spark.conf.set("spark.sql.sources.partitionOverwriteMode", PartitionOverwriteMode.DYNAMIC.toString)

          locationToStaticPartitionSql.foreach(location2Sql => {
            val createDataSourceSql = "select * from " + srcTbl + " where " + location2Sql._2
            tempViewName = (tempViewName + "0" + location2Sql._1)
              .replace("/", "0").replace("=", "0")
            // drop constant partition value
            val df = spark.sql(createDataSourceSql).drop(location2Sql._4:_*)
            df.createOrReplaceTempView(tempViewName)
            // 设置maxSplitBytes
            // spark.conf.set("spark.sql.files.maxPartitionBytes", maxPartitionBytes)
            val fineGrainedLocation = sourceTblLocation.stripSuffix("/") + "/" + location2Sql._1
            if (s"hdfs dfs -test -e ${fineGrainedLocation}".! == 0) {
              InnerLogger.debug(InnerLogger.SPARK_MOD, s"start to run fineGrainedLocation[${fineGrainedLocation}]...")
              val totalSize = s"hdfs dfs -count ${fineGrainedLocation}".!!
                .split(" ").filter(!_.equals(""))(2).stripMargin
              var parallelism: Long = totalSize.toLong / maxPartitionBytes.toLong
              if (parallelism <= 0) parallelism = 1
              InnerLogger.debug(InnerLogger.SPARK_MOD, "get size of fineGrainedLocation: " +
                s"${fineGrainedLocation},totalSize:${totalSize},maxPartitionBytes:${maxPartitionBytes}," +
                s"parallelism:${parallelism}")
              if (parallelism > 0) {
                insertSql = s"insert overwrite table " + dbName + "." + midTblName +
                  s" partition (${location2Sql._3}) " +
                  s"select /*+ repartition(${parallelism}) */ * from " + tempViewName
                InnerLogger.debug(InnerLogger.SPARK_MOD, "start to execute insertion with static" +
                  s"partition: ${insertSql}")
                var res = true
                try {
                  spark.sql(insertSql)
                } catch {
                  case ex: Exception =>
                    val msg = if (ex.getCause == null) ex.getMessage + "\n" + ex.getStackTrace.mkString("\n")
                    else ex.getMessage + "\n" + ex.getStackTrace.mkString("\n") + "\n" + ex.getCause.toString
                    InnerLogger.error(InnerLogger.SPARK_MOD, s"insert sql[sql:${insertSql},fineGrainedLocation:" +
                      s"${fineGrainedLocation}] executed failed:\n${msg}")
                    res = false
                }
                if (res) InnerLogger.info(InnerLogger.SPARK_MOD, s"execute insertion [${insertSql}] successfully," +
                  s"location[${fineGrainedLocation}]")
              }
            } else {
              InnerLogger.warn(InnerLogger.SPARK_MOD, s"fineGrainedLocation[${fineGrainedLocation}] did not exist!")
            }
          })

          spark.conf.set("spark.sql.sources.partitionOverwriteMode", PartitionOverwriteMode.STATIC.toString)

        } else if (!repartitionByBucketOrPartition) {
          InnerLogger.info(InnerLogger.SPARK_MOD, s"start to execute insertion: spark.sql(${insertSql})")
          spark.sql(insertSql)
        } else {
          // 计算spark.sql.files.maxRecordsPerFile
          if (enableMaxRecordsPerFile && ((bucketColumns != null && bucketColumns.size >= 1) || numOfPartitionLevel.toInt >= 2)) {
            maxRecordsPerFile = countSrc / defaultParallelism.toLong
            spark.conf.set("spark.sql.files.maxRecordsPerFile", maxRecordsPerFile)
            InnerLogger.info(InnerLogger.SPARK_MOD, s"spark.sql.files.maxRecordsPerFile:${maxRecordsPerFile}")
          }

          val df = spark.sql(createDataSourceSql).drop(toDropPartitionField)
          if (dynamicPartitionFields == null && numOfPartitionLevel.toInt > 1) {
            val cts = spark.sql(showCreateSrcTblSql).collect.apply(0).get(0).toString
            dynamicPartitionFields = getDynamicPartitions(cts, fieldOfStaticPartition)
          }
          val repartitionColumns = new ArrayBuffer[String]()
          if (dynamicPartitionFields != null) repartitionColumns ++= dynamicPartitionFields
          if (bucketColumns != null) repartitionColumns ++= bucketColumns

          val columns = new ArrayBuffer[Column]()
          repartitionColumns.map(UnresolvedAttribute(_))
            .foreach(u => columns += Column(u))
          val tempView2 = "repartition_" + tempViewName

          // Sort
          // df.sort(columns.distinct:_*)

          // 重新写一个分区器，根据这些column取hash，同时限制每个分区的record数量。

          df.repartition(defaultParallelism.toInt, columns.distinct:_*)
            .createOrReplaceTempView(tempView2)
          insertSql = s"insert overwrite table " + dbName + "." + midTblName + " partition (${partitionSql}) " + s"select * from " + tempView2
          insertSql = insertSql.replace("${partitionSql}", parSql)
          InnerLogger.info(InnerLogger.SPARK_MOD, s"start to execute insertion: spark.sql(${insertSql})" +
            s" with view(${tempView2}) repartitioned by fields(${columns.toString()})," +
            s"${if (bucketColumns != null && bucketColumns.size > 0) "bucketColumns:" + bucketColumns.mkString(",")} " +
            s"${if (dynamicPartitionFields != null && dynamicPartitionFields.size > 0) "dynamicPartitionFields" + dynamicPartitionFields.mkString(",")}")
          spark.sql(insertSql)
        }
      } catch {
        case ex: Exception =>
          if (!repartitionByBucketOrPartition) InnerLogger.error(InnerLogger.SPARK_MOD, s"execute " +
            s"spark.sql(${insertSql}) failed!")
          throw ex
      } finally {
        repartitionByBucketOrPartition = false
        resetConf("spark.sql.files.maxRecordsPerFile", Some("0"))
        resetConf("spark.sql.files.maxPartitionBytes", SPARK_SQL_FILES_MAXPARTITIONBYTES)
        resetConf("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
      }

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to check count of source and mid table...")
      val countVal = try { spark.sql(countCheckSql).collect().apply(0).get(0).toString.toLong } catch {
        case ex: Exception => {
          InnerLogger.error(InnerLogger.SPARK_MOD, s"execute " +
            s"${spark.sql(countCheckSql).collect().apply(0).get(0).toString.toLong} failed!")
          throw ex
        }
      }
      // todo 这边需要更严格的check
      // todo 去掉判断走coalesce还是repartition
      if (countVal < 0) {
        InnerLogger.error(InnerLogger.SPARK_MOD, "total count of mid-location is less than 0!")
        throw new SparkException("total count of mid-location is less than 0!")
      }
      if (countSrc == 0) {
        // 直接查表查不到的数据，如未被元数据管理的数据，hdfs数据存在但是分区元数据不存在
        InnerLogger.error(InnerLogger.SPARK_MOD, "total count of mid-location is 0!")
        throw new SparkException("total count of mid-location is 0!")
      }
      // check data quality
      if (countSrc != countVal) {
        // check data quality failed!
        val message = s"check data quality failed!" +
          s" count of source table is ${countSrc}, but count of mid table is ${countVal}!"
        InnerLogger.error(InnerLogger.SPARK_MOD, message)
        throw new SparkException(message)
      }

      InnerLogger.debug(InnerLogger.SPARK_MOD, "start to get format of source table...")
      // fixme 如果分区format和表级别format不一致？
      val descDf = spark.sql(s"desc formatted " + srcTbl)
      descDf.createOrReplaceTempView(descViewName)
      val inputFormat = spark.sql(s"select data_type from ${descViewName} " +
        s"where col_name='${INPUT_FORMAT}'").collect().apply(0).get(0).toString
      val outputFormat = spark.sql(s"select data_type from ${descViewName} " +
        s"where col_name='${OUTPUT_FORMAT}'").collect().apply(0).get(0).toString

      resultMap.putAll(schemaMap)
      resultMap.put(INPUT_FORMAT, inputFormat)
      resultMap.put(OUTPUT_FORMAT, outputFormat)
      resultMap.put(APPLICATION_ID, applicationId)
      resultMap.put(COUNT_OF_INIT_LOC, countSrc.toString)
      resultMap.put(COUNT_OF_MID_LOC, countVal.toString)
      // 获取新的文件数量,并更新metric
      val combinedFileNums = s"hdfs dfs -count ${schemaMap.get(MID_DT_LOCATION)}".!!
        .split(" ").filter(!_.equals(""))(1).stripMargin
      resultMap.put(COMBINED_FILE_NUMS, combinedFileNums)

      val successPath = midDTLocation.stripSuffix("/") + "/" + SUCCESS_FILE_NAME
      val content = mapper.writeValueAsBytes(resultMap)
      // todo 如果已经写过了这个文件？
      // todo 如何确保只写一个文件？
      import java.io.{File, FileOutputStream}
      import sys.process._
      if (s"hdfs dfs -test -e ${successPath}".! == 0) {
        // success file已经存在
        if (s"hdfs dfs -rm ${successPath}".! == 0)
          InnerLogger.info(InnerLogger.SPARK_MOD, s"remove the old success file [${successPath}]!")
      }
      val parentPath = s"${tmpParentPath}/${combineId}/"
      val tmpFilePath = parentPath + SUCCESS_FILE_NAME
      if (s"mkdir -p ${parentPath}".! == 0) {
        val outputFile = new File(tmpFilePath)
        new FileOutputStream(outputFile).write(content)
        val res = s"hdfs dfs -put ${tmpFilePath} ${successPath}".!
        InnerLogger.info(InnerLogger.SPARK_MOD,
          s"write success-file to ${successPath} with end code : ${res}!")
      }
    }

    /** 批量并行，并阻塞直到该批次数据全部执行结束 */
    def runMultiCombineWorkConc(concSize: Int, values: Array[String]) = {
      val pool = new ThreadPoolExecutor(concSize, concSize, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])
      val futureList = new ArrayBuffer[Future[_]]()
      values.foreach(v => {
        val future = pool.submit(new Runnable {
          override def run(): Unit = runSingleCombineWork(v)
        })
        futureList += future
      })
      futureList.foreach(f => {
        try {
          f.get()
        } catch {
          case ex: Exception =>
            val msg = if (ex.getCause == null) ex.getMessage + "\n" + ex.getStackTrace.mkString("\n")
            else ex.getMessage + "\n" + ex.getStackTrace.mkString("\n") + "\n" + ex.getCause.toString
            InnerLogger.error(InnerLogger.SPARK_MOD, msg)
        }
      })

    }
    scheduleWorkArray(sparkConcurrency)
  }

  /** submit work of ec or file_combine to spark */
  def scheduleWork(): Unit = {
    /** step3: acquire resource dynamically | start *******************************************************************/
    currentStep = JobType.scheduleWork
    if (curJobs.size() == 0) {
      return
    }
    var resourceSeq = List[Tuple2[Int, Int]]()
    curJobs.values().forEach(jsonStr => {
      var acquireCores = defaultAcquireCores
      var acquireMem = defaultAcquireMem
      // todo jackson
      val map = parseRaw(jsonStr).get.asInstanceOf[JSONObject].obj
      val computedParallelism = map.get(COMPUTED_PARALLELISM).get.toString.toLong
      if (sparkDynamicAllocationMaxExecutors*4*10 < computedParallelism) {
        // 需要申请更多core
        var cores: Int = (computedParallelism / 10 / sparkDynamicAllocationMaxExecutors).toInt
        if (cores > defaultMaxCoresPerExecutor) cores = defaultMaxCoresPerExecutor
        acquireCores = cores
      }
      val maxPartitionBytes = map.get(MAX_PARTITION_BYTES).get.toString.toLong
      var mem: Int = (((maxPartitionBytes * 2 / sparkMemoryFraction) + 300) / 1024 /1024 /1024).toInt
      if (mem < 2) mem = 2
      acquireMem = mem * acquireCores
      if (acquireMem > defaultMaxMemPerExecutor) acquireMem = defaultMaxMemPerExecutor
      resourceSeq = (acquireCores, acquireMem) :: resourceSeq ::: Nil
    })
    // 所有作业中，按照单个作业的最高标准申请资源
    var maxCores = defaultAcquireCores
    var maxMem = defaultAcquireMem
    resourceSeq.foreach(t => {
      if (t._1 > maxCores) maxCores = t._1
      if (t._2 > maxMem) maxMem = t._2
    })
    InnerLogger.info(InnerLogger.SCHE_MOD, s"acquire resource dynamically: " +
      s"acquire-cores-per-executor:${maxCores},acquire-mem-per-executor:${maxMem}")
    /** step3: acquire resource dynamically | end *********************************************************************/

    /** step4: submit spark-application | start ***********************************************************************/
    val jobArray = JSONArray(curJobs.values().toArray().toList).toString()
    InnerLogger.debug(InnerLogger.SCHE_MOD, s"jobArray: ${jobArray}")
    // todo
    submitSparkApp(maxCores, maxMem, jobArray, testMode)
    /** step4: submit spark-application | end *************************************************************************/
  }

  /** check write of spark and change directory */
  def checkWork(): Unit = {
    /** step5: check spark-application | start ************************************************************************/
    currentStep = JobType.checkWork
    if (curJobs.size() == 0) {
      return
    }
    MysqlSingleConn.init()
    val pool = new ThreadPoolExecutor(batchSize, batchSize, 0L,
      TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable])
    val futureList = new ArrayBuffer[Future[_]]()
    val futureToJson = new java.util.HashMap[Future[_], String]()
    curJobs.values().forEach(jsonStr => {
      val future = pool.submit(new CheckSingleWork(jsonStr))
      futureList.+=(future)
      futureToJson.put(future, jsonStr)
    })
    futureList.foreach(future => {
      try { future.get() }
      catch {
        case ex: Exception => InnerLogger.error(InnerLogger.CHECK_MOD,
          ex.getMessage + "\n" + ex.getStackTrace.mkString("\n"))
          // todo del mid dt location metadata
          val jsonStr = futureToJson.get(future)
          val mapper = new ObjectMapper()
          val record: java.util.HashMap[String, String] = mapper.readValue(jsonStr, classOf[java.util.HashMap[String, String]])
          val midDtLocation = record.get(MID_DT_LOCATION)
          deleteFileIfExist(midDtLocation)
          InnerLogger.info(InnerLogger.CHECK_MOD, s"check failed! Delete midDtLocation[${midDtLocation}]!")
      }
    })
    MysqlSingleConn.close()
    InnerLogger.info(InnerLogger.CHECK_MOD, "all partitions checked finished!" +
      "\n\t")
    // todo 这边增加统计信息，多少成功，多少失败，失败code
    // todo 统计完了，写到hive表
    // metric收集
    val buffer = new StringBuffer()
    jobIdToJobStatus.values().forEach(map => {
      buffer.append(s"record[${map.toString}] check successfully!\n")
    })
    buffer.append(s"number of total succeeded records: ${jobIdToJobStatus.size()}\n")
    InnerLogger.info(InnerLogger.METRIC_COLLECT, buffer.toString)
    if (!shutdownSparkContextForcely) {
      spark.close()
    }
    /** step5: check spark-application | end **************************************************************************/
  }

  /** for test */
  def trigger2(): Unit = {
    // update bip_cloddata_other_need_ec_list_test set ec_status= 0 where id = 1111;
    val params = Seq("testId", "testSid", "bip_cloddata_other_need_ec_list_test", "root.basic_platform.critical", "0", "1").toArray
    initParams(params)
    val executor = new EcAndFileCombine
    executor.encapsulateWork()
    executor.scheduleWork()
  }

  def trigger(): Unit = {
    val executor = new EcAndFileCombine
    executor.encapsulateWork()
    executor.scheduleWork()
    System.exit(0)
  }

}

// /home/vipshop/platform/spark-3.0.1/jars/hadoop-common-3.2.0-vipshop-2.0.jar
// /home/vipshop/platform/spark-3.0.1/jars/hadoop-client-3.2.0-vipshop-2.0.jar
// /home/vipshop/platform/spark-3.0.1/jars/woodstox-core-5.0.3.jar
object InnerUtils {
  var configuration: Configuration = getHadoopConf

  /** 分布式dump orc file */
  def dumpOrcFileWithSpark(spark: SparkSession, parentPath: String,
                           parallelism: Int = 500, dumpWithCommand: Boolean = true): Boolean = {
    val path = new Path(parentPath)
    val fileInPath = new ArrayBuffer[Path]()
    getAllFilesInPath(path, configuration, fileInPath)
    println("fileInPath" + fileInPath.mkString(","))
    val allFiles = fileInPath.map(_.getName)
    val rdd = spark.sparkContext.makeRDD(allFiles, parallelism)
    val broadcastedHadoopConf =
      spark.sparkContext.broadcast(new SerializableConfiguration(configuration))
    val dumpRetRdd = rdd.mapPartitions(iter => {
      val corruptFiles = new java.util.ArrayList[String]()
      // 接受广播变量configuration
      val conf = broadcastedHadoopConf.value.value
      iter.foreach(pathStr => {
        // todo dump orc file
        // if (s"hive --orcfiledump ${path}".! != 0) dumpRet = false

        val path = new Path(pathStr)
        val fs = path.getFileSystem(conf)
        val dataFileLen = fs.getFileStatus(path).getLen()
        val sideFile = OrcAcidUtils.getSideFile(path)
        val sideFileExists = fs.exists(sideFile)
        var openDataFile = false
        var openSideFile = false
        var reader: DistributedFileSystem = null

        if (fs.isInstanceOf[DistributedFileSystem]) {
          reader = fs.asInstanceOf[DistributedFileSystem]
          openDataFile = !reader.isFileClosed(path)
          openSideFile = sideFileExists && !reader.isFileClosed(sideFile)
        }

        if (!openDataFile && !openSideFile) {
          // reader = null
          var reader: Reader = null
          if (sideFileExists) {
            val maxLen = OrcAcidUtils.getLastFlushLength(fs, path)
            val sideFileLen = fs.getFileStatus(sideFile).getLen()
            // System.err.println("Found flush length file " + sideFile + " [length: " + sideFileLen + ", maxFooterOffset: " + maxLen + "]");
            if (maxLen == -1L) {
              if (dataFileLen > maxLen) {
                // System.err.println("Data file has more data than max footer offset:" + maxLen + ". Adding data file to recovery list.");
                if (corruptFiles != null) {
                  corruptFiles.add(path.toUri().toString())
                }
              }

              // return null
            }

            try {
              reader = OrcFile.createReader(path, OrcFile.readerOptions(conf).maxLength(maxLen))
              if (dataFileLen > maxLen) {
                // System.err.println("Data file has more data than max footer offset:" + maxLen + ". Adding data file to recovery list.");
                if (corruptFiles != null) {
                  corruptFiles.add(path.toUri().toString())
                }
              }
            } catch {
              case e: Exception =>
                if (corruptFiles != null) {
                  corruptFiles.add(path.toUri().toString())
                }
                // System.err.println("Unable to read data from max footer offset. Adding data file to recovery list.");
                // return null
            }
          } else {
            reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))
          }

          // return reader

        } else {
          if (openDataFile && openSideFile) {
            System.err.println("Unable to perform file dump as " + path + " and " + sideFile + " are still open for writes.");
          } else if (openSideFile) {
            System.err.println("Unable to perform file dump as " + sideFile + " is still open for writes.");
          } else {
            System.err.println("Unable to perform file dump as " + path + " is still open for writes.");
          }
          // return null
        }


      })
      Seq(corruptFiles.size() == 0).toIterator
    })
    val booleans: Array[Boolean] = dumpRetRdd.collect()
    !booleans.contains(false)
  }

  def getAllFilesInPath(parentPath: Path, configuration: Configuration, buffer: ArrayBuffer[Path]): Unit = {
    val fileSystem = parentPath.getFileSystem(configuration)
    val fileStatus = fileSystem.getFileStatus(parentPath)
    if (fileStatus.isDirectory) {
      val fileStatuses = fileSystem.listStatus(parentPath)
      fileStatuses.foreach(fileStatus => {
        if (fileStatus.isDirectory) {
          getAllFilesInPath(fileStatus.getPath, configuration, buffer)
        } else if (fileStatus.isFile) {
          buffer += fileStatus.getPath
        }
      })
    } else if (fileStatus.isFile) {
      buffer += fileStatus.getPath
    }
  }

  def getHadoopConf(): Configuration = {
    var dir = System.getenv(hadoopConfDir)
    if (dir == null) {
      dir = defaultHadoopConfDir
      InnerLogger.debug("InnerUtils", s"use default hadoop conf dir:${defaultHadoopConfDir}")
    } else {
      InnerLogger.debug("InnerUtils", s"use hadoop conf dir from sys env:${dir}")
    }
    dir = dir.stripSuffix("/") + "/"
    val conf = new Configuration()
    conf.addResource(new Path(dir + "hdfs-site.xml"))
    conf
  }

  /** for test */
  def getHadoopConfInShell(): Configuration = {
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}
    import java.io.{BufferedInputStream, File, FileInputStream}
    val dir = "/home/vipshop/conf/"
    val conf = new Configuration()
    val inputStream = new BufferedInputStream(new FileInputStream(new File(dir + "hdfs-site.xml")))
//    conf.addResource(inputStream)
    conf.addResource(dir + "hdfs-site.xml")
    conf
  }
}



