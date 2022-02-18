#!/bin/bash

local_dir="/tmp/tpc_ds_spark/xuefei"
action=$1
spark_home=$2
spark_conf=$3
if [[ "$action" == "quality" || "$action" == "performance" ]];then
  test_data_db_name=$4
  test_data_db_location=$5
  test_data_result_location=$6
elif [[ "$action" == "parse" ]]; then
  standard_location=$4
  test_location=$5
elif [[ "$action" == "gen" ]]; then
  # gen data
  echo
else
  echo "参数一必须是:[quality,performance,gen,parse]!"
fi


rm -r $local_dir
mkdir -p $local_dir
hdfs dfs -get hdfs://bipcluster/user/hdfs/xuefei/tpc_ds.jar $local_dir/tpc_ds.jar
export SPARK_HOME=/home/vipshop/platform/$spark_home
export SPARK_CONF_DIR=/home/vipshop/conf/$spark_conf


run_test_quality(){
  export db_name=$1
  export db_location=$2
  export result_location=$3
  export quality_or_perf=$4

  $SPARK_HOME/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue root.basic_platform.critical \
  --driver-memory 4G \
  --executor-memory 16G \
  --executor-cores 4 \
  --jars $local_dir/tpc_ds.jar << 'EOF'
    import com.databricks.spark.sql.perf.{Benchmark, ExperimentRun}
    import org.apache.spark.sql.{Row, SparkSession}
    import org.apache.spark.sql.functions.{col, substring}
    import org.codehaus.jackson.JsonNode
    import org.codehaus.jackson.map.ObjectMapper
    import java.util

    val mapper = new ObjectMapper()

    def parseCrcSum(experimentRunNode: JsonNode, name2Result: util.HashMap[String, String]) = {
      val resultsNode = experimentRunNode.get("results")
      if (resultsNode.isArray) {
        resultsNode.forEach(result => {
          val nameNode = result.get("name")
  //        val tablesNode = result.get("tables")
          val resultNode = result.get("result")
          name2Result.put(nameNode.toString, resultNode.toString)
        })
      } else {
        println("resultsNode is not an array node, failed!")
      }
    }


    def runTest(spark: SparkSession, databaseName: String,
                resultLocation: String, dataQuality: Boolean = false) = {
      import com.databricks.spark.sql.perf.tpcds.TPCDS
      val tpcds = new TPCDS (sqlContext = spark.sqlContext)
      val iterations = 1 // how many iterations of queries to run.
      val queries =
        if (dataQuality) {
          println("start check dataQuality!")
          tpcds.tpcds2_4Queries.map(q => q.checkResult) // queries to run.
        } else tpcds.tpcds2_4Queries
      val timeout = 24*60*60 // timeout, in seconds.
      // Run:
      spark.sql(s"use $databaseName")
      val experiment = tpcds.runExperiment(
        queries,
        iterations = iterations,
        resultLocation = resultLocation,
        forkThread = true)
      experiment.waitForFinish(timeout)
      experiment
    }

    def parseResult(spark: SparkSession,
                    resultLocation: String) = {
      // Get all experiments results.
      import org.apache.spark.sql.types._
      val schema = StructType(Array(
        StructField("configuration", StringType),
        StructField("iteration", IntegerType),
        StructField("results", ArrayType(StructType(Array(
          StructField("name", StringType),
          StructField("mode", StringType),
          StructField("parameters", MapType(StringType, StringType)),
          StructField("joinTypes", ArrayType(StringType)),
          StructField("tables", ArrayType(StringType)),
          StructField("parsingTime", DoubleType),
          StructField("analysisTime", DoubleType),
          StructField("optimizationTime", DoubleType),
          StructField("planningTime", DoubleType),
          StructField("executionTime", DoubleType),
          StructField("result", LongType),
          StructField("breakDown", ArrayType(StructType(
            Array(
              StructField("nodeName", StringType),
              StructField("nodeNameWithArgs", StringType),
              StructField("index", IntegerType),
              StructField("children", ArrayType(IntegerType)),
              StructField("executionTime", DoubleType),
              StructField("delta", DoubleType)
            )
          ))),
          StructField("queryExecution", StringType),
          StructField("failure", StructType(Array(StructField("className", StringType), StructField("message", StringType)))),
          StructField("mlResult", ArrayType(StructType(Array(
            StructField("metricName", StringType),
            StructField("metricValue", DoubleType),
            StructField("isLargerBetter", BooleanType)
          )))),
          StructField("benchmarkId", StringType)
        )))),
        StructField("tags", StringType),
        StructField("timestamp", LongType)
      ))

      val resultTable = spark.read.schema(schema).json(resultLocation)
      resultTable.createOrReplaceTempView("sqlPerformance")
      val result = spark.sql("select results from sqlPerformance ").collectAsList()
      val firstRow = result.get(0)

      val experimentRunNode = mapper.readTree(firstRow.prettyJson)
      // todo 保存结果，且默认不展示执行计划
      experimentRunNode
    }

    def compareTwoResult(standardLoc: String, testLoc: String): Unit = {
      val name2ResultOfStd = new util.HashMap[String, String]()
      val name2ResultOfTest = new util.HashMap[String, String]()

      val experimentRun = parseResult(SparkSession.getActiveSession.get, standardLoc)
      parseCrcSum(experimentRun, name2ResultOfStd)

      val experimentRun2 = parseResult(SparkSession.getActiveSession.get, testLoc)
      parseCrcSum(experimentRun2, name2ResultOfTest)

      assert(!name2ResultOfStd.isEmpty)
      assert(name2ResultOfStd.size() == name2ResultOfTest.size(),
        "size of resultMap of standard and test are not equal!")

      name2ResultOfStd.forEach((k, v) => {
        assert(name2ResultOfTest.containsKey(k))
        if (v == null || v.equalsIgnoreCase("null"))
          assert(name2ResultOfTest.get(k) == null || name2ResultOfTest.get(k).equalsIgnoreCase("null"))
        else {
          if (name2ResultOfTest.get(k).toLong != v.toLong) {
            println(s"[ERROR] key: ${k}, value: [std: $v, test: ${name2ResultOfTest.get(k)}]")
          }
        }
      })
    }

    val dbName = sys.env("db_name").toString
    val dbLocation = sys.env("db_location").toString
    val resultLocation = sys.env("result_location").toString
    val king = sys.env("quality_or_perf").toString
    import com.databricks.spark.sql.perf.tpcds.GenTPCDSData

    if (king.equalsIgnoreCase("quality")) {
      val experiment = runTest(SparkSession.getActiveSession.get, dbName, resultLocation, true)
      println("[INFO] result path of quality test: ", experiment.resultPath.stripSuffix("/") + "/*")
      val experimentRun = parseResult(SparkSession.getActiveSession.get, experiment.resultPath.stripSuffix("/") + "/*")
      parseCrcSum(experimentRun, name2Result)
      println("[INFO] queryToResult:")
      println(name2Result)
    } else {
      val experiment = runTest(SparkSession.getActiveSession.get, dbName, resultLocation, false)
      println("[INFO] result path of performance test: ", experiment.resultPath.stripSuffix("/") + "/*")
    }

EOF
}

compare_data_quality(){
  export standard_loc=$1
  export test_loc=$2
  $SPARK_HOME/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue root.basic_platform.critical \
  --driver-memory 4G \
  --executor-memory 12G \
  --executor-cores 4 \
  --jars $local_dir/tpc_ds.jar << 'EOF'

    import com.databricks.spark.sql.perf.{Benchmark, ExperimentRun}
    import org.apache.spark.sql.{Row, SparkSession}
    import org.apache.spark.sql.functions.{col, substring}
    import org.codehaus.jackson.JsonNode
    import org.codehaus.jackson.map.ObjectMapper
    import java.util

    val mapper = new ObjectMapper()

    def parseCrcSum(experimentRunNode: JsonNode, name2Result: util.HashMap[String, String]) = {
      val resultsNode = experimentRunNode.get("results")
      if (resultsNode.isArray) {
        resultsNode.forEach(result => {
          val nameNode = result.get("name")
  //        val tablesNode = result.get("tables")
          val resultNode = result.get("result")
          name2Result.put(nameNode.toString, resultNode.toString)
        })
      } else {
        println("resultsNode is not an array node, failed!")
      }
    }

    def parseResult(spark: SparkSession,
                    resultLocation: String) = {
      // Get all experiments results.
      import org.apache.spark.sql.types._
      val schema = StructType(Array(
        StructField("configuration", StringType),
        StructField("iteration", IntegerType),
        StructField("results", ArrayType(StructType(Array(
          StructField("name", StringType),
          StructField("mode", StringType),
          StructField("parameters", MapType(StringType, StringType)),
          StructField("joinTypes", ArrayType(StringType)),
          StructField("tables", ArrayType(StringType)),
          StructField("parsingTime", DoubleType),
          StructField("analysisTime", DoubleType),
          StructField("optimizationTime", DoubleType),
          StructField("planningTime", DoubleType),
          StructField("executionTime", DoubleType),
          StructField("result", LongType),
          StructField("breakDown", ArrayType(StructType(
            Array(
              StructField("nodeName", StringType),
              StructField("nodeNameWithArgs", StringType),
              StructField("index", IntegerType),
              StructField("children", ArrayType(IntegerType)),
              StructField("executionTime", DoubleType),
              StructField("delta", DoubleType)
            )
          ))),
          StructField("queryExecution", StringType),
          StructField("failure", StructType(Array(StructField("className", StringType), StructField("message", StringType)))),
          StructField("mlResult", ArrayType(StructType(Array(
            StructField("metricName", StringType),
            StructField("metricValue", DoubleType),
            StructField("isLargerBetter", BooleanType)
          )))),
          StructField("benchmarkId", StringType)
        )))),
        StructField("tags", StringType),
        StructField("timestamp", LongType)
      ))

      val resultTable = spark.read.schema(schema).json(resultLocation)
      resultTable.createOrReplaceTempView("sqlPerformance")
      val result = spark.sql("select results from sqlPerformance ").collectAsList()
      val firstRow = result.get(0)

      val experimentRunNode = mapper.readTree(firstRow.prettyJson)
      // todo 保存结果，且默认不展示执行计划
      experimentRunNode
    }

    def compareTwoResult(standardLoc: String, testLoc: String): Unit = {
      val name2ResultOfStd = new util.HashMap[String, String]()
      val name2ResultOfTest = new util.HashMap[String, String]()

      val experimentRun = parseResult(SparkSession.getActiveSession.get, standardLoc)
      parseCrcSum(experimentRun, name2ResultOfStd)

      val experimentRun2 = parseResult(SparkSession.getActiveSession.get, testLoc)
      parseCrcSum(experimentRun2, name2ResultOfTest)

      assert(!name2ResultOfStd.isEmpty)
      assert(name2ResultOfStd.size() == name2ResultOfTest.size(),
        "size of resultMap of standard and test are not equal!")

      var flag = true
      name2ResultOfStd.forEach((k, v) => {
        try {
          assert(name2ResultOfTest.containsKey(k))
          if (v == null || v.equalsIgnoreCase("null"))
            assert(name2ResultOfTest.get(k) == null || name2ResultOfTest.get(k).equalsIgnoreCase("null"))
          else {
            if (name2ResultOfTest.get(k).toLong != v.toLong) {
              flag = false
              println(s"[ERROR] key: ${k}, value: [std: $v, test: ${name2ResultOfTest.get(k)}]")
            }
          }
        } catch {
          case e: java.lang.Exception =>
            flag = false
        }
      })
      if (!flag) {
        System.exit(1)
      }
    }

    val standardLoc = sys.env("standard_loc").toString
    val testLoc = sys.env("test_loc").toString
    println("[INFO] 开始对比数据质量，提示：q68-v2.4和q39a-v2.4查询结果不是确定性的，需要额外对比一下count值。")
    compareTwoResult(standardLoc, testLoc)
EOF
}

extCheckCount(){
  $SPARK_HOME/bin/spark-shell \
  --master yarn \
  --deploy-mode client \
  --queue root.basic_platform.critical \
  --driver-memory 4G \
  --executor-memory 12G \
  --executor-cores 4 \
  --jars $local_dir/tpc_ds.jar << 'EOF'

    import com.databricks.spark.sql.perf.{Benchmark, ExperimentRun}
    import org.apache.spark.sql.{Row, SparkSession}
    import org.apache.spark.sql.functions.{col, substring}
    import org.codehaus.jackson.JsonNode
    import org.codehaus.jackson.map.ObjectMapper
    import java.util
    def extCheckCount(spark: SparkSession): Unit = {
      import com.databricks.spark.sql.perf.tpcds.TPCDS
      val tpcds = new TPCDS (sqlContext = spark.sqlContext)
      val queries = tpcds.tpcds2_4Queries
        .filter(q => {
          q.name.equalsIgnoreCase("q68-v2.4") ||
            q.name.equalsIgnoreCase("q39a-v2.4")
        })
      queries.map(query => {
        val c = spark.sql(query.sqlText.get).count
        println(s"[Info] query [${query.name}] count: $c")
      })
    }
    // count 68和39a
    extCheckCount(SparkSession.getActiveSession.get)
EOF
}

count_check(){
  export SPARK_HOME=/home/vipshop/platform/$spark_home
  echo "[INFO] current spark_home: /home/vipshop/platform/$spark_home"
  extCheckCount
  export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
  echo
  echo "[INFO] current spark_home: /home/vipshop/platform/spark-3.0.1"
  extCheckCount
}

if [[ "$action" == "quality" ]];then
  # test data quality
#  echo "[Info] start to run tpc-ds data quality test of /home/vipshop/platform/spark-3.0.1."
#  export SPARK_HOME=/home/vipshop/platform/spark-3.0.1
#  run_test_quality $test_data_db_name $test_data_db_location $test_data_result_location "quality"
#  # todo 同时跑标准的和测试的数据,并发
  export SPARK_HOME=/home/vipshop/platform/$spark_home
  echo "[Info] start to run tpc-ds data quality test of /home/vipshop/platform/$spark_home."
  run_test_quality $test_data_db_name $test_data_db_location $test_data_result_location "quality"
elif [[ "$action" == "performance" ]]; then
  # test performance
  echo "[Info] start to run tpc-ds performance test."
  run_test_quality $test_data_db_name $test_data_db_location $test_data_result_location "performance"
elif [[ "$action" == "gen" ]]; then
  # gen data
  echo "[Info] start to gen data."
elif [[ "$action" == "parse" ]]; then
  # result parsing
  echo "start to compare parsing result"
  compare_data_quality $standard_location $test_location
  echo "start to check external count of q68-v2.4 and q39a-v2.4"
  count_check
else
  echo "参数一必须是:[quality,performance,gen,parse]!"
fi
