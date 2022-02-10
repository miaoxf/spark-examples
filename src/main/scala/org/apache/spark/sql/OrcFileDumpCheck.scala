package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch
import org.apache.orc.{OrcFile, Reader, RecordReader}
import org.apache.spark.sql.MysqlSingleConn.getConnection
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedInputStream, File, FileInputStream}
import scala.collection.mutable.ArrayBuffer

object OrcFileDumpCheck {

    // 获取子分区下的所有文件
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

    // 校验单分区下的所有文件
    def dumpOrcFileWithSpark(spark: SparkSession, parentPath: String,
                             parallelism: Int = 500, configuration: Configuration): Array[String] = {
        val path = new Path(parentPath)
        val fileInPath = new ArrayBuffer[Path]()

        getAllFilesInPath(path, configuration, fileInPath)
        val allFiles = fileInPath.map(_.toString)

        val rdd = spark.sparkContext.makeRDD(allFiles, parallelism)
        val broadcastedHadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(configuration))

        val dumpRetRdd = rdd.mapPartitions(iter => {
            val fileCorruptList = new ArrayBuffer[String]()

            iter.foreach(pathStr => {
                if (!pathStr.contains(".COMBINE_SUCCESS")) {
                    val hdfsConf = broadcastedHadoopConf.value.value
                    // 通过初始化RecordReader检测orc文件是否损坏
                    val path = new Path(pathStr)

                    // InnerLogger.debug(InnerLogger.CHECK_MOD, s"start check file : ${pathStr} ")
                    var batchCount = 0
                    var rowCount = 0
                    val reader: Reader = OrcFile.createReader(path, OrcFile.readerOptions(hdfsConf))
                    val records: RecordReader = reader.rows()
                    val batch: VectorizedRowBatch = reader.getSchema().createRowBatch()
                    while (records.nextBatch(batch)) {
                        batchCount += 1
                        rowCount += batch.size
                    }
                    records.close()
                    reader.close()
                    // InnerLogger.debug(InnerLogger.CHECK_MOD, s"batchCount : ${pathStr} , rowCount : ${rowCount}")
                    // InnerLogger.debug(InnerLogger.CHECK_MOD, s"end check file : ${pathStr} is ok!")
                }
            })
            fileCorruptList.toIterator
        })

        dumpRetRdd.collect()
    }

    def updateStatus(checkCluster: String,status: String, value: Any, recordId: Int): Int = {

        val statement = getConnection.createStatement()
        val sql =
            s"""
               |update ${checkCluster} set ${status} = ${value.toString}
               |  where id = ${recordId}
               |""".stripMargin

        InnerLogger.debug("update-mysql", s"execute sql ${sql}")
        statement.executeUpdate(sql)
    }

    /** for test */
    def getHadoopConf(): Configuration = {
        val conf = new Configuration()
        conf.addResource("hdfs-site.xml")
        conf.addResource("core-site.xml")
        conf
    }

    /** for online */
    def getHadoopConfInShell(): Configuration = {
        val dir = "/home/vipshop/conf/"
        val conf = new Configuration()
        conf.addResource(dir + "hdfs-site.xml")
        conf.addResource(dir + "core-site.xml")
        conf
    }

    def main(args: Array[String]): Unit = {

        var configuration: Configuration = getHadoopConfInShell

        val conf = new SparkConf()

        //        conf.set("spark.master", "local[1]")
        //          .setAppName("OrcFileDumpTest")
        conf.set("spark.master", "yarn")
          .set("spark.submit.deployMode", "client")
          .setAppName("OrcFileDumpCheck")


        val builder = SparkSession.builder().config(conf)
        var spark: SparkSession = null
        spark = builder.getOrCreate()
        InnerLogger.info(InnerLogger.SCHE_MOD, "Created Spark session")

        val parPath = args(0)
        val paral = if (args.size > 1) args(1).toInt else 100

        var corFileList = new Array[String](0)

        corFileList = dumpOrcFileWithSpark(spark, parPath, paral,configuration)


        if (corFileList.length == 0) {
            InnerLogger.info(InnerLogger.SCHE_MOD, s"${parPath} all file is correct !!!")
        } else {

            corFileList.toIterator.foreach(path => {
                InnerLogger.error(InnerLogger.SCHE_MOD, s"file: ${path} is orc corrupted !!!")
            })

        }

    }
}
