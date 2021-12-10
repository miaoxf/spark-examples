package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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
                    try {
                        // InnerLogger.debug(InnerLogger.CHECK_MOD, s"start check file : ${pathStr} ")
                        val reader: Reader = OrcFile.createReader(path, OrcFile.readerOptions(hdfsConf))
                        val records: RecordReader = reader.rows()
                        records.close()
                        reader.close()
                        // InnerLogger.debug(InnerLogger.CHECK_MOD, s"end check file : ${pathStr} is ok!")
                    } catch {
                        case ex: Exception => {
                            fileCorruptList += pathStr
                            InnerLogger.error(InnerLogger.CHECK_MOD, s"end check file : ${pathStr} is corrupted!")
                        }
                    }
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

        val checkCluster = args(0)
        val checkSize = if (args.size > 1) args(1).toInt else 2

        MysqlSingleConn.init()
        val rs = MysqlSingleConn.executeQuery(s"select id,location,file_count from " +
          s"${checkCluster} where ec_status = 2 and clean_status = 0 limit ${checkSize}")

        var corFileList = new Array[String](0)
        var parPath = ""
        var id: Int = -1
        var fileCount: Int = 1
        var paral: Int = 1

        if (rs.next()) {
            id = rs.getInt(1)
            parPath = rs.getString(2)
            fileCount = rs.getInt(3)
            paral = fileCount/100 + 1

            // update mysql 检测状态 clean_status=1
//            updateStatus(checkCluster,)
            corFileList = dumpOrcFileWithSpark(spark, parPath, paral,configuration)
        }

        if (corFileList.length == 0) {
            // update mysql 成功状态 clean_status=2

            InnerLogger.info(InnerLogger.SCHE_MOD, s"${parPath} all file is correct !!!")
        } else {
            // update mysql 失败状态 clean_status=7
            // MysqlSingleConn.updateQuery(failedSql)
            corFileList.toIterator.foreach(path => {
                InnerLogger.error(InnerLogger.SCHE_MOD, s"file: ${path} is orc corrupted !!!")
            })
        }

        MysqlSingleConn.close()
    }
}
