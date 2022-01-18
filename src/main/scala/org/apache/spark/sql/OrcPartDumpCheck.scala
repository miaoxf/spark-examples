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

object OrcPartDumpCheck {

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

    def updateInitStatus(checkCluster: String,id: Long,status: Int,action_id: Long,action_sid: Long): Int = {

        val statement = getConnection.createStatement()
        val sql =
            s"""
               |update ${checkCluster} set check_status=${status},
               |action_id=${action_id},action_sid=${action_sid}
               |where id=${id}
               |""".stripMargin

        InnerLogger.debug("update-init-mysql", s"execute sql ${sql}")
        statement.executeUpdate(sql)
    }

    def updateLastStatus(checkCluster: String,id: Long,status: Int,corruptedcount: Int): Int = {

        val statement = getConnection.createStatement()
        val sql =
            s"""
               |update ${checkCluster} set check_status=${status},corrupted_count=${corruptedcount}
               |where id=${id}
               |""".stripMargin

        InnerLogger.debug("update-last-mysql", s"execute sql ${sql}")
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

        val action_id = args(0).toLong
        val action_sid = args(1).toLong
        val checkCluster = args(2)
        val paral = if (args.size > 3) args(3).toInt else 500
        var location = ""
        var id = 0
        var corFileList = new Array[String](0)

        val configuration: Configuration = getHadoopConfInShell
        val conf = new SparkConf()
        conf.set("spark.master", "yarn")
          .set("spark.submit.deployMode", "client")
          .setAppName("OrcFileDumpCheck")


        val builder = SparkSession.builder().config(conf)
        var spark: SparkSession = null
        spark = builder.getOrCreate()
        InnerLogger.info(InnerLogger.SCHE_MOD, "Created Spark session")

        val getDataSourceSql =
            s"""
               |select id,db_name,tbl_name,location from ${checkCluster}
               |    where check_status=0 order by priority desc limit 1
               |""".stripMargin

        InnerLogger.debug(InnerLogger.ENCAP_MOD, s"sql to get datasource: ${getDataSourceSql}")
        MysqlSingleConn.init()
        val rs = MysqlSingleConn.executeQuery(getDataSourceSql)
        while (rs.next()) {
            id = rs.getInt("id")
            location = rs.getString("location")
        }
        updateInitStatus(checkCluster, id, 1, action_id, action_sid)

        corFileList = dumpOrcFileWithSpark(spark, location, paral, configuration)

        if (corFileList.length == 0) {
            // update mysql 成功状态 check_status=2
            updateLastStatus(checkCluster, id, 2, 0)
            InnerLogger.info(InnerLogger.SCHE_MOD, s"${location} all file is correct !!!")
        } else {
            // update mysql 失败状态 check_status=5
            updateLastStatus(checkCluster, id, 5, corFileList.length)
            corFileList.toIterator.foreach(path => {
                InnerLogger.error(InnerLogger.SCHE_MOD, s"file: ${path} is orc corrupted !!!")
            })
        }

        MysqlSingleConn.close()

    }
}
