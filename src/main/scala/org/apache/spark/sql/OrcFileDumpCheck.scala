package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.{OrcFile, Reader, RecordReader}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object OrcFileDumpCheck {

    var configuration: Configuration = getHadoopConf

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
                             parallelism: Int = 500): Array[String] = {
        val path = new Path(parentPath)
        val fileInPath = new ArrayBuffer[Path]()

        getAllFilesInPath(path, configuration, fileInPath)
        val allFiles = fileInPath.map(_.toString)
        val rdd = spark.sparkContext.makeRDD(allFiles, parallelism)
        val broadcastedHadoopConf = spark.sparkContext.broadcast(new SerializableConfiguration(configuration))

        val dumpRetRdd = rdd.mapPartitions(iter => {
            val fileCorruptList = new ArrayBuffer[String]()
            iter.foreach(pathStr => {
                val hdfsConf = broadcastedHadoopConf.value.value
                // 通过初始化RecordReader检测orc文件是否损坏
                val path = new Path(pathStr)
                try {
                    InnerLogger.info(InnerLogger.CHECK_MOD, s"start check file : ${pathStr} ")
                    val reader: Reader = OrcFile.createReader(path, OrcFile.readerOptions(hdfsConf))
                    val records: RecordReader = reader.rows()
                    InnerLogger.info(InnerLogger.CHECK_MOD, s"end check file : ${pathStr} is ok!")
                } catch {
                    case ex: Exception => {
                        fileCorruptList += pathStr
                        InnerLogger.error(InnerLogger.CHECK_MOD, s"end check file : ${pathStr} is corrupted!")
                    }
                }
            })
            fileCorruptList.toIterator
        })

        dumpRetRdd.collect()
    }

    def getHadoopConf(): Configuration = {
        val conf = new Configuration()
        conf.addResource("hdfs-site.xml")
        conf.addResource("core-site.xml")
        conf
    }

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()

        conf.set("spark.master", "local[1]")
          .setAppName("OrcFileDumpTest")


        val builder = SparkSession.builder().config(conf)
        var spark: SparkSession = null
        spark = builder.getOrCreate()
        InnerLogger.info(InnerLogger.SCHE_MOD, "Created Spark session")

        val parPath = "hdfs://bipnormal/user/muskluo"

        val corFileList = dumpOrcFileWithSpark(spark, parPath, 5)
        if (corFileList.length == 0) {
            InnerLogger.info(InnerLogger.SCHE_MOD, s"${parPath} all file is correct")
        } else {
            corFileList.toIterator.foreach(path => {
                InnerLogger.error(InnerLogger.SCHE_MOD, s"file: ${path} is orc corrupted")
            })
        }
    }
}
