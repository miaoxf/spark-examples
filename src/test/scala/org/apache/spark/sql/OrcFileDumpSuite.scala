package org.apache.spark.sql

import org.apache.spark.sql.EcAndFileCombine.hadoopConfDir
import org.scalatest.funsuite.AnyFunSuite

class OrcFileDumpSuite extends AnyFunSuite {
  test("orc dump") {
    System.getenv(hadoopConfDir)
  }
}
