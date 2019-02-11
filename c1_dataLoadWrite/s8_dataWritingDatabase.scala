package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s8_dataWritingDatabase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 접속정보 설정 (1)
    var staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

    // 관계형 데이터베이스 Oracle 연결 (2)
    val dataFromOracle= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    // 데이터 확인 (3)
    println(dataFromOracle.show(5))

    // 데이터베이스 주소 및 접속정보 설정 (1)
    var outputUrl = "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 저장 (2)
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    val table = "kopo_product_volume_out2"
    //append
    dataFromOracle.write.mode("overwrite").jdbc(outputUrl, table, prop)

  }
}
