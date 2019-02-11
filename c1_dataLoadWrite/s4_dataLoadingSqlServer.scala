package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

// import 문 생략
object s4_dataLoadingSqlServer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 접속정보 설정 (1)
    var staticUrl = "jdbc:sqlserver://127.0.0.1:1433;databaseName=kopo"

    var staticUser = "haiteam"
    var staticPw = "kopo1234!"
    var selloutDb = "kopo_product_volume"

    // 관계형 데이터베이스 SqlServer 연결 (2)
    val dataFromSqlServer= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    // 데이터 확인 (3)
    println(dataFromSqlServer.show(5))
  }
}
