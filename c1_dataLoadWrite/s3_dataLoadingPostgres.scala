package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

// import 문 생략
object s3_dataLoadingPostgres {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 접속정보 설정 (1)
    var staticUrl = "jdbc:postgresql://127.0.0.1:5432/postgres"

    var staticUser = "postgres"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

    // 관계형 데이터베이스 Postgresql 연결 (2)
    val dataFromPostgresql= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    // 데이터 확인 (3)
    print(dataFromPostgresql.show(5))
  }
}
