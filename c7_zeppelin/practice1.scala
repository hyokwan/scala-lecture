package com.spark.c7_zeppelin

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object practice1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///데이터 파일 로딩
    // Oracle 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/xe"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_product_volume"

    // 관계형 데이터베이스 Oracle 연결
    val dataFromOracle= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    // 데이터 확인
    println(dataFromOracle.show(5))

    // View 테이블 생성
    dataFromOracle.createOrReplaceTempView("kopo_table")

//    %sql
//    select regionid
//    ,productgroup
//    ,substring(yearweek,5,2)
//    ,round(avg(volume),2)
//    from test
//      where 1=1
//    group by regionid, productgroup, substring(yearweek,5,2)
//    order by regionid, productgroup, substring(yearweek,5,2)
  }
}
