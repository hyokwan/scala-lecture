package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

// import 문 생략
object s6_dataLoadingHdfs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기 (1)
    var paramFile = "KOPO_BATCH_SEASON_MPARA.txt"

    // 절대경로 입력
    val hdfsData = spark.read.
      option("header","true").
      format("csv").
      load("hdfs://192.168.0.30:9000/kopo/test.csv")

    // 데이터 확인 (2)
    print(hdfsData.show)

  }
}
