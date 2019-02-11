package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object S3_rddConvert {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var selloutFile = "kopo_product_volume.csv"

    // 절대경로 입력
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+selloutFile)

    // 데이터 확인
    println(selloutData.show)

    // 데이터프레임 -> RDD 변환
    var selloutRdd = selloutData.rdd

    selloutRdd.first
  }
}
