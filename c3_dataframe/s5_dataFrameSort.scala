package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s5_dataFrameSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var selloutFile = "kopo_product_volume.csv"

    // 절대경로 입력
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+selloutFile)

    // 데이터 확인
    print(selloutData.show(5))

    // 기본정렬 : 컬럼별 오름차순
    var sortedDataAuto = selloutData.
      sort("REGIONID","PRODUCTGROUP","YEARWEEK")

    // 정렬방법 정의 후 정렬 : 컬럼별
    var sortedDataManual = selloutData.
      sort($"REGIONID".asc, $"PRODUCTGROUP".desc, $"YEARWEEK".asc)

    // 데이터 확인
    print(sortedDataAuto.show(5))

    // 데이터 확인
    print(sortedDataManual.show(5))


  }
}
