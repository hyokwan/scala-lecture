package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s2_dataFrameSelect {
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
    println(selloutData.show)

    var selectedData = selloutData.
      filter( ($"PRODUCTGROUP" === "ST0002") &&
        ($"VOLUME" > 150000))
    print(selectedData.show)

    var columnSelectedData =
      selectedData.
        select("REGIONID","PRODUCTGROUP","YEARWEEK")

    print(columnSelectedData.show)
  }
}
