package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object s7_dataFrameJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var selloutFile = "kopo_product_volume.csv"
    var promotionFile = "promotionData.csv"

    // 파일 불러오기 함수 정의
    def csvFileLoading(workPath: String, fileName: String)
    : org.apache.spark.sql.DataFrame = {
      var outDataFrame=
      spark.read.format("csv").
      option("header","true").
      option("Delimiter",",").
      load(workPath+fileName)
      outDataFrame
    }

    // 거래 데이터
    var selloutData = csvFileLoading(filePath,selloutFile)
    // 프로모션 데이터
    var promotionData = csvFileLoading(filePath,promotionFile)

    // inner join 예제
    var innerJoin = selloutData.
      join(promotionData,
        Seq("REGIONID","PRODUCTGROUP","YEARWEEK"),
        joinType = "inner" )

    // left join 예제
    var leftJoin = selloutData.
      join(promotionData,
        Seq("REGIONID","PRODUCTGROUP","YEARWEEK"),
        joinType = "left_outer" )

    // inner 조인 확인
    print(innerJoin.show(5), innerJoin.count)

    // left 조인 확인
    print(leftJoin.show(5), leftJoin.count)

  }
}
