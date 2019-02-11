package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object s6_dataFrameGroupby {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
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

    // 집계함수 구현
    // 함수 라이브러리 로딩
    import org.apache.spark.sql.functions._

    // 지역(REGIONID), 상품(PRODUCTGROUP) 별 평균 거래량
    var groupbyData = selloutData.groupBy($"REGIONID", $"PRODUCTGROUP").
      agg(mean($"VOLUME") as "MEAN_VOLUME")

    // 데이터 확인
    print(groupbyData.show(5))

  }
}
