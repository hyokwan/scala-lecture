package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object s4_dataFrameManipulation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 데이터 파일명 정의
    var promotionDataFile = "promotionData.csv"

    // 데이터 불러오기
    var promotionData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+promotionDataFile)

    // 데이터 확인
    println(promotionData.show(5))

    // 데이터 타입 확인 (1)
    promotionData.dtypes.foreach(println)

    // 데이터 타입 변경 (2)
    var promotionDataType = promotionData.
      withColumn("PRICE", $"PRICE".cast("Double")).
      withColumn("DISCOUNT", $"DISCOUNT".cast("Double"))

    // 데이터 타입 확인
    promotionDataType.dtypes.foreach(println)

    ////////////////////// 데이터 가공 /////////////////////////

    // 라이브러리 추가 (1)
    import org.apache.spark.sql.functions._

    // 데이터 가공 (2)
    var promotionDataFinal = promotionDataType.
      withColumn("NEW_DISCOUNT",
        when($"DISCOUNT" > $"PRICE", 0).
        otherwise($"DISCOUNT"))

    // 데이터 확인
    print(promotionDataFinal.show(5))

  }
}
