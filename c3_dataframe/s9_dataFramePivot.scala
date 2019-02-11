package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s9_dataFramePivot {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var dtResultFile = "decisionTreeResult.csv"

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
    var dtResultData = csvFileLoading(filePath,dtResultFile)

    // 타입 변경 및 정렬
    var refinedData = dtResultData.
      withColumn("SALES", $"SALES".cast("Double")).
      sort("PRODUCT","ITEM","YEARWEEK")

    var pivotDf = refinedData.groupBy("PRODUCT","ITEM","YEARWEEK").
      pivot("MEASURE", Seq("REAL_QTY","PREDICTION_QTY")).
      sum("SALES")

    print(pivotDf.show(5))
  }
}
