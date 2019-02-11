package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object s8_dataFrameAdvFunction {
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

    // 데이터 컬럼 인덱싱
    var selloutColumns = selloutData.columns.map(x=>{x.toLowerCase})
    var regionidNo = selloutColumns.indexOf("regionid")
    var productgNo = selloutColumns.indexOf("productgroup")
    var yearweekNo = selloutColumns.indexOf("yearweek")
    var volumeNo = selloutColumns.indexOf("volume")

    var selloutRdd = selloutData.rdd

    var MAX_VOLUME = 700000

    // rdd - transformation
    var filteredRdd = selloutRdd.
      map(row=>{
        var orgColumns = row.toSeq.toList
        var volume = row.getString(volumeNo).toDouble
        if(volume > MAX_VOLUME){
          volume = MAX_VOLUME
        }
        (orgColumns :+ volume)
      })

    // rdd - action
    // rdd 행 개수
    var rowCount = filteredRdd.count
    // rdd 첫번째 행 반환
    var firstRow = filteredRdd.first
  }
}
