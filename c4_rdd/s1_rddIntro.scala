package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s1_rddIntro {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 파일명 설정 및 파일 읽기
    var selloutFile = "KOPO_PRODUCT_VOLUME.csv"

    // 절대경로 입력
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+selloutFile)

    // 데이터 확인
    println(selloutData.show)

    // 컬럼별 인데스 생성
    var rddColumns = selloutData.columns

    var regionidNo = rddColumns.indexOf("REGIONID")
    var productgNo = rddColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var volumeNo = rddColumns.indexOf("VOLUME")

    // 데이터프레임 -> RDD 변환 (1)
    var selloutRdd = selloutData.rdd

    var MAX_VOLUME = 700000

    // 거래량이 최대거래량 (70만) 이상인 경우 최대 거래량 값으로 변경
    var filteredRdd = selloutRdd.
      map(row=>{
        var orgColumns = row.toSeq.toList
        var volume = row.getString(volumeNo).toDouble
        if(volume >= MAX_VOLUME){
          volume = MAX_VOLUME
        }
        (orgColumns :+ volume)
      })

    // rdd 행 개수
    var rowCount = filteredRdd.count
    // rdd 첫번째 행 반환
    var firstRow = filteredRdd.first

  }
}
