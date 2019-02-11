package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

object s5_rddMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "kopo_product_volume.csv"

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

    var filterRddSampleData = csvFileLoading(filePath,sampleName)

    // 데이터 확인
    println(filterRddSampleData.show(5))

    // 컬럼 인덱스
    var rddColumns = filterRddSampleData.columns

    var regionidNo = rddColumns.indexOf("REGIONID")
    var productgNo = rddColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var volumeNo = rddColumns.indexOf("VOLUME")

    var filteredRdd = filterRddSampleData.rdd

    // 생산 한계치 설정
    var MAX_VOLUME = 700000

    // 거래량이 한계 값 upperBand 70만 이상 시 70만 설정
    var mappedRdd = filteredRdd.map(row=>{

      // 거래량 컬럼정보 변수 맵핑
      var volume = row.getString(volumeNo).toDouble

      // 한계치 이상 시 한계치 값으로 변경
      if(volume >= MAX_VOLUME){ volume = MAX_VOLUME}

      // 출력 컬럼정보 생성
      Row(row.getString(regionidNo)
        , row.getString(productgNo)
        , row.getString(yearweekNo)
        , volume)
    })

    var debuggingCase1 = filteredRdd.first

    var debuggingCase2 = filteredRdd.filter(row=>{
      var checkValid = true
      if(row.getString(volumeNo).toDouble > MAX_VOLUME) {
        checkValid = false
      }
      checkValid
    }).first

  }
}
