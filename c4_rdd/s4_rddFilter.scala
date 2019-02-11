package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s4_rddFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "filterRddSample.csv"

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
    var missingValueColumns = filterRddSampleData.columns

    var regionidNo = missingValueColumns.indexOf("REGIONID")
    var productgNo = missingValueColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = missingValueColumns.indexOf("YEARWEEK")
    var volumeNo = missingValueColumns.indexOf("VOLUME")

    var filterSampleRdd = filterRddSampleData.rdd

    // yearweek 자리수 확인
    var YEARWEEK_SIZE = 6
    var filteredRdd = filterSampleRdd.filter(row=>{
      var checkValid = true
      // 컬럼 개수 확인
      if(row.getString(yearweekNo).size != YEARWEEK_SIZE){
        checkValid = false
      }
      checkValid
    })

    // missingValue 처리
    var missingFilteredRdd = filteredRdd.filter(row=>{

      // 컬럼 개수 확인
      var rowSize = row.size
      // 데이터 유효성 판단 변수 생성
      var checkValid = true

      for (i <- 0  until rowSize) {
        if(row.isNullAt(i) == true)
          checkValid = false
      }
      checkValid
    })

    // 디버깅 하기 (첫 번째 행)
    var row = missingFilteredRdd.first

    // 디버깅 하기 (원하는 행)
    var row2 = missingFilteredRdd.filter(x=>{
      (x.getString(volumeNo) == null)
    }).first

  }
}
