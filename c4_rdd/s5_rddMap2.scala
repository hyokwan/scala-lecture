package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s5_rddMap2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "kopo_promotion_info.csv"

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
    var itemNo = rddColumns.indexOf("ITEM")
    var targetWeekNo = rddColumns.indexOf("TARGETWEEK")
    var planWeekNo = rddColumns.indexOf("PLANWEEK")
    var priceNo = rddColumns.indexOf("PRICE")
    var discountNo = rddColumns.indexOf("DISCOUNT")

    var filterSampleRdd = filterRddSampleData.rdd

    var filteredRdd = filterSampleRdd.filter(row=>{
      var checkValid = true
      var price = row.getString(priceNo).toDouble
      var discount = row.getString(discountNo).toDouble

      // 할인가격이 판매가격보다 큰경우 데이터 삭제
      if(discount > price){
        checkValid = false
      }
      checkValid
    })

    // missingValue 처리
    var mappedRdd = filteredRdd.map(row=>{

      //var orgColumns = row.toSeq.toList
      var price = row.getString(priceNo).toDouble
      var discount = row.getString(discountNo).toDouble

      // 프로모션 비율정보 생성
      var promotionRate = 0.0d
      promotionRate = if(price != 0){
        discount/price
      }else{
        0.0d
      }
      (row.getString(regionidNo), row.getString(productgNo),
        row.getString(itemNo), row.getString(targetWeekNo),
        row.getString(planWeekNo), row.getString(priceNo),
        row.getString(discountNo) )
    })

  }
}
