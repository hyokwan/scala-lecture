package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s7_rddGroupMapFunction {
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

    var rddSampleData = csvFileLoading(filePath,sampleName)

    // 데이터 확인
    println(rddSampleData.show(5))

    // 컬럼별 인데스 생성
    var rddColumns = rddSampleData.columns

    var regionidNo = rddColumns.indexOf("REGIONID")
    var productgNo = rddColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var volumeNo = rddColumns.indexOf("VOLUME")

    // 1. RDD 변환
    var sampleRdd = rddSampleData.rdd

    // regionid, productgroup, yearweek, volume
    // 2. 지역, 상품별 평균 거래량 산출
    var groupMapFunction = sampleRdd.groupBy(x=>{
      (x.getString(regionidNo),
        x.getString(productgNo))}).
      map(x=>{
        // 그룹별 분산처리가 수행됨
        var key = x._1
        var data = x._2
        var size = data.size
        var volumeSum = data.map(x=>{x.getString(volumeNo).toDouble}).sum
        var average = volumeSum/size
        (key, (size,average))
      }).collectAsMap
  }
}
