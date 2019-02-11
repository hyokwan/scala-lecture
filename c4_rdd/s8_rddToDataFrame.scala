package com.spark.c4_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object s8_rddToDataFrame {
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

    // 1. 컬럼별 인데스 생성
    var rddColumns = rddSampleData.columns

    var regionidNo = rddColumns.indexOf("REGIONID")
    var productgNo = rddColumns.indexOf("PRODUCTGROUP")
    var yearweekNo = rddColumns.indexOf("YEARWEEK")
    var volumeNo = rddColumns.indexOf("VOLUME")

    // 1. RDD 가공작업 수행
    var sampleRdd = rddSampleData.rdd.map(x=>{
      (x.getString(regionidNo),
        x.getString(productgNo),
        x.getString(yearweekNo),
        x.getString(volumeNo))
    })

    // 2. 데이터프레임 변환
    var resultDF = sampleRdd.
      toDF("REGIONID","PRODUCT","YEARWEEK","VOLUME")

    import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
    import org.apache.spark.sql.Row

    // 1. RDD 가공작업 수행
    var sampleRddRow = rddSampleData.rdd.map(x=>{
      Row(x.getString(regionidNo),
        x.getString(productgNo),
        x.getString(yearweekNo),
        x.getString(volumeNo))
    })

    // 2. 데이터프레임 변환
    var resultRowDf = spark.createDataFrame(sampleRddRow,
      StructType(Seq(
        StructField("REGIONID", StringType),
        StructField("PRODUCT", StringType),
        StructField("YEARWEEK", StringType),
        StructField("VOLUME", StringType)
      )))

  }
}
