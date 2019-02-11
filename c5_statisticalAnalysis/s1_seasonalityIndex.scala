package com.spark.c5_statisticalAnalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object s1_seasonalityIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    /// 데이터 파일 로딩
    // 파일명 설정 및 파일 읽기
    var filePath = "c:/spark/bin/data/"
    var sampleName = "kopo_product_sellout.csv"

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
    var sampleData = csvFileLoading(filePath,sampleName)
    print(sampleData.show(5))

    sampleData.createOrReplaceTempView("keydata")

    println(sampleData.show())

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////

    //    var mainDataSelectSql = "select regionid, regionname, ap1id, ap1name, accountid, accountname," +
    //      "salesid, salesname, productgroup, product, item," +
    //      "yearweek, year, week, " +
    //      "cast(qty as double) as qty," +
    //      "cast(target as double) as target," +
    //      "idx from selloutTable where 1=1"

    // VIEW 테이블 생성
    sampleData.createOrReplaceTempView("SELLOUT_VIEW")

    // 데이터 정제
    var exceptWeek = "53"
    var minVolume = 0
    var maxVolume = 700000
    var refinedQuery = """
      SELECT REGIONID,
         PRODUCT,
         YEARWEEK,
         -- 거래량 최대 생산량 제거
         CAST( CASE WHEN QTY < """ + minVolume + """ THEN 0
                    WHEN QTY > """ + maxVolume + """ THEN 700000
               ELSE QTY END AS DOUBLE) AS QTY
       FROM SELLOUT_VIEW
       WHERE 1=1
       -- 53주차 정보 제거
       AND SUBSTR(YEARWEEK,5,6) != """ + exceptWeek
    var selloutDf = spark.sql(refinedQuery )

    print(selloutDf.show(5))

    // 컬럼 인덱싱
    var sampleDfColumns = selloutDf.columns.map(x=>{x.toLowerCase})

    var regionidNo = sampleDfColumns.indexOf("regionid")
    var productNo = sampleDfColumns.indexOf("product")
    var yearweekNo = sampleDfColumns.indexOf("yearweek")
    var qtyNo = sampleDfColumns.indexOf("qty")

    // 계절성 지수 산출
    var seasonRdd = selloutDf.rdd.
      groupBy(x=>{ (x.getString(regionidNo),
        x.getString(productNo))}).
      flatMap(x=>{

        var key = x._1
        var data = x._2

        // 평균을 구하세요
        var sum_qty = data.map(x=>{x.getDouble(qtyNo)}).sum
        var size = data.size
        var avg = Math.round(sum_qty/size)

        // 연주차별 효과 = 연주차 거래량 / 평균 거래량
        var finalData = data.map(x=>{

          var ratio = 1.0d
          var each_qty = x.getDouble(qtyNo)
          ratio = each_qty/avg
          ratio = Math.round(ratio*100.0)/100.0d

          (x.getString(regionidNo),
            x.getString(productNo),
            x.getString(yearweekNo),
            x.getDouble(qtyNo),
            avg.toDouble,
            ratio.toDouble)})
        finalData
      })

    // Row를 정의하지 않으면 바로 데이터프레임 변환가능
    var yearweekResult = seasonRdd.
      toDF("REGIONID","PRODUCT","YEARWEEK","QTY","AVG_QTY","RATIO")
    println(yearweekResult.show(5))

    yearweekResult.createOrReplaceTempView("MIDDLE_VIEW")

    var seasonalityQuery = """
      SELECT REGIONID,
         PRODUCT,
         SUBSTRING(YEARWEEK,5,2) AS WEEK,
         ROUND(AVG(RATIO),5) AS RATIO
       FROM MIDDLE_VIEW
       GROUP BY REGIONID, PRODUCT, SUBSTRING(YEARWEEK,5,2)"""
    var seasonalityDf = spark.sql(seasonalityQuery )

    print(seasonalityDf.sort("REGIONID","PRODUCT","WEEK").show(5))

  }
}
