package com.spark.c2_sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s1_sparkHivePractice {
  def main(args: Array[String]): Unit = {
    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    //var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // val hiveCtx = new SQLContext(sc) //hiveContext 생성
    var masterFile = "KOPO_REGION_MASTER.csv"

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 파일명 설정 및 파일 읽기
    var selloutFile = "KOPO_PRODUCT_VOLUME_JOIN.csv"

    // 데이터 불러오기
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+selloutFile)

    // View 테이블 생성 (1)
    selloutData.createOrReplaceTempView("SELLOUT_VIEW")


    // 새로운 열(컬럼) 생성 및
    // 조건절 활용하여 거래량 1000건 이상 데이터 조회
    var refinedDataExample = spark.sql(
      " SELECT " +
        " CONCAT(REGIONID,PRODUCT) AS KEY, " +
        " REGIONID, " +
        " PRODUCT," +
        " YEARWEEK, " +
        " CAST(QTY AS Double) " +
        " FROM SELLOUT_VIEW " +
        " WHERE 1=1 " +
        " AND QTY > 1000 ")

    // 데이터 정렬하기
    var sortedDataExample = spark.sql(
      " SELECT " +
        " REGIONID, " +
        " PRODUCT," +
        " YEARWEEK, " +
        " QTY " +
        " FROM SELLOUT_VIEW " +
        " ORDER BY REGIONID, PRODUCT DESC, YEARWEEK")

    // 집계함수를 통한 지역,상품 별 평균 거래량 산출 (2)
    var groupByExample = spark.sql(
      " SELECT REGIONID, PRODUCT, ROUND(AVG(QTY),2) AS AVG_QTY " +
        " FROM SELLOUT_VIEW " +
        " GROUP BY REGIONID, PRODUCT ")

    //집계함수 PartitionBy 예제
    var partitionByExample = spark.sql(
      " SELECT A.REGIONID, " +
        " A.PRODUCT," +
        " A.YEARWEEK, " +
        " A.QTY, " +
        " AVG(A.QTY) OVER(PARTITION BY A.REGIONID, A.PRODUCT) " +
        " AS AVG_QTY" +
        " FROM SELLOUT_VIEW A " )

    // 서브쿼리 예제
    var subQueryExample = spark.sql("" +
      "SELECT B.*, B.QTY/B.AVG_QTY AS RATIO " +
      "FROM ( " +
      " SELECT A.REGIONID, " +
      "      A.PRODUCT," +
      "      A.YEARWEEK, " +
      "      A.QTY, " +
      "      AVG(A.QTY) OVER(PARTITION BY A.REGIONID, A.PRODUCT) " +
      " AS AVG_QTY" +
      " FROM SELLOUT_VIEW A) B " )

    // 서브쿼리 및 고급함수(CASE-WHEN) 예제
    var subQueryAndFunctionExample = spark.sql("" +
      "SELECT B.*, " +
      " ROUND(CASE WHEN B.AVG_QTY = 0 THEN 1 " +
      "       ELSE B.QTY/B.AVG_QTY END , 2) AS RATIO " +
      "FROM ( " +
      "       SELECT A.*," +
      "       ROUND(AVG(A.QTY) " +
      "       OVER(PARTITION BY A.REGIONID, A.PRODUCT),2) " +
      "       AS AVG_QTY" +
      "       FROM SELLOUT_VIEW A " +
      "       WHERE 1=1 " +
      "       AND SUBSTR(YEARWEEK,1,4) >= 2015 " +
      "       AND SUBSTR(YEARWEEK,5,2) != 53 " +
      "       AND PRODUCT IN ('PRODUCT1','PRODUCT2') ) B " )

    // 고급함수 (Pivot) 예제
    var pivotResult =
      subQueryAndFunctionExample.
        groupBy("REGIONID","PRODUCT").
        pivot("YEARWEEK",Seq("201501","201502")).
        sum("RATIO")

    var subQueryAndFunctionExample2 = spark.sql("" +
      "SELECT C.REGIONID, C.PRODUCT, " +
      "SUBSTR(C.YEARWEEK,5,2) AS WEEK, " +
      "AVG(C.RATIO) AS AVG_RATIO FROM ( " +
      "SELECT B.*, " +
      " ROUND(CASE WHEN B.AVG_QTY = 0 THEN 1 " +
      "       ELSE B.QTY/B.AVG_QTY END , 2) AS RATIO " +
      "FROM ( " +
      "       SELECT A.*," +
      "       ROUND(AVG(A.QTY) OVER(PARTITION BY A.REGIONID, A.PRODUCT),2) " +
      "       AS AVG_QTY" +
      "       FROM SELLOUT_VIEW A " +
      "       WHERE 1=1 " +
      "       AND SUBSTR(A.YEARWEEK,1,4) >= 2015 " +
      "       AND SUBSTR(A.YEARWEEK,5,2) != 53 " +
      "       AND A.PRODUCT IN ('PRODUCT1','PRODUCT2') ) B ) C " +
      "GROUP BY C.REGIONID, C.PRODUCT, SUBSTR(C.YEARWEEK,5,2) " )

    var subQueryAndFunctionExample3 = spark.sql("" +
      "SELECT B.*, " +
      " ROUND(CASE WHEN B.AVG_QTY = 0 THEN 1 " +
      "       ELSE B.QTY/B.AVG_QTY END , 2) AS RATIO" +
      "FROM ( " +
      "       SELECT A.*," +
      "       ROUND(AVG(A.QTY) OVER(PARTITION BY A.REGIONID, A.PRODUCT),2) " +
      "       AS AVG_QTY" +
      "       FROM SELLOUT_VIEW A " +
      "       WHERE 1=1 " +
      "       AND SUBSTR(YEARWEEK,1,4) >= 2015 " +
      "       AND SUBSTR(YEARWEEK,5,2) != 53 " +
      "       AND PRODUCT IN ('PRODUCT1','PRODUCT2') ) B " )


    var basicExample = spark.sql(
      " SELECT REGIONID, PRODUCT, SUBSTR(YEARWEEK,5,2) AS YEAR FROM SELLOUT_VIEW " +
        "WHERE 1=1"
    )

  }
}
