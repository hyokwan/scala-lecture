package com.spark.c8_package

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s1_packageCode {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////////////////////
    // 1. 데이터 파일 로딩
    var staticUrl = "jdbc:oracle:thin:@192.168.0.7:1521/xe"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "predict_step1"

    val inData= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",selloutDb).
      option("user",staticUser).
      option("password",staticPw).load

    ///////////////////////////////////////////
    // 2. 표준 레이아웃 분석모델 구동
    // IDX, SALESID, ITEM, YEARWEEK, WEEK,
    // AVERAGE4WEEK, INFCST, OUTFCST, REGIONID,
    // PRODUCTGROUP, PRODUCT, PLANWEEK
    inData.createOrReplaceTempView("inTable")
    var logicQuery =
      """
        SELECT
        IDX, SALESID, ITEM, YEARWEEK, WEEK, AVERAGE4WEEK,
        -- 이전 단계의 OUTFCST는 현재 단계의 INFCST
        OUTFCST AS INFCST,
        -- 현재 단계의 OUTFCST는 보정단계 적용
        OUTFCST * 1.2 AS OUTFCST,
        REGIONID, PRODUCTGROUP, PRODUCT, PLANWEEK
        FROM inTable
      """
    var result = spark.sql(logicQuery)

    ///////////////////////////////////////////
    // 3. 데이터 저장
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "predict_step2"

    result.write.mode("overwrite").jdbc(staticUrl, table, prop)

  }
}
