package com.spark.c2_sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s2_sparkHiveJoin {
  def main(args: Array[String]): Unit = {
    ////////////////////////////////////  Spark-session definition  ////////////////////////////////////
    //var spark = SparkSession.builder().config("spark.master","local").getOrCreate()
    val conf = new SparkConf().setAppName("Test").setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 파일명 설정 및 파일 읽기 (2)
    var selloutFile = "KOPO_PRODUCT_VOLUME_JOIN.csv"
    var masterFile = "KOPO_REGION_MASTER.csv"

    // 데이터 불러오기
    var selloutData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+selloutFile)

    // 데이터 불러오기
    var masterData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+masterFile)

    // View 테이블 생성 (2)
    selloutData.createOrReplaceTempView("sellouttable")
    masterData.createOrReplaceTempView("mastertable")

    // SQL 실행 (3)
    var innerJoinResult = spark.sql("select b.regionname, a.product, a.yearweek, a.qty " +
      " from sellouttable a " +
      " inner join mastertable b  " +
      " on a.regionid = b.regionid")

    // 결과 확인
    print(innerJoinResult.show(2))

    // SQL 실행
    var leftJoinResult = spark.sql("select b.regionname, a.product, a.yearweek, a.qty " +
      " from sellouttable a " +
      " left join mastertable b  " +
      " on a.regionid = b.regionid")

    // 결과 확인
    print(leftJoinResult.show)

    // SQL 실행
    var rightJoinResult = spark.sql("select b.regionname, a.product, a.yearweek, a.qty " +
      " from sellouttable a " +
      " right join mastertable b  " +
      " on a.regionid = b.regionid")

    // 결과 확인
    print(rightJoinResult.show)
  }
}
