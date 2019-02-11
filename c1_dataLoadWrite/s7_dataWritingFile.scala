package com.spark.c1_dataLoadWrite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s7_dataWritingFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 파일명 설정 및 파일 읽기 (2)
    var selloutFile = "KOPO_PRODUCT_VOLUME.csv"

    // 절대경로 입력
    var paramData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+selloutFile)

    // 데이터 확인 (3)
    println(paramData.show)

    paramData.
      coalesce(1). // 파일개수
      write.format("csv").  // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/kopo_product_volume_out.csv") // 저장파일명
  }
}
