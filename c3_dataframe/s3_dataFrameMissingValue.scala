package com.spark.c3_dataframe

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object s3_dataFrameMissingValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 데이터 파일명 정의
    var missingValueFile = "missingValue.csv"

    // 데이터 불러오기
    var missingValueData=
      spark.read.format("csv").
        option("header","true").
        option("Delimiter",",").
        load("c:/spark/bin/data/"+missingValueFile)

    // 데이터 확인
    println(missingValueData.show(5))

    // 특정 컬럼에 대한 탐색 (1)
    var missingColumnData = missingValueData.
      filter( ($"VOLUME".isNull) ||
              ($"TARGET".isNull))

    // 전체 컬럼에 대한 탐색 (2)
    var missingAnyData = missingValueData.
      filter(row => {row.anyNull})

    // 데이터 확인
    println(missingColumnData.show(5))

    // 데이터 확인
    println(missingAnyData.show(5))

    // 전체컬럼 null 값 채우기
    var filteredAllData = missingValueData.na.fill("0")

    // 특정컬럼 null 값 채우기
    var targetColumns = Array("VOLUME","TARGET")
    var filteredTargetData = missingValueData.na.fill("0", targetColumns)

    // 데이터 확인
    print(filteredAllData.show(5))

    // 데이터 확인
    print(filteredTargetData.show(5))
  }
}
