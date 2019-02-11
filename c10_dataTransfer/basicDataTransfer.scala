package com.spark.c10_dataTransfer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object basicDataTransfer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setAppName("DataBasicTransfer")
      // setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    ///////////////////////////  데이터 파일 로딩  ////////////////////////////////////
    // 접속정보 설정 (1)
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var paramTable = "kopo_parameter"

    // 데이터 불러오기
    val paramData= spark.read.format("jdbc").
      option("url",staticUrl).
      option("dbtable",paramTable).
      option("user",staticUser).
      option("password",staticPw).load

    // 컬럼별 인데스 생성
    var paramColumns = paramData.columns
    var pcatNo = paramColumns.indexOf("PARAM_CATEGORY")
    var pnameNo = paramColumns.indexOf("PARAM_NAME")
    var valueNo = paramColumns.indexOf("PARAM_VALUE")

    var paramRdd = paramData.rdd
    var paramGroupMap = paramRdd.groupBy(x=>{
      (x.getString(pcatNo),
        x.getString(pnameNo)) }).
      map(x=>{
        // 그룹별 분산처리가 수행됨
        var key = x._1
        var data = x._2
        var paramValue = data.map(x=>{x.getString(valueNo)}).toArray
        (key, paramValue)
      }).collectAsMap

    var inTable = paramGroupMap("COMMON","INTABLE")(0)
    var outTable = paramGroupMap("COMMON","OUTTABLE")(0)

    var query = "(select * from " + inTable+ ") intable"

    // 데이터 불러오기
    val inData= spark.read.
      format("jdbc").
      option("url",staticUrl).
      option("dbtable",query).
      option("user",staticUser).
      option("password",staticPw).load

    // 데이터베이스 주소 및 접속정보 설정 (1)
    var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 접속 설정하기
    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)

    // 데이터 저장하기
    inData.
      write.
      mode("overwrite").
      jdbc(outputUrl, outTable, prop)

    println("import completed")
  }
}
